"""Familiar platform adapter for Hermes: the channel a Familiar client reaches a machine through.

Familiar is a client for Hermes instances. It reads an instance's sessions and jobs over the instance's own
HTTP API, but a cron job's OUTPUT is pushed and never read: the Jobs API carries status alone. This adapter
is the push side, and it is the whole reason the plugin exists.

Registering a platform with a ``cron_deliver_env_var`` is what makes ``deliver: familiar`` a valid job
target (``cron/scheduler_delivery.py`` -> ``_is_known_delivery_platform``), and ``send()`` is the one method
the delivery path needs. So a job's output lands in Familiar instead of in a file on the instance that no
client can read.

A person's message arrives at this machine's own loopback ingress and becomes a turn - ``interactive_resume``
is True, because Familiar has somebody sitting in front of it. ``supports_async_delivery`` stays False and no
``platform_hint`` is set.

Settings, ``config.yaml platforms.familiar.extra.<key>`` first and the env var second (``extra`` wins):

	url           FAMILIAR_URL           default http://127.0.0.1:3100
	token         FAMILIAR_TOKEN         required: the bearer token Familiar issued for this instance
	instance      FAMILIAR_INSTANCE      optional label, defaults to the profile's name
	home_channel  FAMILIAR_HOME_CHANNEL  default "default"

Standard library only, on purpose. A missing third-party import would disable the channel silently, and a
delivery path is the last place that should be conditional. The POST runs in a worker thread, so it never
blocks the gateway's event loop.
"""

from __future__ import annotations

import socket
import time
import uuid
import asyncio
import contextlib
import hashlib
import json
import logging
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from gateway.config import Platform, PlatformConfig
from gateway.platforms.event import MessageEvent, MessageType
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms._shared import get_scoped_secret as _get_scoped_secret

logger = logging.getLogger(__name__)

DEFAULT_URL = "http://127.0.0.1:3100"
DEFAULT_TARGET = "default"
NOTIFY_PATH = "/api/hermes/notifications"
#: Where the agent's asks go: a clarify question, an approval, and the gateway's own confirmation. Same
#: convention as the notification path - one base URL, one path per kind of thing being delivered.
ASK_PATH = "/api/hermes/ask"

#: The channel: the connection THIS machine opens, and the requests Familiar sends down it. Everything else here
#: is outbound too, but this is the one that makes the machine reachable in the other direction - which is what a
#: machine behind NAT, with no port forwarded and no key, cannot otherwise be.
CHANNEL_STREAM_PATH = "/api/channel/stream"
CHANNEL_REPLY_PATH = "/api/channel/reply"
CHANNEL_PAIR_PATH = "/api/channel/pair"
CHANNEL_PAIR_WAIT_PATH = "/api/channel/pair/wait"
# How long the pair endpoint waits for a code to arrive before answering without one. The code takes a round trip
# to the reader's Familiar, so an endpoint that answered instantly would say "no code" to a person who has no way
# of knowing that the answer wanted asking for again. Bounded, because a Familiar nobody can reach must not turn
# a read of the machine into a wait for it.
PAIR_PROBE_WAIT_S = 10.0
PAIR_PROBE_TICK_S = 0.1

#: What a paired machine answers out of its OWN state. These are the paths the app already reads over an
#: instance's HTTP API, answered from Hermes' own store and in the same shape - because the app parses one
#: shape, and a second one would be a second source of truth. A path that is not here is refused by name.
_API_ROUTES = (
	("GET", r"/api/sessions", "_api_list_sessions"),
	("GET", r"/api/sessions/(?P<session>[^/]+)/messages", "_api_session_messages"),
	("POST", r"/api/sessions/(?P<session>[^/]+)/fork", "_api_fork_session"),
	("GET", r"/api/sessions/(?P<session>[^/]+)", "_api_get_session"),
	("PATCH", r"/api/sessions/(?P<session>[^/]+)", "_api_patch_session"),
	("DELETE", r"/api/sessions/(?P<session>[^/]+)", "_api_delete_session"),
	("GET", r"/api/model/options", "_api_model_options"),
	("GET", r"/api/jobs", "_api_list_jobs"),
	("POST", r"/api/jobs", "_api_create_job"),
	("GET", r"/api/jobs/(?P<job>[^/]+)", "_api_get_job"),
	("PATCH", r"/api/jobs/(?P<job>[^/]+)", "_api_update_job"),
	("DELETE", r"/api/jobs/(?P<job>[^/]+)", "_api_delete_job"),
	("POST", r"/api/jobs/(?P<job>[^/]+)/(?P<action>run|pause|resume)", "_api_job_action"),
)

#: How many matches of a search this machine will READ to count them. Counting a search is reading it: Hermes'
#: store counts a listing without a search term, nothing over it counts with one, and the search itself runs in
#: SQL over whole compression chains - so the only thing that can answer "how many" is the store, one row at a
#: time. Past this ceiling it answers ``None``: the machine would rather say it did not count than report a number
#: it never read. A search narrow enough to fill a page is counted long before getting here.
_SEARCH_COUNT_CEILING = 5000

#: The cron store's own limits, copied from the API server so a job made down the connection is held to exactly
#: what one made over HTTP is held to: one rule, whichever door it came through.
_JOB_ID_RE = re.compile(r"[a-f0-9]{12}")
_JOB_UPDATE_FIELDS = {"name", "schedule", "prompt", "deliver", "skills", "skill", "repeat", "enabled"}
_MAX_JOB_NAME = 200
_MAX_JOB_PROMPT = 5000

#: What a paired machine's state is called. Where it lives is `_state_path`.
STATE_FILE = "state.json"

#: The directory this plugin keeps that state in, under Hermes' home rather than inside the plugin.
STATE_DIR = "familiar-platform"

#: A connection that drops is retried: the far end restarting is not a reason to give up. Backing off, because a
#: machine that reconnects in a tight loop is a machine nobody can use.
RECONNECT_MIN_S = 1.0
RECONNECT_MAX_S = 30.0

#: The answer to a confirmation this adapter raised: ``cf:<confirm_id>:<once|cancel>``. The gateway's own text
#: fallback spells these as commands (``/approve``, ``/cancel``); this is the same answer without relying on a
#: message that is not a message becoming one.
CONFIRM_PREFIX = "cf:"

#: Loopback-only: what this machine answers when somebody asks it what code it is showing.
PAIR_PATH = "/familiar/pair"


def _state_home() -> Path:
	"""Hermes' own home, which is where a plugin's state belongs.

	Asked of Hermes rather than read off the environment: the home is profile-aware, so a machine running two profiles
	keeps two pairings, which is what having two profiles means. The env var is the fallback for an older Hermes that
	does not offer the function - it is where that home lives anyway.
	"""
	try:
		from hermes_constants import get_hermes_home

		return Path(get_hermes_home()) / STATE_DIR
	except Exception:  # noqa: BLE001 - an import that fails must not cost the machine its pairing
		return Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))) / STATE_DIR


def _state_path() -> Path:
	"""Where the token and the remembered refusal live.

	Under Hermes' home, NOT beside the plugin. Beside the plugin is where this started, and it is the one place an
	update is guaranteed to destroy: `plugins install --force` removes this plugin's whole directory and lays a new
	one down, so every update threw the token away and every machine had to pair again - a step with no meaning for
	the reader, who had done nothing to lose it. Not in a Hermes config either: a credential that arrives by pairing
	has no business in a file a reader edits by hand.

	A state file from before this change is still found and MOVED, once: a machine that already paired must not have
	to pair again because the plugin learned where to put things.
	"""
	path = _state_home() / STATE_FILE

	if path.exists():
		return path

	legacy = Path(__file__).resolve().parent / STATE_FILE

	if legacy.exists():
		try:
			path.parent.mkdir(parents=True, exist_ok=True)
			legacy.replace(path)

			return path
		except Exception as error:  # noqa: BLE001 - an old file that will not move still beats no token at all
			logger.warning("[familiar] could not move this machine's pairing out of the plugin directory: %s", error)

			return legacy

	try:
		path.parent.mkdir(parents=True, exist_ok=True)
	except Exception:  # noqa: BLE001 - a home that will not take a directory fails loudly on the write instead
		pass

	return path


def _load_token() -> str:
	"""The token this machine was given when it was paired, or an empty string."""
	try:
		return str(json.loads(_state_path().read_text(encoding="utf-8")).get("token") or "")
	except Exception:  # noqa: BLE001 - no state, unreadable state: both mean "not paired yet"
		return ""


def _save_token(token: str) -> None:
	"""Keep the token across restarts. A machine that has to pair again after every restart is not paired."""
	try:
		_state_path().write_text(json.dumps({"token": token}, indent="\t"), encoding="utf-8")
	except Exception as error:  # noqa: BLE001 - a token that will not persist is worth a warning, not a crash
		logger.warning("[familiar] could not save this machine's pairing: %s", error)


def _clear_token() -> None:
	try:
		_state_path().unlink(missing_ok=True)
	except Exception as error:  # noqa: BLE001
		logger.warning("[familiar] could not clear this machine's pairing: %s", error)


def _token_hint(token: str) -> str:
	"""A short, non-reversible name for a token, so a refusal can be remembered without keeping the token."""
	return hashlib.sha256(token.encode("utf-8")).hexdigest()[:8]


def _load_refused() -> str:
	"""The token this Familiar has already refused, as its hint, or an empty string.

	Only ever a token that came from OUTSIDE this install - the environment or the config. The one pairing gave
	us is deleted outright when it stops being accepted, so there is nothing to remember.
	"""
	try:
		return str(json.loads(_state_path().read_text(encoding="utf-8")).get("refused") or "")
	except Exception:  # noqa: BLE001 - no state, unreadable state: nothing has been refused
		return ""


def _mark_refused(token: str) -> None:
	"""Remember that this machine must stop presenting a token it was configured with.

	The token itself is somebody's deliberate setting and is not this plugin's to erase: what is written down is
	that presenting it again would fail, scoped to THAT token's hint so a corrected value in the environment is
	honoured the moment it changes. Without it a machine whose instance was deleted elsewhere retries the same
	dead token forever - reporting ``paired: true``, showing no code, with no way back from the app.
	"""
	try:
		state: Dict[str, Any] = {}

		if _state_path().exists():
			state = json.loads(_state_path().read_text(encoding="utf-8")) or {}

		state["refused"] = _token_hint(token)
		state.pop("token", None)
		_state_path().write_text(json.dumps(state, indent="\t"), encoding="utf-8")
	except Exception as error:  # noqa: BLE001 - worth a warning, not a crash
		logger.warning("[familiar] could not record that a token was refused: %s", error)


def _plugin_version() -> str:
	"""The build this plugin is, read from its own manifest.

	One place names a build, and it is the place an operator edits, so what an instance reports cannot drift
	from what was installed. That is the point: "did my update land?" is otherwise answered by guessing, and a
	stale plugin and a broken one look alike from the other end of a connection.
	"""
	try:
		manifest = (Path(__file__).resolve().parent / "plugin.yaml").read_text(encoding="utf-8")

		for line in manifest.split("\n"):
			if line.startswith("version:"):
				return line.split(":", 1)[1].strip().strip('"').strip("'")
	except Exception as error:  # noqa: BLE001 - a version is worth having, not worth failing over
		logger.debug("[familiar] could not read the plugin's own version: %s", error)

	return "unknown"


#: Reported with everything this plugin sends, so the app can say which build an instance is running.
PLUGIN_VERSION = _plugin_version()
#: Where Familiar posts what a person said, and the answers to the asks above. One route for both:
#: the gateway routes a message that arrives while a run is parked to the clarify intercept.
#: Where the agent's reply goes. A reply in a conversation is not a job delivery, so it must not land
#: in the notification list a cron output is addressed to - hence a path of its own.
MESSAGE_PATH = "/api/hermes/message"
INGRESS_PATH = "/familiar/ingress"
#: The port this adapter listens on inside the instance. The api_server holds 8642 and the dashboard
#: 8080, so the channel takes the next one.
DEFAULT_INGRESS_PORT = 8644

"""
What one delivery carries before it is cut.

Familiar caps a delivery at the same 20 000 characters and REFUSES more rather than trimming it, so a message
longer than this has to be cut here or it does not arrive at all. The cap is in characters on both sides and
sits well under the 100 kB JSON body a default Express parser accepts: 20 000 characters is at most 80 kB in
UTF-8, so the same text fits whatever it is written in. Familiar keeps what it is sent; the instance keeps the
whole output in its own cron journal.
"""
MAX_MESSAGE_LENGTH = 20_000
TIMEOUT_SECONDS = 15.0


class _DeliveryError(Exception):
	"""A delivery that did not land. ``retryable`` separates "try again" from "this will never work"."""

	def __init__(self, message: str, *, retryable: bool = False) -> None:
		super().__init__(message)
		self.retryable = retryable


def _setting(extra: Dict[str, Any], key: str, env: str, default: str = "") -> str:
	"""``config.yaml extra[key]`` wins over the env var ``env``, as every platform plugin's settings do."""
	value = extra.get(key)
	if value is not None and str(value).strip():
		return str(value).strip()
	return (_get_scoped_secret(env, default) or default).strip()


def _default_instance_name() -> str:
	"""The profile this install is, as a label Familiar can show.

	``default`` for the default home, otherwise the profile directory's name. Derived rather than hardcoded:
	the label travels to a client that may serve several instances, so it has to say which one spoke.
	"""
	try:
		from hermes_constants import get_hermes_home

		name = Path(get_hermes_home()).name
	except Exception:
		return DEFAULT_TARGET
	return DEFAULT_TARGET if name.lower() == "hermes" else name


def _instance_name(extra: Dict[str, Any]) -> str:
	return _setting(extra, "instance", "FAMILIAR_INSTANCE") or _default_instance_name()


def _job_name(job_id: str) -> Optional[str]:
	"""The job's own name, when this process can see the cron store.

	Best effort on purpose: a delivery must not fail because a name could not be read, and the id is already in
	the payload for a client that wants to resolve the name itself. A gateway delivers through this plugin, so
	the store is the same one the scheduler just read.
	"""
	try:
		from cron.jobs import get_job

		return (get_job(job_id) or {}).get("name") or None
	except Exception:
		logger.debug("[familiar] could not read the name of job %s", job_id, exc_info=True)
		return None


def _payload(instance: str, target: str, content: str, metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
	"""What Familiar receives: the notification, not the job.

	``jobId`` is filled on a cron delivery because the scheduler puts it in the route metadata
	(``cron/scheduler_delivery.py`` -> ``_live_route_metadata``). A ``send_message`` call carries no job, so
	the field is null rather than absent - a client shows "not from a job" and "job unknown" differently.

	``jobName`` rides with it because that is what a reader recognizes in a notification list; the id stays for
	anything that has to act on the job.
	"""
	job_id = (metadata or {}).get("job_id")
	truncated = len(content) > MAX_MESSAGE_LENGTH
	if truncated:
		logger.warning(
			"[familiar] delivery truncated from %d to %d chars", len(content), MAX_MESSAGE_LENGTH)
	return {
		"instance": instance,
		"target": target,
		"pluginVersion": PLUGIN_VERSION,
		"content": content[:MAX_MESSAGE_LENGTH],
		"truncated": truncated,
		"jobId": str(job_id) if job_id else None,
		"jobName": _job_name(str(job_id)) if job_id else None,
		"sentAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
	}


def _user_agent(payload: Dict[str, Any]) -> str:
	"""Who is calling, for the far end's logs.

	A delivery names its instance; a channel reply has no instance in it, because the request id it is
	answered under is the whole address. So the name is left out rather than invented - which is also what a
	missing key used to do here, by raising into the one path that owes an answer.
	"""
	instance = str(payload.get("instance") or "").strip()

	return f"hermes-familiar (instance:{instance})" if instance else "hermes-familiar"


def _post(url: str, token: str, payload: Dict[str, Any], path: str = NOTIFY_PATH) -> Dict[str, Any]:
	"""One POST to Familiar, run in a worker thread. Returns its parsed response body.

	``urllib`` rather than a client library: a delivery channel that stops working because an optional
	import moved is worse than a few more lines here.
	"""
	request = urllib.request.Request(
		f"{url}{path}",
		data=json.dumps(payload).encode("utf-8"),
		method="POST",
		headers={
			"Content-Type": "application/json",
			"Authorization": f"Bearer {token}",
			"User-Agent": _user_agent(payload),
		},
	)
	try:
		with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
			raw = response.read().decode("utf-8", errors="replace")
	except urllib.error.HTTPError as error:
		detail = ""
		try:
			detail = error.read().decode("utf-8", errors="replace")[:200]
		except Exception:
			pass
		# A refused token will not fix itself; a server fault might.
		raise _DeliveryError(f"HTTP {error.code} {detail}".strip(), retryable=error.code >= 500) from error
	except (urllib.error.URLError, TimeoutError, OSError) as error:
		raise _DeliveryError(f"could not reach {url}: {error}", retryable=True) from error
	if not raw.strip():
		return {}
	try:
		body = json.loads(raw)
	except json.JSONDecodeError:
		return {}
	return body if isinstance(body, dict) else {}


def _ask_payload(
	instance: str,
	target: str,
	kind: str,
	body: str,
	choices: Optional[List[str]],
	request_id: str,
	session_key: str,
	callback_prefix: str,
	command: Optional[str] = None,
	description: Optional[str] = None,
	expires_in: Optional[int] = None,
) -> Dict[str, Any]:
	"""What Familiar receives when the agent needs an answer.

	``requestId`` is the thing that makes the answer land: the gateway resolves the reply through
	``tools.clarify_gateway.resolve_gateway_clarify`` (or ``tools.approval.resolve_gateway_approval``), and
	neither can when the id is lost. ``callbackPrefix`` is the shared convention - ``cl`` for clarify,
	``appr`` for an approval - so the client builds the same callback ids every adapter uses.
	"""
	return {
		"instance": instance,
		"target": target,
		"pluginVersion": PLUGIN_VERSION,
		"kind": kind,
		"body": body,
		"choices": choices or [],
		# An approval has two halves and they are not the same thing: `command` is what would run, `description`
		# is why. A client renders them differently, so they travel separately.
		"command": command,
		"description": description,
		"requestId": request_id,
		"sessionKey": session_key,
		"callbackPrefix": callback_prefix,
		# Seconds left on the hold, when the gateway put one on it. A countdown the client runs from a duration
		# survives the two clocks disagreeing; an absolute time would not.
		"expiresIn": expires_in,
	}


def check_requirements() -> bool:
	"""Always loadable: standard library only, so there is no dependency to be missing."""
	return True


def validate_config(config) -> bool:
	"""True when there is a Familiar to talk to, in ``extra`` or the env.

	A URL is the whole of it. The token arrives by PAIRING, so requiring one here would refuse to load the plugin on
	a machine that has not been paired yet - which is precisely the machine that pairs.
	"""
	return bool(_setting(getattr(config, "extra", {}) or {}, "url", "FAMILIAR_URL"))


def is_connected(config) -> bool:
	"""Configured is connected.

	The gateway enables a plugin platform when this says so (``gateway/config.py``), so it must answer the
	real question - "did the user configure this?" - or the gateway tries to deliver through a channel that
	was never set up and reports failures nobody asked for.
	"""
	return validate_config(config)


# -- The shapes a client parses -------------------------------------------------


def _api_error(message: str, code: str) -> Dict[str, Any]:
	"""Hermes' own error envelope, so a refusal reads the same through either door."""
	return {"error": {"message": message, "type": "invalid_request_error", "param": None, "code": code}}


def _session_payload(session: Dict[str, Any]) -> Dict[str, Any]:
	"""One session, in the shape the instance's API answers with.

	The fields are the API server's own client-safe list (``api_server.py::_session_response``): a full system
	prompt or model config never crosses a client surface, only whether one is there. Copied deliberately and kept
	identical, because the app parses that shape and a second one would drift from it.
	"""
	safe_keys = (
		"id", "source", "user_id", "model", "title", "started_at", "ended_at", "end_reason",
		"message_count", "tool_call_count", "input_tokens", "output_tokens",
		"cache_read_tokens", "cache_write_tokens", "reasoning_tokens", "estimated_cost_usd",
		"actual_cost_usd", "api_call_count", "parent_session_id", "last_active", "preview",
		"_lineage_root_id", "pinned", "archived", "hidden")
	payload = {key: session.get(key) for key in safe_keys if key in session}
	# SQLite stores the flags as 0/1.
	payload.update({flag: bool(payload[flag]) for flag in ("pinned", "archived", "hidden") if flag in payload})
	payload["has_system_prompt"] = bool(session.get("system_prompt"))
	payload["has_model_config"] = bool(session.get("model_config"))

	return payload


def _message_payload(message: Dict[str, Any]) -> Dict[str, Any]:
	"""One message, in the shape the instance's API answers with (``api_server.py::_message_response``).

	Compaction scaffolding is stripped the same way: a standalone handoff becomes a hidden empty row with a stable
	id, and a merged one keeps only the real prior-tail content.
	"""
	from agent.compaction_display import (
		_COMPACTION_INTERNAL_FIELDS, project_compaction_message_for_display)

	projected = project_compaction_message_for_display(message)

	if projected is None:
		projected = {key: value for key, value in message.items() if key not in _COMPACTION_INTERNAL_FIELDS}
		projected["content"] = ""
		projected["display_kind"] = "hidden"

	safe_keys = (
		"id", "session_id", "role", "content", "tool_call_id", "tool_calls", "tool_name",
		"timestamp", "token_count", "finish_reason", "reasoning", "reasoning_content",
		"display_kind")

	return {key: projected.get(key) for key in safe_keys if key in projected}

class FamiliarAdapter(BasePlatformAdapter):
	"""Familiar as a channel: what a person says here reaches the agent, and its asks reach them."""

	MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH

	# A person IS there: Familiar has an ingress now, so a prompt asking whether to resume an interrupted turn
	# lands in front of someone who can answer it. Both flags are read generically by the gateway.
	interactive_resume = True

	# A channel, not a chat app: Familiar cannot EDIT a message it was sent, so it must not be streamed into.
	# The gateway's default here is True, which built a stream consumer for Familiar, pushed the reply into a
	# message nobody could update, and then counted the turn as delivered - so the answer never arrived.
	SUPPORTS_MESSAGE_EDITING = False
	supports_async_delivery = False

	def __init__(self, config: PlatformConfig) -> None:
		super().__init__(config=config, platform=Platform("familiar"))
		extra = config.extra or {}
		self._url: str = _setting(extra, "url", "FAMILIAR_URL", DEFAULT_URL).rstrip("/")
		self._token: str = _setting(extra, "token", "FAMILIAR_TOKEN")
		self._instance: str = _instance_name(extra)
		self._ingress_token: str = _setting(extra, "ingress_token", "FAMILIAR_INGRESS_TOKEN", self._token)
		self._ingress_host: str = _setting(extra, "ingress_host", "FAMILIAR_INGRESS_HOST", "127.0.0.1")
		self._ingress_port: int = int(
			_setting(extra, "ingress_port", "FAMILIAR_INGRESS_PORT", str(DEFAULT_INGRESS_PORT)) or DEFAULT_INGRESS_PORT)
		self._ingress_runner: Optional[Any] = None
		# chat id -> the session key that conversation is routed by. The routing entry is the only thing that
		# knows which SESSION an address is on, and it is the gateway's, so it has to be asked per reply rather
		# than remembered: /new, /reset and a compression rotation all move the key to another session.
		self._session_keys: Dict[str, str] = {}
		# confirm_id -> (session_key, chat_id): what answering a confirmation this adapter raised resolves, and
		# where to say so. The pending hold itself lives in the gateway (``tools.slash_confirm``), so this is
		# only the address of the answer.
		self._confirms: Dict[str, Any] = {}
		# chat key -> the directory this conversation works in. Kept in the process as well as applied, so every
		# later turn of the conversation gets it even though only the ACT of choosing comes through the ingress.
		self._directories: Dict[str, str] = {}
		# The connection this machine holds to Familiar, and the code it is showing while it waits to be paired.
		self._channel_task: Optional[Any] = None
		self._pair_code: str = ""
		# The pairing attempt in flight, if any. One, not one per caller: every attempt mints its own code.
		self._pairing_task: "asyncio.Task[None] | None" = None
		# The lifted HomeChannel is the canonical store; extra and the env are the fallbacks.
		self._home: str = (
			str(getattr(config.home_channel, "chat_id", "") or "")
			or _setting(extra, "home_channel", "FAMILIAR_HOME_CHANNEL", DEFAULT_TARGET)
		)

	# -- Connection lifecycle -----------------------------------------------

	def _presented_token(self) -> str:
		"""The token this machine offers: the one pairing gave it first, then the one it was configured with.

		The pairing comes first because it belongs to THIS install and is newer than anything configured before it.
		A refusal is remembered against the configured token's hint, so one this Familiar has already rejected is
		not offered again - which is the whole difference between a machine that pairs again and one that sits
		holding a dead token, showing no code and no way forward.
		"""
		stored = _load_token()

		if stored:
			return stored

		if self._token and _token_hint(self._token) == _load_refused():
			return ""

		return self._token

	def _take_token(self, token: str) -> None:
		"""The token this machine was paired with, in both directions it is used.

		One credential, presented two ways: the machine proves itself with it when it holds the connection, and its
		own ingress route asks for it when something arrives here. The ingress copy is seeded from the config at
		load, which is EMPTY on a machine that has not paired yet - so a listener that comes up before pairing has to
		be given the token when pairing supplies one, or it keeps refusing its own calls until a restart. An
		explicitly configured ingress token is left alone: that is somebody's deliberate setting.
		"""
		self._token = token

		if not self._ingress_token:
			self._ingress_token = token

	async def connect(self, *, is_reconnect: bool = False) -> bool:
		"""Open the ingress: the one route Familiar posts a person's message to.

		This is the direction that makes Familiar a channel rather than a notifier. The other direction - the
		notifications and the asks - is outbound HTTP, and needs no listener at all.
		"""
		# The CHANNEL is the direction that needs nothing on this machine: it dials out, so a machine that has never
		# been paired still connects - and connecting is how it pairs. The INGRESS needs a token to authenticate with,
		# so a machine without one skips it and says why rather than refusing to load at all.
		started = False

		if self._url and self._channel_task is None:
			self._channel_task = asyncio.create_task(self._channel_loop())
			started = True
		elif not self._url:
			logger.warning("[familiar] no FAMILIAR_URL configured, so there is nothing to connect to")

		if not self._token:
			# The listener comes up BEFORE pairing, which is the point of it: the code this machine shows is read
			# from `GET /familiar/pair`, and whoever reads it is by definition sitting at the machine that has not
			# paired yet. Binding only once a token existed left that reader with nothing to read, while the guide
			# and the app both pointed at it.
			#
			# What this does NOT open: a message. The route authenticates with the token this machine pairs with,
			# and there is none yet, so every call is refused (401) rather than accepted from anyone who reaches
			# loopback. The two GETs are loopback-only by design, which is exactly who the code is for.
			logger.info("[familiar] not paired yet: the channel asks for a code, and its ingress serves it")

		from aiohttp import web

		app = web.Application()
		app.router.add_post(INGRESS_PATH, self._handle_ingress)
		app.router.add_get(INGRESS_PATH, self._handle_ingress_probe)
		# What a person standing at this machine can ask it: the pairing code, or that it is already paired.
		app.router.add_get(PAIR_PATH, self._handle_pair_probe)

		runner = web.AppRunner(app, access_log=None)
		await runner.setup()
		site = web.TCPSite(runner, self._ingress_host, self._ingress_port)
		try:
			await site.start()
		except OSError as error:
			logger.error("[familiar] ingress could not bind %s:%s - %s", self._ingress_host, self._ingress_port, error)
			return False

		self._ingress_runner = runner
		logger.info("[familiar] ingress listening on http://%s:%s%s", self._ingress_host, self._ingress_port, INGRESS_PATH)

		self._mark_connected()

		return True

	async def _machine_name(self) -> str:
		"""What this machine calls itself when it pairs. The instance is named this until somebody renames it."""
		try:
			return socket.gethostname() or "Hermes"

		except Exception:  # noqa: BLE001 - a machine with no name is still a machine
			return "Hermes"

	async def _channel_loop(self) -> None:
		"""Keep a connection to Familiar open, for as long as this machine is running.

		A machine behind NAT cannot be dialled; it can dial. So this holds one outbound connection and everything
		Familiar wants travels down it - no address on this machine, no key, nothing listening.

		A machine that was never paired PAIRS first: it asks for a code and shows it, and the person who can see
		that code is the person sitting here, which is who it is for.
		"""
		delay = RECONNECT_MIN_S

		while True:
			try:
				token = self._presented_token()

				if not token:
					self._pair_start()
					await self._pair_hold()
					delay = RECONNECT_MIN_S

					continue

				self._take_token(token)
				await self._hold_channel(token)
				delay = RECONNECT_MIN_S
			except asyncio.CancelledError:
				raise
			except Exception as error:  # noqa: BLE001 - a connection that drops is retried, never fatal
				logger.warning("[%s] channel dropped: %s", self.name, error)

			await asyncio.sleep(delay)
			delay = min(delay * 2, RECONNECT_MAX_S)

	async def _hold_channel(self, token: str) -> None:
		"""Hold the connection open, and answer what comes down it."""
		import aiohttp

		# No total timeout: this connection is meant to stay open for days, and a deadline on it would be a
		# reconnect every time the deadline passed.
		timeout = aiohttp.ClientTimeout(total=None, sock_read=None)

		async with aiohttp.ClientSession(timeout=timeout) as session:
			async with session.get(
				f"{self._url}{CHANNEL_STREAM_PATH}",
				headers={
					"Authorization": f"Bearer {token}",
					"Accept": "text/event-stream",
					# The build this machine runs, on EVERY connection rather than only when it pairs: the version
					# column is the one thing that answers "did the update land?", and a machine set up over its
					# shell never pairs at all - so a version sent only at pairing would never exist for it.
					"X-Familiar-Plugin": PLUGIN_VERSION,
				},
			) as response:
				if response.status == 401:
					# Not a token this Familiar knows: its instance was deleted, or the token was revoked. Pairing
					# again is the answer, and it has to be reachable from BOTH places a token comes from - otherwise
					# the machine is stuck, which is what a reader hits who deletes an instance and reconnects to a
					# machine that still holds its token.
					stored = _load_token()

					if stored:
						logger.warning("[familiar] this Familiar no longer accepts this machine's token; pairing again")
						_clear_token()
					elif self._token and _token_hint(self._token) != _load_refused():
						logger.warning(
							"[familiar] Familiar does not accept the token this machine was configured with; pairing again"
						)
						_mark_refused(self._token)

					raise RuntimeError(f"the channel refused this machine's token (HTTP {response.status})")

				if response.status != 200:
					raise RuntimeError(f"the channel answered HTTP {response.status}")

				logger.info("[familiar] channel open to %s", self._url)

				event = ""

				async for raw in response.content:
					line = raw.decode("utf-8", "replace").rstrip("\r\n")

					if line.startswith("event: "):
						event = line[7:].strip()
					elif line.startswith("data: ") and event == "request":
						try:
							frame = json.loads(line[6:])
						except json.JSONDecodeError:
							logger.warning("[familiar] a request arrived that could not be read")
							continue

						asyncio.create_task(self._answer_request(frame, token))
					elif not line:
						event = ""

	async def _answer_request(self, frame: Dict[str, Any], token: str) -> None:
		"""Do what Familiar asked down the channel, and answer with the id it came with.

		Three things arrive this way. A SETTING changed in the app goes through the same handler the ingress uses
		- one implementation, two doors, and no second idea of what setting a model means. A MESSAGE is handed to
		this machine's own ingress, and a READ is answered from Hermes' own state. An answer is owed either way,
		because the app is waiting on this id and nothing else will settle it.
		"""
		request_id = str(frame.get("id") or "")
		channel = str(frame.get("channel") or self._home)
		action = str(frame.get("action") or "")

		try:
			if action == "say":
				result = await self._channel_say(frame)
			elif action == "api":
				# A READ rather than a setting: the app asks a PATH instead of an address, so the answer is
				# Hermes' own shape of it.
				result = await self._channel_api(frame)
			else:
				known = await self._apply_action(frame, channel)
				result = {"ok": True} if known else {"error": f"unknown action: {action}"}
		except Exception as error:  # noqa: BLE001 - an answer is owed either way
			logger.warning("[%s] a channel request failed: %s", self.name, error)
			result = {"error": str(error)}

		try:
			await asyncio.to_thread(
				_post, self._url, token, {"id": request_id, "result": result}, CHANNEL_REPLY_PATH)
		except _DeliveryError as error:
			logger.warning("[%s] could not answer a channel request: %s", self.name, error)

	async def _channel_say(self, frame: Dict[str, Any]) -> Dict[str, Any]:
		"""Take a message Familiar sent, into this machine's own ingress.

		A message arriving this way IS the same thing the ingress carries, and it is handed to the same door rather
		than to a second implementation of it: the plugin posts to its own loopback ingress with its own token. So
		there is one idea of what a message from Familiar does, and the path that already works is the path used.

		The connection is the credential for reaching Familiar; inside the machine it is still the instance's own
		token, which this plugin holds.
		"""
		import aiohttp

		message = frame.get("message")

		if not isinstance(message, dict) or not str(message.get("text") or "").strip():
			return {"error": "no message"}

		timeout = aiohttp.ClientTimeout(total=30)

		async with aiohttp.ClientSession(timeout=timeout) as session:
			async with session.post(
				f"http://127.0.0.1:{self._ingress_port}{INGRESS_PATH}",
				headers={"Authorization": f"Bearer {self._ingress_token}"},
				json=message,
			) as response:
				try:
					body = await response.json()
				except Exception:  # noqa: BLE001 - an answer that is not JSON is still an answer
					body = {}

				if response.status >= 400:
					return {"error": f"the ingress answered HTTP {response.status}"}

				return {"sessionId": body.get("sessionId")}

	async def _channel_api(self, frame: Dict[str, Any]) -> Dict[str, Any]:
		"""Answer a path Familiar asked for, from THIS machine's own state.

		A paired machine has no address and no key, so nothing that reads an instance over HTTP applies to it: what
		used to be a request to Hermes' API arrives here as a path. The answer is that API's own shape, because the
		app parses one shape and this is the machine's honest half of it.

		A path this machine does not answer is refused BY NAME: an empty success would read as an empty
		conversation, and the reader would believe it.
		"""
		method = str(frame.get("method") or "GET").upper()
		split = urllib.parse.urlsplit(str(frame.get("path") or ""))
		path = split.path.rstrip("/") or "/"
		query = urllib.parse.parse_qs(split.query)

		for route_method, pattern, handler in _API_ROUTES:
			match = re.fullmatch(pattern, path)

			if match and route_method == method:
				return await getattr(self, handler)(match.groupdict(), query, frame.get("body"))

		return {"status": 501, "body": _api_error(f"this machine does not answer {method} {path}", "unsupported_path")}

	async def _with_session_db(self, work: Any) -> Any:
		"""Run ``work(db)`` off the event loop, against this machine's own session store.

		The store is SQLite, so it never runs on the loop, and the handle goes back to the registry rather than
		being held - which is how the API server opens it too: one store per Hermes home.
		"""
		def run() -> Any:
			from hermes_constants import get_hermes_home
			from hermes_state_registry import acquire, release_or_close

			db = acquire(Path(get_hermes_home()) / "state.db")

			try:
				return work(db)
			finally:
				release_or_close(db)

		return await asyncio.to_thread(run)

	@staticmethod
	def _bounded(query: Dict[str, List[str]], key: str, default: int, maximum: int) -> int:
		"""A page size or an offset, clamped the way the API server clamps it."""
		try:
			value = int((query.get(key) or [str(default)])[0])
		except (TypeError, ValueError):
			return default

		return default if value < 0 else min(value, maximum)

	@staticmethod
	def _no_such_session(session_id: str) -> Dict[str, Any]:
		return {"status": 404, "body": _api_error(f"Session not found: {session_id}", "session_not_found")}

	async def _api_list_sessions(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""GET /api/sessions - this machine's own sessions, most recently active first.

		``search`` is the store's own free-text filter: it matches a session's title or its id ANYWHERE IN ITS
		COMPRESSION CHAIN (so a conversation is found under a name it no longer carries, and ``an94`` finds
		``AN-94``), and it is applied in SQL BEFORE the window - which is the whole reason page two of a search is
		page two of the MATCHES rather than page two of everything. That is also why it is asked for here rather
		than filtered by the caller: a client that filters a page it was given is answering about that page only.

		``total`` is how many rows the filters admit, and it is exact whenever this machine can know it. A listing
		with no search is counted with the store's own count, built from the same WHERE its rows are. A search
		cannot be counted that way - the store counts without a search term and nothing above it counts with one -
		so this machine counts it the only way a search can be counted: by READING the matches its own filter
		admits, whose length IS the answer, up to ``_SEARCH_COUNT_CEILING``. Past that ceiling - and only past it -
		``total`` is ``None``, and a client must show what it has rather than invent a number.
		"""
		limit = self._bounded(query, "limit", 50, 200)
		offset = self._bounded(query, "offset", 0, 1_000_000)
		source = (query.get("source") or [""])[0] or None
		search = (query.get("search") or [""])[0].strip() or None
		include_children = (query.get("include_children") or [""])[0].lower() in ("1", "true", "yes")

		def read(db: Any) -> Dict[str, Any]:
			sessions = db.list_sessions_rich(
				source=source, limit=limit, offset=offset, include_children=include_children,
				order_by_last_active=True, include_pinned=True, search_query=search)
			# Pins are back-filled PAST the limit, so only the recency window decides whether another page exists.
			windowed = sum(1 for session in sessions if not session.get("pinned"))
			has_more = windowed >= limit

			if search and has_more:
				# A search that FILLS the window is the one case with no count anywhere above this machine, so the
				# matches are read: the same filter the rows just came through, asked for compact and pinned
				# included, so what comes back IS the match set and its length is the total rather than a guess.
				matches = db.list_sessions_rich(
					source=source, limit=_SEARCH_COUNT_CEILING + 1, offset=0, include_children=include_children,
					order_by_last_active=True, include_pinned=True, compact_rows=True, search_query=search)
				total = len(matches) if len(matches) <= _SEARCH_COUNT_CEILING else None
			elif search:
				# Short of the window means there is nothing after it: its own length is the answer.
				total = offset + windowed
			else:
				total = db.session_count(source=source, exclude_children=not include_children)

			return {"sessions": sessions, "has_more": has_more, "total": total}

		page = await self._with_session_db(read)

		return {"status": 200, "body": {
			"object": "list", "data": [_session_payload(session) for session in page["sessions"]],
			"limit": limit, "offset": offset, "has_more": page["has_more"], "total": page["total"]}}

	async def _api_get_session(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""GET /api/sessions/{id} - one session, which is the cheap question "has anything changed?"."""
		session = await self._with_session_db(lambda db: db.get_session(groups["session"]))

		if not session:
			return self._no_such_session(groups["session"])

		return {"status": 200, "body": {"object": "hermes.session", "session": _session_payload(session)}}

	async def _api_session_messages(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""GET /api/sessions/{id}/messages - a conversation's transcript, paginated the API's own way."""
		session_id = groups["session"]
		order = (query.get("order") or [None])[0]

		if order not in (None, "oldest", "latest"):
			return {"status": 400, "body": _api_error("order must be one of: oldest, latest", "invalid_pagination")}

		raw_limit = (query.get("limit") or [None])[0]
		raw_offset = (query.get("offset") or ["0"])[0]

		try:
			offset = int(raw_offset)
			requested = None if raw_limit is None else int(raw_limit)
		except (TypeError, ValueError):
			offset = requested = -1

		if offset < 0 or (requested is not None and requested < 0):
			return {"status": 400, "body": _api_error(
				"limit and offset must be non-negative integers", "invalid_pagination")}

		# No limit asked for means the LATEST page, which is what the app reads when it opens a thread.
		default_page = requested is None
		latest_page = order == "latest" or (order is None and default_page)
		limit = 500 if default_page else min(requested, 500)

		def work(db: Any) -> Any:
			if not db.get_session(session_id):
				return None

			# The id asked about resolves to the session that actually holds the messages: a compression rotates
			# the session under a conversation without changing the conversation.
			resolved = db.resolve_resume_session_id(session_id)

			return (resolved, db.get_messages(resolved, limit=limit, offset=offset, latest=latest_page))

		answer = await self._with_session_db(work)

		if answer is None:
			return self._no_such_session(session_id)

		resolved, messages = answer

		return {"status": 200, "body": {
			"object": "list", "session_id": resolved,
			"data": [_message_payload(message) for message in messages],
			"pagination": {"limit": limit, "offset": offset,
				"order": order or ("latest" if default_page else "oldest"), "returned": len(messages)}}}

	async def _api_patch_session(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""PATCH /api/sessions/{id} - what the app changes about a session it did not create: its name.

		The machine's own flags are carried too, because the app relays them; a field this does not know is
		refused by name rather than accepted and ignored.
		"""
		session_id = groups["session"]
		fields = body if isinstance(body, dict) else {}
		unknown = sorted(set(fields) - {"title", "end_reason", "pinned", "archived", "hidden", "unread"})

		if unknown:
			return {"status": 400, "body": _api_error(
				f"Unsupported session fields: {', '.join(unknown)}", "unsupported_session_field")}

		for flag in ("pinned", "archived", "hidden", "unread"):
			if flag in fields and not isinstance(fields[flag], bool):
				return {"status": 400, "body": _api_error(f"'{flag}' must be a boolean", "invalid_session_field")}

		def work(db: Any) -> Any:
			if not db.get_session(session_id):
				return None

			if "title" in fields:
				db.set_session_title(session_id, "" if fields["title"] is None else str(fields["title"]))
			# Pinned last: set_session_pinned clears hidden, so a pin in the same request wins over an explicit
			# hidden - the same order the API server uses.
			for flag, setter in (("archived", db.set_session_archived), ("hidden", db.set_session_hidden),
			                     ("pinned", db.set_session_pinned)):
				if flag in fields:
					setter(session_id, fields[flag])
			if "unread" in fields:
				db.set_session_read(session_id, read=not fields["unread"])
			if fields.get("end_reason"):
				db.end_session(session_id, str(fields["end_reason"]))

			return db.get_session(session_id)

		try:
			session = await self._with_session_db(work)
		except ValueError as error:
			# A name another session already holds: the machine says which one, and it is a 400 rather than a
			# failure of the machine.
			return {"status": 400, "body": _api_error(str(error), "invalid_title")}

		if session is None:
			return self._no_such_session(session_id)

		return {"status": 200, "body": {"object": "hermes.session", "session": _session_payload(session)}}

	def _cron(self) -> Dict[str, Any]:
		"""This machine's own cron store, imported where the API server imports it.

		The plugin is inside Hermes, so the jobs a client creates are the same records the scheduler runs - reached
		directly rather than through an HTTP surface that has to be switched on and keyed first.
		"""
		from cron.jobs import (
			get_job, list_jobs, pause_job, remove_job, resume_job, trigger_job, update_job)
		from cron.scheduler import create_job_with_scheduler_registration

		return {"list": list_jobs, "get": get_job, "create": create_job_with_scheduler_registration,
			"update": update_job, "remove": remove_job, "pause": pause_job, "resume": resume_job,
			"trigger": trigger_job}

	@staticmethod
	def _jobs_changed() -> None:
		"""Tell a co-resident provider the store moved, the way the API's own writes do."""
		with contextlib.suppress(Exception):
			from cron.scheduler import _notify_provider_jobs_changed

			_notify_provider_jobs_changed()

	@staticmethod
	def _no_such_job() -> Dict[str, Any]:
		return {"status": 404, "body": _api_error("Job not found", "job_not_found")}

	@staticmethod
	def _job_prompt_error(prompt: str) -> str:
		"""The API's own prompt guard, so a job made here is held to what one made over HTTP is held to."""
		if len(prompt) > _MAX_JOB_PROMPT:
			return f"Prompt must be ≤ {_MAX_JOB_PROMPT} characters"

		try:
			from tools.cronjob_tools import _scan_cron_prompt
		except Exception:  # noqa: BLE001 - the scanner is optional hardening, never a reason to refuse
			return ""

		return _scan_cron_prompt(prompt) or ""

	async def _api_fork_session(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""POST /api/sessions/{id}/fork - branch a conversation, which is how a topic starts from a session.

		The child is created FIRST and the source ended after it, which is the CLI's own order: a create that fails
		must never leave the source ended with nothing to carry on from.
		"""
		source_id = groups["session"]
		fields = body if isinstance(body, dict) else {}

		def work(db: Any) -> Any:
			source = db.get_session(source_id)

			if not source:
				return None

			fork_id = str(fields.get("id") or fields.get("session_id") or "").strip() or (
				f"{time.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}")

			if db.get_session(fork_id):
				return "exists"

			# `_branched_from` is the durable branch marker: an unmarked child is walked into its parent's
			# lineage and vanishes from a default listing.
			db.create_session(fork_id, "familiar", model=source.get("model"),
				system_prompt=source.get("system_prompt"), parent_session_id=source_id,
				model_config={"_branched_from": source_id})
			db.end_session(source_id, "branched")
			db.replace_messages(fork_id, db.get_messages(source_id))
			title = fields.get("title")
			if title is None:
				base = source.get("title") or "fork"
				title = f"{base} fork"
				with contextlib.suppress(Exception):
					title = db.get_next_title_in_lineage(base)
			db.set_session_title(fork_id, str(title))

			return db.get_session(fork_id) or {"id": fork_id, "parent_session_id": source_id}

		try:
			forked = await self._with_session_db(work)
		except ValueError as error:
			return {"status": 400, "body": _api_error(str(error), "invalid_title")}

		if forked is None:
			return self._no_such_session(source_id)

		if forked == "exists":
			return {"status": 409, "body": _api_error("Session already exists", "session_exists")}

		return {"status": 201, "body": {"object": "hermes.session", "session": _session_payload(forked)}}

	async def _api_delete_session(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""DELETE /api/sessions/{id} - forget a conversation on this machine."""
		session_id = groups["session"]

		def work(db: Any) -> Any:
			if not db.get_session(session_id):
				return None

			return bool(db.delete_session(session_id))

		deleted = await self._with_session_db(work)

		if deleted is None:
			return self._no_such_session(session_id)

		return {"status": 200, "body": {
			"object": "hermes.session.deleted", "id": session_id, "deleted": deleted}}

	async def _api_model_options(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""GET /api/model/options - the model picker's inventory, built where Hermes builds it."""
		refresh = (query.get("refresh") or [""])[0].lower() in ("1", "true", "yes")

		def work() -> Any:
			from hermes_cli.inventory import build_model_options_payload, load_picker_context

			return build_model_options_payload(
				load_picker_context(), include_unconfigured=True, refresh=refresh)

		# Enrichment can fetch pricing and provider catalogs, which is the API server's reason for the thread too.
		return {"status": 200, "body": await asyncio.to_thread(work)}

	async def _api_list_jobs(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""GET /api/jobs - what this machine has scheduled, a page at a time.

		Paged HERE, for the same reason the sessions list is: the app asks for what its table shows, so a machine with
		a hundred jobs answers with a page rather than all of them.

		NOT sorted, and the order is the instance's own. A table that holds one page cannot sort the list - it would
		sort the page and call it the answer - so the app offers no sort control here and nothing is asked for one,
		the same way the sessions and topics lists work. This machine decides the order it runs things in.
		"""
		include_disabled = (query.get("include_disabled") or [""])[0].lower() in ("true", "1")
		limit = self._bounded(query, "limit", 50, 200)
		offset = self._bounded(query, "offset", 0, 1_000_000)
		search = (query.get("search") or [""])[0].strip().lower()

		jobs = await asyncio.to_thread(
			lambda: self._cron()["list"](include_disabled=include_disabled) or [])

		if search:
			jobs = [
				job for job in jobs
				if search in str(job.get("name") or "").lower() or search in str(job.get("prompt") or "").lower()
			]

		page = jobs[offset:offset + limit]

		return {"status": 200, "body": {
			"object": "list", "data": page, "limit": limit, "offset": offset,
			"has_more": offset + len(page) < len(jobs), "total": len(jobs)}}

	async def _api_get_job(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""GET /api/jobs/{id} - one job, as the store holds it."""
		job = await asyncio.to_thread(self._cron()["get"], groups["job"])

		return {"status": 200, "body": {"job": job}} if job else self._no_such_job()

	async def _api_create_job(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""POST /api/jobs - schedule something on this machine.

		The job's ORIGIN is this channel, so `deliver: origin` means "wherever Familiar is" rather than an API
		server that answers nobody - the same intent a reader picks in the app, on the machine's own terms.
		"""
		fields = body if isinstance(body, dict) else {}
		name = str(fields.get("name") or "").strip()
		schedule = str(fields.get("schedule") or "").strip()
		prompt = fields.get("prompt") or ""

		if not name:
			return {"status": 400, "body": _api_error("Name is required", "invalid_job")}

		if len(name) > _MAX_JOB_NAME:
			return {"status": 400, "body": _api_error(
				f"Name must be ≤ {_MAX_JOB_NAME} characters", "invalid_job")}

		if not schedule:
			return {"status": 400, "body": _api_error("Schedule is required", "invalid_job")}

		problem = self._job_prompt_error(str(prompt))

		if problem:
			return {"status": 400, "body": _api_error(problem, "invalid_job")}

		repeat = fields.get("repeat")

		if repeat is not None and (not isinstance(repeat, int) or repeat < 1):
			return {"status": 400, "body": _api_error("Repeat must be a positive integer", "invalid_job")}

		kwargs: Dict[str, Any] = {
			"prompt": prompt, "schedule": schedule, "name": name,
			"deliver": fields.get("deliver") or "local",
			# The PLATFORM, not the label: a delivery resolves a platform by this value, and "Familiar" is
			# what the row is titled.
			"origin": {"platform": self.platform.value, "chat_id": self._home},
		}

		for key in ("paused", "paused_reason"):
			if key in fields:
				kwargs[key] = fields[key]

		if fields.get("skills"):
			kwargs["skills"] = fields["skills"]

		if repeat is not None:
			kwargs["repeat"] = repeat

		try:
			job = await asyncio.to_thread(lambda: self._cron()["create"](**kwargs))
		except ValueError as error:
			return {"status": 400, "body": _api_error(str(error), "invalid_job")}
		except Exception as error:  # noqa: BLE001 - a store that refuses says why, in its own words
			# A scheduler that cannot register the job is a 424 on the API server, and the store's own code is
			# what carries the reason - so it is passed through rather than flattened.
			status = int(getattr(error, "status", 500) or 500)

			return {"status": status, "body": _api_error(str(error), "job_create_failed")}

		return {"status": 200, "body": {"job": job}}

	async def _api_update_job(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""PATCH /api/jobs/{id} - what a reader changed about a job."""
		fields = body if isinstance(body, dict) else {}
		# Whitelisted the way the API server whitelists it: a field nobody may set is not silently stored.
		sanitized = {key: value for key, value in fields.items() if key in _JOB_UPDATE_FIELDS}

		if not sanitized:
			return {"status": 400, "body": _api_error("No valid fields to update", "invalid_job")}

		if "name" in sanitized and len(str(sanitized["name"])) > _MAX_JOB_NAME:
			return {"status": 400, "body": _api_error(
				f"Name must be ≤ {_MAX_JOB_NAME} characters", "invalid_job")}

		if "prompt" in sanitized:
			problem = self._job_prompt_error(str(sanitized["prompt"] or ""))

			if problem:
				return {"status": 400, "body": _api_error(problem, "invalid_job")}

		def work() -> Any:
			job = self._cron()["update"](groups["job"], sanitized)

			if job:
				self._jobs_changed()

			return job

		job = await asyncio.to_thread(work)

		return {"status": 200, "body": {"job": job}} if job else self._no_such_job()

	async def _api_delete_job(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""DELETE /api/jobs/{id} - take a job off the schedule."""
		def work() -> Any:
			removed = self._cron()["remove"](groups["job"])

			if removed:
				self._jobs_changed()

			return removed

		return {"status": 200, "body": {"ok": True}} if await asyncio.to_thread(work) else self._no_such_job()

	async def _api_job_action(
			self, groups: Dict[str, str], query: Dict[str, List[str]], body: Any) -> Dict[str, Any]:
		"""POST /api/jobs/{id}/run|pause|resume - one job, acted on now."""
		action = groups["action"]
		job_id = groups["job"]

		if not _JOB_ID_RE.fullmatch(job_id):
			return {"status": 400, "body": _api_error("Invalid job ID format", "invalid_job_id")}

		fields = body if isinstance(body, dict) else {}
		extra: Optional[str] = None

		if action == "run" and fields.get("prompt") is not None:
			extra = str(fields["prompt"])
			problem = self._job_prompt_error(extra)

			if problem:
				return {"status": 400, "body": _api_error(problem, "invalid_job")}

			extra = extra or None

		def work() -> Any:
			store = self._cron()

			if action == "run":
				job = store["trigger"](job_id, extra_prompt=extra)
			else:
				job = (store["pause"] if action == "pause" else store["resume"])(job_id)

			if job and action != "run":
				self._jobs_changed()

			return job

		job = await asyncio.to_thread(work)

		return {"status": 200, "body": {"job": job}} if job else self._no_such_job()

	def _pair_start(self) -> None:
		"""Start a pairing attempt, unless one is already in flight.

		ONE, not one per caller. Every attempt mints its own code, so two attempts would leave this machine showing
		one code while the log and Familiar talked about the other, and the one a reader typed could be the one the
		wait behind it had already given up on.
		"""
		if self._pairing_task is None or self._pairing_task.done():
			self._pairing_task = asyncio.create_task(self._pair())
			self._pairing_task.add_done_callback(self._pair_attempt_finished)

	async def _pair_hold(self) -> None:
		"""Wait on the attempt in flight, whoever started it. Returns the moment it is done.

		Not shielded: this is called from the channel loop, whose own cancellation - a gateway restart, a plugin
		unload - is the signal to stop asking, and the attempt belongs to that loop's lifetime.
		"""
		if self._pairing_task is not None:
			await self._pairing_task

	def _pair_attempt_finished(self, task: Any) -> None:
		"""Read an attempt's outcome where it happened, so a failure is reported rather than logged later.

		A task whose exception nobody reads is reported as an unretrieved exception at collection time, which names
		neither the reason nor the machine - and the code an attempt produces is only ever read by a person looking
		at the log.
		"""
		if task.cancelled():
			return

		error = task.exception()

		if error is not None:
			logger.warning("[familiar] pairing could not be started: %s", error)

	async def _pair(self) -> None:
		"""Ask Familiar for a code, show it, and wait for it to be claimed.

		The code is logged, and served on this machine's own loopback, because whoever can see either is whoever is
		sitting at the machine - which is exactly who the code is for. Nothing is created on the other end until
		they type it, so asking costs nothing and leaves nothing behind.
		"""
		import aiohttp

		timeout = aiohttp.ClientTimeout(total=None, sock_read=None)

		async with aiohttp.ClientSession(timeout=timeout) as session:
			started = await session.post(
				f"{self._url}{CHANNEL_PAIR_PATH}",
				json={"name": await self._machine_name(), "version": PLUGIN_VERSION},
			)
			body = await started.json() if started.status == 200 else {}
			code = str(body.get("code") or "")
			secret = str(body.get("secret") or "")

			if not code or not secret:
				raise RuntimeError(f"pairing was refused (HTTP {started.status})")

			self._pair_code = code
			logger.warning("[familiar] PAIR: enter the code %s in Familiar, and this machine connects", code)

			async with session.get(f"{self._url}{CHANNEL_PAIR_WAIT_PATH}?secret={secret}") as response:
				event = ""

				async for raw in response.content:
					line = raw.decode("utf-8", "replace").rstrip("\r\n")

					if line.startswith("event: "):
						event = line[7:].strip()
					elif line.startswith("data: ") and event == "paired":
						try:
							paired = json.loads(line[6:])
						except json.JSONDecodeError:
							continue

						token = str(paired.get("token") or "")

						if token:
							_save_token(token)
							self._take_token(token)
							self._pair_code = ""
							logger.info("[familiar] paired: this machine is reachable through the channel now")

						return

	async def disconnect(self) -> None:
		if self._channel_task is not None:
			self._channel_task.cancel()

			with contextlib.suppress(asyncio.CancelledError, Exception):
				await self._channel_task

			self._channel_task = None

		if self._ingress_runner is not None:
			await self._ingress_runner.cleanup()
			self._ingress_runner = None

		self._mark_disconnected()

	# -- Outbound ------------------------------------------------------------

	async def send(
		self,
		chat_id: str,
		content: str,
		reply_to: Optional[str] = None,
		metadata: Optional[Dict[str, Any]] = None,
	) -> SendResult:
		"""Send a reply into a Familiar conversation. ``chat_id`` is the conversation; the default is the
		instance's own inbox."""
		if not self._token:
			return SendResult(success=False, error="FAMILIAR_TOKEN is not configured")

		payload = _payload(self._instance, chat_id or self._home, content, metadata)
		payload["sessionId"] = self._current_session_id(chat_id)
		try:
			# MESSAGE_PATH, not the notification path: this is a reply in a conversation, and it would be wrong
			# in the list an operator reads job output from. A cron delivery still uses the notification path,
			# through standalone_sender_fn.
			body = await asyncio.to_thread(_post, self._url, self._token, payload, MESSAGE_PATH)
		except _DeliveryError as error:
			logger.warning("[%s] Delivery failed: %s", self.name, error)
			return SendResult(success=False, error=str(error), retryable=error.retryable)
		return SendResult(success=True, message_id=str(body["id"]) if body.get("id") else None)
	async def _handle_ingress_probe(self, request: Any) -> Any:
		"""A health probe: whether this instance can be reached at all."""
		from aiohttp import web

		return web.json_response({"ok": True, "platform": "familiar"})

	async def _handle_pair_probe(self, request: Any) -> Any:
		"""The code this machine is showing, or that it is already paired.

		On loopback only, like the rest of the ingress: whoever can reach this is whoever is sitting at the machine,
		which is exactly who the code is for.

		Asked while this machine has neither a token nor a code, it starts asking and waits for the code to arrive
		instead of answering "no code" and leaving the reader to work out that the command wants running again -
		which is not something the reader can know. The wait is bounded, so a Familiar nobody can reach answers
		without a code, which is then the truth rather than a stall.
		"""
		from aiohttp import web

		if not self._presented_token():
			self._pair_start()

			loop = asyncio.get_running_loop()
			deadline = loop.time() + PAIR_PROBE_WAIT_S

			while not self._pair_code:
				if loop.time() >= deadline or (self._pairing_task is not None and self._pairing_task.done()):
					break

				await asyncio.sleep(PAIR_PROBE_TICK_S)

		return web.json_response({
			"paired": bool(self._presented_token()),
			"code": self._pair_code or None,
			"version": PLUGIN_VERSION,
		})

	async def _handle_ingress(self, request: Any) -> Any:
		"""A message from Familiar, turned into the event the gateway runs.

		One route carries everything a person does: what they typed, a tapped choice on a clarify question, and
		an approval decision. The gateway decides which - a message that arrives while a run is parked goes to
		the clarify intercept, and a callback id is resolved by it - so this only has to hand the text over.
		"""
		from aiohttp import web

		# The same token Familiar issues for deliveries, presented the other way round. Neither direction
		# authenticates the other: the instance is the one proving who it is here.
		presented = request.headers.get("Authorization", "")
		expected = self._ingress_token or self._token

		if not expected or presented != f"Bearer {expected}":
			logger.warning("[familiar] refused an ingress call: bad or missing token")
			return web.json_response({"error": "unauthorized"}, status=401)

		try:
			payload = await request.json()
		except Exception as error:  # noqa: BLE001 - a malformed body is the caller's problem, not the gateway's
			logger.warning("[familiar] ingress parse error: %s", error)
			return web.json_response({"error": "invalid payload"}, status=400)

		text = str(payload.get("text") or "").strip()
		chat_id = str(payload.get("channel") or payload.get("chatId") or self._home or DEFAULT_TARGET)

		# Something about the conversation rather than something said in it - a setting Familiar changed, which the
		# instance has to be told. It is not a turn, so it does not become one.
		if str(payload.get("action") or "").strip():
			await self._apply_action(payload, chat_id)

			return web.json_response({"ok": True})

		if not text:
			return web.json_response({"error": "missing text"}, status=400)

		# An answer to a confirmation this adapter raised. It is not something the reader SAID, so it must not
		# become a turn: it belongs to the hold, and it is resolved here.
		if text.startswith(CONFIRM_PREFIX):
			await self._resolve_confirm(text, chat_id)

			return web.json_response({"ok": True})

		source = self.build_source(
			chat_id=chat_id,
			chat_name=str(payload.get("channelName") or chat_id),
			chat_type="dm",
			user_id=str(payload.get("userId") or "familiar"),
			user_name=str(payload.get("userName") or "Familiar"),
		)
		event = MessageEvent(
			text=text,
			message_type=MessageType.TEXT,
			source=source,
			raw_message=payload,
			message_id=str(payload.get("messageId") or "") or None,
		)

		# A missing handler makes handle_message a silent no-op, and this endpoint answers "accepted" either
		# way - so say so instead of accepting a message that can never become a turn.
		if not getattr(self, "_message_handler", None):
			logger.error("[familiar] a message arrived but the gateway never registered a message handler for "
			             "this adapter, so no turn can start")
			return web.json_response({"accepted": False, "reason": "no message handler"}, status=503)

		# Which session this conversation is on, resolved here rather than left to the client: Familiar minted
		# the ADDRESS and the gateway mints the session id, and nothing outside this process can map one to the
		# other. It rides this response, so a client knows its conversation's session from the first message.
		session_id = await self._resolve_session(source, declared=str(payload.get("sessionId") or ""))

		# Dispatched as a task, as every adapter does it: the turn can take minutes, and the caller - Familiar -
		# is waiting on a prompt HTTP response, not on the agent.
		# The directory this conversation was given, applied again now that its session is resolved: a
		# gateway that restarted would otherwise run this turn wherever IT was launched from. Applied before
		# the turn is scheduled, because the turn is what reads it.
		key = self._session_keys.get(str(chat_id))
		if key:
			self._apply_directory_now(key)

		task = asyncio.create_task(self.handle_message(event))

		def _report(task: "asyncio.Task") -> None:
			self._background_tasks.discard(task)
			if not task.cancelled() and task.exception() is not None:
				logger.error("[familiar] the turn from the ingress failed: %s", task.exception())

		self._background_tasks.add(task)
		task.add_done_callback(_report)

		return web.json_response({"accepted": True, "messageId": event.message_id, "sessionId": session_id})

	async def _resolve_session(self, source: Any, declared: str = "") -> str:
		"""The session this conversation is on, creating it when the conversation has not started yet.

		An address is not a session: Familiar mints the chat id, the gateway mints the session id the first time a
		key is used, and the two are never equal. This is the only place that can say which session an address is
		on, so it says it back to whoever asked.

		``declared`` is a session the client already holds and means this conversation to continue - a session it
		forked, most of all. It is ADOPTED rather than replaced: the key is pointed at it, which is the same move
		``/resume`` makes, and the reason a fork needs the channel to adopt it before its first message can carry
		its context.
		"""
		store = getattr(self, "_session_store", None)
		if store is None:
			return ""

		try:
			entry = await asyncio.to_thread(store.get_or_create_session, source)
			if declared and entry.session_id != declared:
				# Order matters: switch_session only re-points a key that already exists, so the entry is made
				# first and the switch ends it rather than leaving two live sessions for one conversation.
				switched = await asyncio.to_thread(store.switch_session, entry.session_key, declared)
				if switched is not None:
					entry = switched
			self._session_keys[str(source.chat_id)] = entry.session_key
			return entry.session_id
		except Exception as error:  # noqa: BLE001 - a session nobody can name must not cost the turn
			logger.warning("[familiar] could not resolve the session for %s: %s", source.chat_id, error)
			return ""

	def _current_session_id(self, chat_id: str) -> str:
		"""The session a conversation is on NOW, or "" when nothing here has routed it.

		Read per reply rather than remembered from the ingress, so a rotation that happened DURING the turn -
		``/new``, ``/reset``, a compression that moved the conversation - is already reflected in what the client
		is told. That is what lets a topic follow its conversation instead of being pinned to a dead session id.
		"""
		key = self._session_keys.get(str(chat_id or self._home))
		store = getattr(self, "_session_store", None)
		if not key or store is None:
			return ""
		try:
			return store.peek_session_id(key) or ""
		except Exception:  # noqa: BLE001 - a reply without a session id is still a reply
			return ""

	async def send_clarify(
		self,
		chat_id: str,
		question: str,
		choices: Optional[List[str]] = None,
		clarify_id: str = "",
		session_key: str = "",
		**kwargs: Any,
	) -> SendResult:
		"""Carry a clarify question to Familiar with the id its answer must come back with.

		The gateway is blocked on this: the next inbound message from Familiar is routed to the clarify text
		intercept, and a tap on one of the choices resolves it through ``resolve_gateway_clarify``. Nothing is
		resolved here - this only gets the question out, and the id with it.
		"""
		if not self._token:
			return SendResult(success=False, error="FAMILIAR_TOKEN is not configured")

		payload = _ask_payload(
			self._instance, chat_id or self._home, "clarify", question, choices, clarify_id, session_key, "cl")
		try:
			await asyncio.to_thread(_post, self._url, self._token, payload, ASK_PATH)
		except _DeliveryError as error:
			logger.warning("[%s] Clarify failed: %s", self.name, error)
			return SendResult(success=False, error=str(error), retryable=error.retryable)

		return SendResult(success=True, message_id=clarify_id or None)

	async def send_slash_confirm(
		self,
		chat_id: str,
		title: str = "",
		message: str = "",
		session_key: str = "",
		confirm_id: str = "",
		**kwargs: Any,
	) -> SendResult:
		"""Carry the gateway's own confirmation to Familiar, and remember what answering it resolves.

		The gateway holds a command it will not take back - starting a fresh session, clearing, an undo - until
		this is answered. Familiar shows it as a decision of its own rather than as part of a turn, so the reader
		gets Approve and Cancel, and no "always": that is a persisted gateway setting, and a setting belongs to a
		settings page rather than to a question asked in a chat.

		The hold EXPIRES, and the remaining time travels with the ask - read from the gateway's own clock rather
		than assumed, so the countdown the reader sees is the one that is actually running.
		"""
		if not self._token:
			return SendResult(success=False, error="FAMILIAR_TOKEN is not configured")

		command = title
		expires_in: Optional[int] = None
		try:
			from tools import slash_confirm as _slash_confirm

			pending = _slash_confirm.get_pending(session_key)
			if pending and str(pending.get("confirm_id") or "") == str(confirm_id or ""):
				command = str(pending.get("command") or "") or title
				elapsed = time.time() - float(pending.get("created_at") or 0)
				expires_in = max(0, int(_slash_confirm.DEFAULT_TIMEOUT_SECONDS - elapsed))
		except Exception as error:  # noqa: BLE001 - an ask without a clock on it still beats no ask
			logger.debug("[%s] could not read the confirmation's remaining time: %s", self.name, error)

		self._confirms[str(confirm_id)] = (session_key, chat_id or self._home)
		payload = _ask_payload(
			self._instance,
			chat_id or self._home,
			"confirm",
			message,
			["once", "cancel"],
			confirm_id,
			session_key,
			"cf",
			command=command if command.startswith("/") else (f"/{command}" if command else None),
			expires_in=expires_in,
		)
		try:
			await asyncio.to_thread(_post, self._url, self._token, payload, ASK_PATH)
		except _DeliveryError as error:
			logger.warning("[%s] Confirmation failed: %s", self.name, error)
			return SendResult(success=False, error=str(error), retryable=error.retryable)

		return SendResult(success=True, message_id=str(confirm_id) or None)

	async def _apply_action(self, payload: Dict[str, Any], chat_id: str) -> bool:
		"""Apply a setting Familiar changed, without it becoming something said in the conversation.

		Two so far: which model a conversation runs on, and where it works. Both are kept under the CHAT key rather
		than a session id, and that is the whole point - the key is what survives ``/new``, ``/reset`` and a
		compression, so a setting chosen for a topic is still the setting when that topic starts a fresh session.
		"""
		action = str(payload.get("action") or "").strip()
		# A table rather than a chain of comparisons: a new setting is a line here and a method beside the others.
		handler = {"model": self._apply_model, "directory": self._apply_directory}.get(action)

		if handler is None:
			logger.warning("[%s] ignoring an action it does not know: %s", self.name, action)

			return False

		key = self._chat_key(str(chat_id))

		if not key:
			return False

		handler(key, payload)

		return True

	def _chat_key(self, chat_id: str) -> str:
		"""The routing key for a conversation: what a setting is kept under, and what outlives a session."""
		key = self._session_keys.get(chat_id)
		store = getattr(self, "_session_store", None)

		if key:
			return key

		if store is None:
			logger.warning("[%s] no session store, so nothing can be set on a conversation here", self.name)

			return ""

		# Nothing has been said here yet, so the key is minted the way the first message would have minted it.
		try:
			source = self.build_source(
				chat_id=chat_id, chat_name=chat_id, chat_type="dm", user_id="familiar", user_name="Familiar")
			key = store.get_or_create_session(source).session_key
			self._session_keys[chat_id] = key

			return key
		except Exception as error:  # noqa: BLE001 - a setting that cannot land is worth a warning, not a crash
			logger.warning("[%s] could not resolve the conversation: %s", self.name, error)

			return ""

	def _apply_model(self, key: str, payload: Dict[str, Any]) -> None:
		"""Point the conversation at a model. Empty clears it, which is the instance's own default."""
		store = getattr(self, "_session_store", None)
		model = str(payload.get("model") or "").strip()

		if store is None:
			return

		# None clears it, which is how a conversation goes back to the instance's own default.
		store.set_model_override(key, {"model": model} if model else None)
		logger.info("[%s] model for %s is now %s", self.name, key, model or "(the instance's default)")

	def _apply_directory(self, key: str, payload: Dict[str, Any]) -> None:
		"""Where the conversation works: applied to this key's turns, and written on the session row.

		Both halves are needed, and Hermes itself keeps it this way. The task override is what a running turn's
		terminal actually reads, and it is keyed by the CHAT key - so the directory is still the directory after a
		new session. The session row is the durable record: it survives a restart, and a fork or a compression
		inherits it.
		"""
		directory = str(payload.get("directory") or "").strip()

		if not directory:
			self._directories.pop(key, None)

			return

		self._directories[key] = directory
		self._apply_directory_now(key)

	def _apply_directory_now(self, key: str) -> None:
		"""Apply the directory this conversation was given. Safe to call on every turn: it is idempotent, and the
		one it is called for is the one a new session would otherwise forget."""
		directory = self._directories.get(key)

		if not directory:
			return

		try:
			from tools.terminal_tool import register_task_env_overrides

			# "session", not "process": this is a workspace the reader picked, and the process's own cwd is where
			# the gateway happened to be launched - the value terminal_tool refuses for exactly that reason.
			register_task_env_overrides(key, {"cwd": directory, "cwd_source": "session"})
		except Exception as error:  # noqa: BLE001 - a directory that cannot be applied still belongs to the topic
			logger.warning("[%s] could not apply the working directory: %s", self.name, error)

		try:
			from pathlib import Path

			from hermes_constants import get_hermes_home
			from hermes_state_registry import acquire, release_or_close

			db = acquire(Path(get_hermes_home()) / "state.db")
			try:
				if db is not None:
					db.update_session_cwd(key, directory)
			finally:
				release_or_close(db)
		except Exception as error:  # noqa: BLE001 - the live half already landed, so this is the durable one
			logger.warning("[%s] could not persist the working directory: %s", self.name, error)

	async def _resolve_confirm(self, text: str, chat_id: str) -> None:
		"""Resolve the confirmation the reader answered, and say what happened either way.

		Answering it here rather than handing it to the gateway is deliberate: the hold is module state inside
		this process, and the outcome of resolving it is a reply in this conversation. An expired hold resolves
		to nothing, and the honest answer to a tap that arrived too late is that nothing was done.
		"""
		parts = text.split(":", 2)
		confirm_id = parts[1] if len(parts) > 1 else ""
		choice = parts[2] if len(parts) > 2 else ""
		remembered = self._confirms.pop(confirm_id, None)
		if not remembered:
			await self.send(chat_id, "That confirmation is no longer waiting, so nothing was done.")

			return
		session_key, _ = remembered
		try:
			from tools import slash_confirm as _slash_confirm

			answer = await _slash_confirm.resolve(session_key, confirm_id, choice)
		except Exception as error:  # noqa: BLE001 - a failed resolve is still an answer the reader needs
			logger.warning("[%s] Could not resolve the confirmation: %s", self.name, error)
			answer = None
		await self.send(chat_id, answer or "That confirmation is no longer waiting, so nothing was done.")

	async def send_exec_approval(
		self,
		chat_id: str,
		command: str,
		session_key: str = "",
		description: str = "",
		**kwargs: Any,
	) -> SendResult:
		"""Carry a command approval to Familiar. Its answer goes to ``resolve_gateway_approval``."""
		if not self._token:
			return SendResult(success=False, error="FAMILIAR_TOKEN is not configured")

		payload = _ask_payload(
			self._instance,
			chat_id or self._home,
			"approval",
			description or command,
			kwargs.get("choices") or ["once", "deny"],
			str(kwargs.get("request_id") or ""),
			session_key,
			"appr",
			command=command,
			description=description,
		)
		try:
			await asyncio.to_thread(_post, self._url, self._token, payload, ASK_PATH)
		except _DeliveryError as error:
			logger.warning("[%s] Approval failed: %s", self.name, error)
			return SendResult(success=False, error=str(error), retryable=error.retryable)

		return SendResult(success=True, message_id=str(kwargs.get("request_id") or "") or None)


	async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
		return {"name": chat_id or self._home, "type": "dm"}


# -- Plugin registration -----------------------------------------------------


def _env_enablement() -> Optional[dict]:
	"""Seed ``PlatformConfig.extra`` from env vars during gateway config load.

	``None`` when there is nowhere to talk to: a platform nobody configured must not be enabled, or the gateway
	delivers into it and reports failures for a channel the user never asked for. The ``home_channel`` key
	is lifted into a ``HomeChannel`` on the ``PlatformConfig`` instead of being merged into ``extra``.

	A URL is the whole of "configured" now. The token arrives by PAIRING, so a machine that has only been pointed
	at a Familiar still loads - it shows a code, and whoever is sitting at it types that into the app.
	"""
	url = (_get_scoped_secret("FAMILIAR_URL", "") or "").strip().rstrip("/")
	token = (_get_scoped_secret("FAMILIAR_TOKEN", "") or "").strip()

	if not url and not token:
		return None

	seed: Dict[str, Any] = {"url": url or DEFAULT_URL}

	if token:
		seed["token"] = token
	instance = (_get_scoped_secret("FAMILIAR_INSTANCE", "") or "").strip()
	if instance:
		seed["instance"] = instance
	home = (_get_scoped_secret("FAMILIAR_HOME_CHANNEL", "") or "").strip() or DEFAULT_TARGET
	seed["home_channel"] = {"chat_id": home, "name": "Familiar"}
	return seed


async def _standalone_send(
	pconfig,
	chat_id: str,
	message: str,
	*,
	thread_id: Optional[str] = None,
	media_files: Optional[list] = None,
	force_document: bool = False,
) -> Dict[str, Any]:
	"""Out-of-process delivery, for a cron tick with no gateway holding this adapter.

	The same POST as the adapter's, minus the job id: this signature carries no metadata, so the standalone
	path cannot say which job spoke. A normal install delivers through the live adapter instead; this path
	is what keeps a headless tick working.
	"""
	extra = getattr(pconfig, "extra", {}) or {}
	url = _setting(extra, "url", "FAMILIAR_URL", DEFAULT_URL).rstrip("/")
	token = _setting(extra, "token", "FAMILIAR_TOKEN")
	if not token:
		return {"error": "Familiar delivery: FAMILIAR_TOKEN is not configured"}
	home = getattr(pconfig, "home_channel", None)
	target = chat_id or str(getattr(home, "chat_id", "") or "") or _setting(
		extra, "home_channel", "FAMILIAR_HOME_CHANNEL", DEFAULT_TARGET)
	payload = _payload(_instance_name(extra), target, message, None)
	try:
		body = await asyncio.to_thread(_post, url, token, payload)
	except _DeliveryError as error:
		return {"error": f"Familiar delivery failed: {error}"}
	return {
		"success": True,
		"platform": "familiar",
		"chat_id": payload["target"],
		"message_id": str(body["id"]) if body.get("id") else None,
	}

def register(ctx) -> None:
	"""Plugin entry point, called once at startup."""
	ctx.register_platform(
		name="familiar",
		label="Familiar",
		adapter_factory=lambda cfg: FamiliarAdapter(cfg),
		check_fn=check_requirements,
		validate_config=validate_config,
		is_connected=is_connected,
		required_env=[],
		install_hint="Nothing to install: standard library only",
		env_enablement_fn=_env_enablement,
		# This is the line that makes `deliver: familiar` a job target: the scheduler reads the home channel
		# from this env var name when a job asks for the bare platform.
		cron_deliver_env_var="FAMILIAR_HOME_CHANNEL",
		# Who may speak to the agent through this channel. Without these the gateway reads every inbound message as
		# an unknown user and refuses it - "Unauthorized user: familiar (Familiar)" - which is exactly what it was
		# doing, so a message arrived and then went nowhere.
		allowed_users_env="FAMILIAR_ALLOWED_USERS",
		allow_all_env="FAMILIAR_ALLOW_ALL_USERS",
		standalone_sender_fn=_standalone_send,
		# 0 = the router does not chunk: one delivery becomes one notification, and the adapter truncates
		# rather than turning a long brief into a burst of them.
		max_message_length=0,
		emoji="🛰️",
	)

