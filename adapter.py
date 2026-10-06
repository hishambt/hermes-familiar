"""Familiar platform adapter for Hermes: delivery only, over HTTP.

Familiar is a client for Hermes instances. It reads an instance's sessions and jobs over the instance's own
HTTP API, but a cron job's OUTPUT is pushed and never read: the Jobs API carries status alone. This adapter
is the push side, and it is the whole reason the plugin exists.

Registering a platform with a ``cron_deliver_env_var`` is what makes ``deliver: familiar`` a valid job
target (``cron/scheduler_delivery.py`` -> ``_is_known_delivery_platform``), and ``send()`` is the one method
the delivery path needs. So a job's output lands in Familiar instead of in a file on the instance that no
client can read.

Nothing arrives over it. Familiar talks to the instance on the instance's API, so this platform has no
inbound path and never carries a turn: ``interactive_resume`` and ``supports_async_delivery`` are both
False, and no ``platform_hint`` is set because no session ever runs here.

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
import asyncio
import contextlib
import json
import logging
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

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

#: Where a paired machine keeps the token it was given. Beside the plugin, because it belongs to the INSTALL and
#: not to a Hermes config: nothing a reader edits by hand should hold a credential that arrives by pairing.
STATE_FILE = "state.json"

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


def _state_path() -> Path:
	return Path(__file__).resolve().parent / STATE_FILE


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
			"User-Agent": f"hermes-familiar (instance:{payload['instance']})",
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
		# The lifted HomeChannel is the canonical store; extra and the env are the fallbacks.
		self._home: str = (
			str(getattr(config.home_channel, "chat_id", "") or "")
			or _setting(extra, "home_channel", "FAMILIAR_HOME_CHANNEL", DEFAULT_TARGET)
		)

	# -- Connection lifecycle -----------------------------------------------

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
			logger.info("[familiar] not paired yet: the channel asks for a code, and the ingress waits for one")

			if started:
				self._mark_connected()

			return started

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
				token = self._token or _load_token()

				if not token:
					await self._pair()
					delay = RECONNECT_MIN_S

					continue

				self._token = token
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
				headers={"Authorization": f"Bearer {token}", "Accept": "text/event-stream"},
			) as response:
				if response.status == 401:
					# Not a token this Familiar knows: revoked, or this machine was unpaired elsewhere. Pairing again
					# is the answer - but only for a token WE stored, because one from the environment is somebody's
					# deliberate configuration and clearing it would fight them.
					if _load_token():
						logger.warning("[familiar] this Familiar no longer accepts this machine's token; pairing again")
						_clear_token()

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

		What arrives this way is the same thing the ingress carries - a setting changed in the app - so it goes
		through the same handler: one implementation, two doors, and no second idea of what setting a model means.
		An answer is owed either way, because the app is waiting on this id and nothing else will settle it.
		"""
		request_id = str(frame.get("id") or "")
		channel = str(frame.get("channel") or self._home)
		action = str(frame.get("action") or "")

		try:
			if action == "say":
				result = await self._channel_say(frame)
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
							self._token = token
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
		"""
		from aiohttp import web

		return web.json_response({
			"paired": bool(self._token),
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
		if not self._ingress_token or presented != f"Bearer {self._ingress_token}":
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

