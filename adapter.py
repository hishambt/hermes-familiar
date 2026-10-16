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

This file is the CONNECTION half: holding the channel, pairing, delivering, and the ingress a person's message
arrives at. The reads and actions a client asks this machine for - sessions, conversations, jobs - are in
``answers.py``, which the class below inherits. They were one file, and the answer half outgrew the other.
"""

from __future__ import annotations

import socket
import time
import asyncio
import contextlib
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

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

#: What a paired machine's state is called. Where it lives is `_state_path`.
STATE_FILE = "state.json"

#: How long a writer waits for another profile to finish with the state file. Every write is a few hundred bytes, so
#: this is not a wait anybody reaches by waiting.
LOCK_WAIT_S = 10.0
LOCK_POLL_S = 0.02
#: A lock file older than this is one whose owner died holding it.
STALE_LOCK_S = 30.0
#: Attempts at moving a finished document into place. A reader holding the file open refuses the move on Windows.
MOVE_TRIES = 20
#: Installing a gateway service clones nothing and starts one process, but it writes a unit and asks systemd to take
#: it - slower than a setting, and bounded so a machine that hangs does not hang the answer.
GATEWAY_INSTALL_TIMEOUT_S = 180

#: The only keys a PAIRING entry may hold. A sibling section of the same file - the ports - must never end up
#: inside one: an entry carrying an unrelated key still looks like a pairing that exists, so clearing it would
#: leave the entry behind and the machine would never read as unpaired again.
PAIRING_KEYS = ("token", "refused")

# ``_token_hint``s this process has already offered and had refused, so a token is tried once per run rather than
# once per connection attempt. A refusal from a PREVIOUS run is deliberately absent from this: see ``_offer_token``.
_REFUSED_THIS_RUN: set = set()

#: The directory this plugin keeps that state in, under Hermes' home rather than inside the plugin. It coincides
#: with the install name and is a separate fact on purpose: one is where state lives, the other is what
#: `plugins install|enable|doctor` call the plugin.
STATE_DIR = "familiar-platform"

#: What Hermes knows this plugin as: its directory name, and what `plugins.enabled` lists.
PLUGIN_NAME = "familiar-platform"
#: What Hermes knows this platform AS. Not the plugin directory's name: the config is keyed by this, so reading a
#: sibling profile's settings means knowing the difference.
PLATFORM_NAME = "familiar"

#: A connection that drops is retried: the far end restarting is not a reason to give up. Backing off, because a
#: machine that reconnects in a tight loop is a machine nobody can use.
# How long to wait before asking again after ONE refusal. Long enough that a token being replaced has finished being
# replaced, short enough that a real revocation costs a breath rather than a pairing.
REFUSAL_RETRY_S = 3.0
RECONNECT_MIN_S = 1.0
RECONNECT_MAX_S = 30.0

#: The answer to a confirmation this adapter raised: ``cf:<confirm_id>:<once|cancel>``. The gateway's own text
#: fallback spells these as commands (``/approve``, ``/cancel``); this is the same answer without relying on a
#: message that is not a message becoming one.
CONFIRM_PREFIX = "cf:"
#: A tapped choice on a clarify question: ``cl:<clarify id>:<index>``.
CLARIFY_PREFIX = "cl:"
#: A decision on an approval: ``appr:<request id>:<choice>``.
APPROVAL_PREFIX = "appr:"

#: Loopback-only: what this machine answers when somebody asks it what code it is showing.
PAIR_PATH = "/familiar/pair"


def _state_home() -> Path:
	"""The MACHINE's Hermes root, which is where a plugin's state belongs.

	A pairing is with a machine, not with a profile: a profile is a dimension OF a paired machine. So this state sits
	above every profile - a named profile's home and the default home resolve to the same root - and every profile's
	gateway reads the one file. What a profile contributes is what the machine REPORTS (`FAMILIAR_INSTANCE`), never
	what it pairs as.

	Per-profile was the earlier reading, and it is the one that reads as obvious from inside a profile: a machine
	running two profiles kept two pairings. That made pairing a per-agent act and left a reader with one machine
	listed twice. One pairing, several profiles.

	Asked of Hermes rather than read off the environment. An older Hermes with no `get_default_hermes_root` falls
	back to the profile's own home, which is where it paired before: a machine must not lose its pairing to a
	version bump.
	"""
	try:
		from hermes_constants import get_default_hermes_root

		return Path(get_default_hermes_root()) / STATE_DIR
	except Exception:  # noqa: BLE001 - a pairing is worth a fallback, never a crash
		pass

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

	# A home that will not take a directory fails loudly on the write instead.
	with contextlib.suppress(Exception):
		path.parent.mkdir(parents=True, exist_ok=True)

	return path


def _profile_key() -> str:
	"""This profile's identity: its own directory name, or ``default`` for the machine's own home.

	Asked of the paths rather than of ``FAMILIAR_INSTANCE``: the label is a thing a reader RENAMES, and a renamed
	label must not move the port this profile keeps.
	"""
	try:
		from hermes_constants import get_hermes_home

		home = Path(get_hermes_home()).resolve()
	except Exception:  # noqa: BLE001 - an import that fails still has a machine to answer for
		return DEFAULT_TARGET

	try:
		root = _state_home().parent.resolve()
	except Exception:  # noqa: BLE001
		return DEFAULT_TARGET

	return DEFAULT_TARGET if home == root else home.name


def _backend_key(url: str) -> str:
	"""What one Familiar deployment is keyed by: where it answers.

	The URL is the identity because it is the one thing that tells two Familiars apart before either has issued a
	token - a machine pairs with an ADDRESS, and a token is what that address handed back. Prod and dev are two keys.
	The trailing slash goes, so the same Familiar written two ways stays one key.
	"""
	return str(url or "").strip().rstrip("/")


def _read_raw() -> dict[str, Any]:
	"""The state file exactly as written; missing or unreadable reads as empty."""
	try:
		raw = json.loads(_state_path().read_text(encoding="utf-8")) or {}
	except Exception:  # noqa: BLE001 - no state, unreadable state: both mean "nothing paired yet"
		return {}

	return raw if isinstance(raw, dict) else {}


def _profile_pairing() -> dict[str, Any]:
	"""A pairing kept under the PROFILE's own home, from when a machine paired per profile.

	That reading is gone - a profile is a dimension of a paired machine - so this file is folded into the machine's
	map once and retired, or a machine that already paired would pair again to no purpose.
	"""
	try:
		from hermes_constants import get_hermes_home

		legacy = Path(get_hermes_home()) / STATE_DIR / STATE_FILE
	except Exception:  # noqa: BLE001
		return {}

	if not legacy.exists():
		return {}

	try:
		if legacy.resolve() == _state_path().resolve():
			return {}  # the default home IS the machine root, so there is nothing to adopt
	except OSError:
		return {}

	try:
		raw = json.loads(legacy.read_text(encoding="utf-8")) or {}
	except Exception:  # noqa: BLE001
		return {}

	if not isinstance(raw, dict):
		return {}

	nested = raw.get("pairings")

	if isinstance(nested, dict) and len(nested) == 1:
		only = next(iter(nested.values()))

		return dict(only) if isinstance(only, dict) else {}

	if raw.get("token") or raw.get("refused"):
		return dict(raw)

	return {}


def _retire_profile_state() -> None:
	"""Drop a profile's own state file once its pairing has been folded into the machine's."""
	try:
		from hermes_constants import get_hermes_home

		(Path(get_hermes_home()) / STATE_DIR / STATE_FILE).unlink(missing_ok=True)
	except Exception as error:  # noqa: BLE001 - an old file that will not go is not worth a failed read
		logger.warning("[familiar] could not retire this profile's own pairing file: %s", error)


def _migrate(raw: dict[str, Any], url: str) -> dict[str, Any]:
	"""Every pairing the machine holds, keyed by backend, with an older file folded in under *url*.

	"Older" is either shape, and both say the same thing - this machine has paired, once, with the Familiar at *url*:
	a FLAT file (``{"token": ...}``), written when a machine could hold only one pairing; or one written under a
	PROFILE's own home, from when pairing was per-profile.
	"""
	key = _backend_key(url)
	pairings = raw.get("pairings")

	if isinstance(pairings, dict):
		out = {str(name): dict(entry) for name, entry in pairings.items() if isinstance(entry, dict)}
	elif raw.get("token") or raw.get("refused"):
		# The PAIRING keys only. Handing the whole document to the map would make every other section part of a
		# pairing, and the entry would then outlive its own token.
		out = {key: {k: v for k, v in raw.items() if k in PAIRING_KEYS}} if key else {}
	else:
		out = {}

	if key and key not in out:
		orphan = _profile_pairing()

		if orphan:
			out[key] = orphan
			_retire_profile_state()

	return out


def _load_pairings(url: str) -> dict[str, Any]:
	"""Every pairing this machine holds, keyed by the backend that issued it."""
	return _migrate(_read_raw(), url)


def _write_raw(state: dict[str, Any]) -> None:
	"""Write the machine's state file exactly as given, and never PART of it.

	Written beside itself and moved into place, because a reader must see one document or the other and nothing in
	between. Writing the file directly TRUNCATES it first, so there is a window - short, and not short enough - where
	the document on disk is empty: every profile of a machine shares this one file, and a reader in that window
	merges its own section into "nothing" and writes that back. One profile recording its port became the loss of the
	pairing every profile depends on, which is a machine that answers nothing and says nothing about why.
	"""
	try:
		path = _state_path()
		path.parent.mkdir(parents=True, exist_ok=True)
		text = json.dumps(state, indent="\t")
		# Named for the process that owns it, because two profiles write here and a shared scratch name is the same
		# collision one step earlier.
		scratch = path.with_name(f"{path.name}.{os.getpid()}.tmp")
		scratch.write_text(text, encoding="utf-8")

		# A reader with this file open is enough to refuse the move on Windows, and a reader is not a reason to lose
		# a write: the move is retried, and the scratch file goes either way.
		for attempt in range(MOVE_TRIES):
			try:
				os.replace(scratch, path)

				break
			except OSError:
				if attempt == MOVE_TRIES - 1:
					raise

				time.sleep(LOCK_POLL_S)
	except Exception as error:  # noqa: BLE001 - state that will not persist is worth a warning, not a crash
		logger.warning("[familiar] could not save this machine's state: %s", error)


def _state_has_content() -> bool:
	"""Whether there is a document on disk, readable or not."""
	try:
		return bool(_state_path().read_text(encoding="utf-8").strip())
	except Exception:  # noqa: BLE001 - a file that will not be read is not a file with nothing in it
		return False


@contextlib.contextmanager
def _state_lock(timeout: float = LOCK_WAIT_S):
	"""One writer at a time, across the PROFILES that share this file.

	They are separate processes - a gateway each - and they all read this file, change their own part of it and write
	it back. Two of those overlapping is not a theoretical race, it is a lost update: whichever read comes first is
	the document the second write is based on. An atomic write alone cannot fix that, because nothing was ever half
	written; the read and the write simply have to be one step, which is what a lock makes them.

	A lock file, and not a lock inside the file, because the thing being protected is the file itself. It carries no
	content: its EXISTENCE is the lock, so it is safe to leave behind and safe to break.
	"""
	path = Path(f"{_state_path()}.lock")
	deadline = time.monotonic() + timeout
	held = False

	while True:
		try:
			os.close(os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY))
			held = True

			break
		except FileExistsError:
			# A lock its owner died holding must not outlive it, and its age is the only thing that tells the two
			# apart: no write takes a fraction of this, so a lock this old is nobody's.
			try:
				if time.time() - path.stat().st_mtime > STALE_LOCK_S:
					path.unlink(missing_ok=True)

					continue
			except OSError:
				pass

			if time.monotonic() > deadline:
				# Said out loud and then done anyway: a machine that stops writing its own state because another
				# profile is wedged is a machine that has lost more than a port.
				logger.warning("[familiar] another profile has held this machine's state file for %ss: writing anyway", timeout)

				break

			time.sleep(LOCK_POLL_S)

	try:
		yield
	finally:
		if held:
			try:
				path.unlink(missing_ok=True)
			except OSError as error:  # noqa: BLE001 - a lock that will not go is broken by its own age
				logger.warning("[familiar] could not release this machine's state lock: %s", error)


def _write_state(**sections: Any) -> None:
	"""Write sections into the machine's one state file, leaving every OTHER section exactly where it is.

	One file holds more than one concern - the pairings, and the loopback port each profile took - so a writer that
	laid down only the half it was holding would drop the other half. The flat keys of a file written before a
	machine could hold two pairings are dropped here: the caller has already folded them into ``pairings``, and
	leaving them beside the map they became would resurrect them on the next read.

	An empty read over a file that is NOT empty means the read failed - the document is corrupt, or was being
	replaced while it was read. Merging this writer's section into "nothing" would persist that nothing and take the
	pairing with it, so a write that brings no pairing of its own is refused instead. That is the whole difference
	between a lost port, which heals itself, and a machine that has to pair again - and it is the case that leaves no
	trace at all unless it is said out loud here.
	"""
	if not _read_raw() and not sections.get("pairings") and _state_has_content():
		logger.warning(
			"[familiar] this machine's state file could not be read, and this write carries no pairing to replace it "
			"with: refusing to overwrite it (%s)",
			", ".join(sorted(sections)),
		)

		return

	# The read and the write are one step: the document the merge is based on must be the one the lock was taken for.
	with _state_lock():
		state = _read_raw()
		state.update(sections)

		# ONLY a writer that just folded the flat document into ``pairings`` may remove its keys. A writer that came
		# to record something else - this profile's port - has not folded anything, and a file still in the flat shape
		# is a machine that PAIRED that way: dropping ``token`` there would take the pairing away from every machine
		# that has one, on the first run after it updated.
		if "pairings" in sections:
			state.pop("token", None)
			state.pop("refused", None)

		_write_raw(state)


def _fold_pairings(state: dict[str, Any], pairings: dict[str, Any]) -> None:
	"""Put a pairings map into a state document, dropping the flat keys it replaced.

	Beside the map rather than in it, those keys are a SECOND copy of the pairing, and one that outlives the map's
	own entry: an unpaired machine would read as paired on the next start.
	"""
	state["pairings"] = pairings
	state.pop("token", None)
	state.pop("refused", None)


def _save_pairings(pairings: dict[str, Any]) -> None:
	"""Write the machine's pairings, each under the backend it belongs to."""
	_write_state(pairings=pairings)


def _familiar_in(home: Path) -> bool:
	"""Whether one profile's home can reach Familiar: the plugin is THERE, and that home's allow-list names it.

	Two separate facts, and both are needed. Present but not enabled is discovered and never loaded; enabled but not
	present has nothing to load. The allow-list rule is the plugin loader's own - a name in ``plugins.enabled``, and a
	missing or malformed key means NOTHING is enabled, which is what makes plugins opt-in in the first place.

	Read per home rather than through the loader: the loader answers for the home the process is RUNNING in and caches
	by file signature, so borrowing it for a sibling profile would answer for the wrong one and poison its cache.
	"""
	if not (home / "plugins" / PLUGIN_NAME).is_dir():
		return False

	try:
		from utils import fast_safe_load

		config = fast_safe_load((home / "config.yaml").read_text(encoding="utf-8")) or {}
	except Exception:  # noqa: BLE001 - a config that cannot be read enables nothing, which is the rule's own default
		return False

	plugins = config.get("plugins") if isinstance(config, dict) else None
	enabled = plugins.get("enabled") if isinstance(plugins, dict) else None

	return isinstance(enabled, list) and PLUGIN_NAME in enabled


def _env_value(home: Path, key: str) -> str:
	"""One key out of a profile's own `.env`, or "".

	Read as a file rather than through the plugin's own settings helper: that helper answers for the profile this
	process RUNS in, and the whole point here is a sibling.
	"""
	try:
		for line in (home / ".env").read_text(encoding="utf-8").splitlines():
			name, _, value = line.partition("=")

			if name.strip() == key:
				return value.strip().strip('"').strip("'")
	except Exception:  # noqa: BLE001 - a home with no .env is a profile pointed at nothing
		return ""

	return ""


def _url_in(home: Path) -> str:
	"""Which Familiar one profile's home is pointed at, or "" when it is pointed at none.

	The two places a profile's own settings can live, in the order the plugin reads them for itself: the config's
	`platforms.familiar.extra.url`, then `FAMILIAR_URL` in that home's `.env`. Read per home rather than through the
	loader, for the same reason `_familiar_in` is - the loader answers for the home this process runs in.

	A profile pointed at nothing is a profile no Familiar has claimed yet, and that is deliberately different from one
	pointed elsewhere: the first is anybody's to adopt, the second belongs to somebody.
	"""
	value = ""

	try:
		from utils import fast_safe_load

		config = fast_safe_load((home / "config.yaml").read_text(encoding="utf-8")) or {}
		platforms = config.get("platforms") if isinstance(config, dict) else None
		familiar = platforms.get(PLATFORM_NAME) if isinstance(platforms, dict) else None
		extra = familiar.get("extra") if isinstance(familiar, dict) else None
		value = str((extra or {}).get("url") or "").strip() if isinstance(extra, dict) else ""
	except Exception:  # noqa: BLE001 - a config that cannot be read points nowhere, which is the truth
		value = ""

	return _backend_key(value or _env_value(home, "FAMILIAR_URL"))

def _home_of(name: str) -> Path | None:
	"""One profile's home by name, or None when this machine has no such profile."""
	for info in _list_profiles():
		if str(getattr(info, "name", "") or "") == name:
			return Path(getattr(info, "path", "") or "")

	return None


def _profiles_report(infos: list[Any], backend: str = "") -> list[dict[str, Any]]:
	"""What Familiar is told about this machine's profiles, one entry each.

	A pure assembly of what Hermes already knows, so the only decision here is which facts travel: the name, whether
	it is the machine's own Hermes home, whether a gateway of its own is running, whether FAMILIAR is on it, and
	whether it is the profile this very connection belongs to.

	Scoped to *backend*, the Familiar asking, because a machine may be paired with two - prod and dev side by side -
	and each is answered for the profiles pointed at it. Without this the machine hands a dev Familiar the names of
	prod's profiles and invites it to command them.

	A profile pointed at NOTHING is kept, and deliberately: that is a profile no Familiar has claimed, which is what
	the picker offers to activate. Only one already pointed at a DIFFERENT Familiar is left out.
	"""
	current = _profile_key()
	report = []

	for info in infos:
		name = str(getattr(info, "name", "") or "")

		if not name:
			continue

		home = Path(getattr(info, "path", "") or "")

		if backend and name != current and _url_in(home) not in ("", backend):
			continue

		report.append(
			{
				"name": name,
				"default": bool(getattr(info, "is_default", False)),
				"gatewayRunning": bool(getattr(info, "gateway_running", False)),
				"familiar": _familiar_in(Path(getattr(info, "path", ""))),
				"current": name == current,
			}
		)

	return report


def _named_profile(raw: Any) -> tuple[str, str]:
	"""A profile name Hermes itself would accept, or a reason it would not.

	Normalised and then validated with Hermes' OWN rule - the function its dashboard and its CLI use - so a name
	Familiar accepts is a name the machine can actually make. Normalised first because a person typing into an app may
	use capitals while the on-disk directory is lowercase, and Hermes documents that split: `normalize_profile_name`
	for what a user types, `validate_profile_name` for what a directory may be called.

	The reason travels back as the app's answer rather than being raised: "that name is not usable" is a fact about the
	request, and the reader is owed it in words.
	"""
	typed = str(raw or "").strip()

	if not typed:
		return "", "no profile named"

	try:
		from hermes_cli.profiles import normalize_profile_name, validate_profile_name

		name = str(normalize_profile_name(typed) or "").strip()
		validate_profile_name(name)
	except ImportError:  # pragma: no cover - an older Hermes, whose rule is documented and stable
		name = typed.lower()

		if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", name):
			return "", f"{typed!r} is not a profile name: lowercase letters, digits, - and _ only"
	except Exception as error:  # noqa: BLE001 - the reason IS the answer
		return "", str(error)

	return name, ""


def _list_profiles() -> list[Any]:
	"""This machine's profiles, as Hermes itself enumerates them.

	``list_profiles`` rather than a glob of ``profiles/*``: it is what ``hermes profile list`` shows, it includes the
	default home, and it leaves out the tombstone a deleted profile can leave behind. A profile Familiar was never
	told about is worse than a profile it was told about and cannot reach.
	"""
	try:
		from hermes_cli.profiles import list_profiles

		return list(list_profiles())
	except Exception as error:  # noqa: BLE001 - an answer is owed, and this one is an empty list plus a log line
		logger.warning("[%s] could not read this machine's profiles: %s", PLUGIN_NAME, error)

		return []


def _install_provenance() -> tuple[str, str]:
	"""This copy's own source and revision, from the metadata Hermes writes when it installs a plugin.

	Read from THIS plugin directory's neighbour rather than from the machine root: the root's `plugins/` belongs to
	whichever profile is the default, and the build to put on another profile is the one running HERE. A machine with
	two builds of one channel would be worse than a profile with none.
	"""
	try:
		metadata = json.loads(
			(Path(__file__).resolve().parent.parent / ".install-metadata.json").read_text(encoding="utf-8"))
	except Exception:  # noqa: BLE001 - an install with no record simply has nothing to copy elsewhere
		return "", ""

	entry = metadata.get(PLUGIN_NAME) if isinstance(metadata, dict) else None
	entry = entry if isinstance(entry, dict) else {}

	return str(entry.get("source") or ""), str(entry.get("revision") or "")


def _run_hermes(argv: list[str], timeout: int = 300) -> tuple[int, str]:
	"""Run Hermes' own command line and read what it said, both streams together.

	`sys.executable -m hermes_cli.main` rather than a `hermes` on PATH: this call comes from inside a running gateway,
	so the interpreter is known and is the one whose Hermes this is - while PATH may hold a profile's own wrapper,
	which is the whole class of mistake this action exists to avoid.

	stdin is CLOSED, so a command that asks a question is refused instead of waiting for an answer that can never
	arrive and dying on the timeout - an installer with nobody at the terminal has nobody to answer it.
	"""
	try:
		done = subprocess.run(
			[sys.executable, "-m", "hermes_cli.main", *argv],
			capture_output=True,
			text=True,
			timeout=timeout,
			stdin=subprocess.DEVNULL,
		)
	except Exception as error:  # noqa: BLE001 - a command that could not run is an answer too
		return 1, str(error)

	return done.returncode, (done.stdout or "") + (done.stderr or "")


def _answer_line(output: str) -> str:
	"""The line of a command's output that explains what happened: what it labelled an error, else its last word.

	Not the first line and not the whole dump: an installer prints progress and warnings before anything matters, and
	whoever reads this answer wants the reason in one sentence.
	"""
	lines = [line.strip() for line in (output or "").splitlines() if line.strip()]

	for line in lines:
		if "error:" in line.lower():
			return line

	return lines[-1] if lines else ""


def _load_ingress_port(profile: str) -> int | None:
	"""The loopback port this profile took last time, or None if it never took one."""
	ports = _read_raw().get("ingress")

	if not isinstance(ports, dict):
		return None

	try:
		return int(ports[profile])
	except (KeyError, TypeError, ValueError):
		return None


@contextlib.contextmanager
def _state_change(writes_pairing: bool = False):
	"""Read this machine's state, change it, and write it back - as ONE step, under the lock.

	For any writer whose new value is built FROM the file. Reading the section here and writing it back there looks
	the same as reading it in the caller and passing it in, and is not: the read would happen outside the lock, on a
	document another profile may already have replaced. That is a lost update, and it is how a profile recording its
	own port dropped the port the other profile had recorded a moment before.

	The same refusal as ``_write_state``, for the same reason: an empty read over a file that is NOT empty is a read
	that failed, and writing this change over it would take the pairing with it. A caller that BRINGS a pairing is the
	recovery, and is allowed through.
	"""
	if not writes_pairing and not _read_raw() and _state_has_content():
		logger.warning(
			"[familiar] this machine's state file could not be read, and this change carries no pairing to replace it "
			"with: leaving it exactly as it is"
		)

		yield None

		return

	with _state_lock():
		state = _read_raw()
		yield state
		_write_raw(state)


def _save_ingress_port(profile: str, port: int) -> None:
	"""Remember which loopback port this profile took.

	Without it the port is decided by whichever gateway happens to start first, so two profiles can swap ports
	across a reboot - and the one place a person reads a pairing code would move out from under them. Recorded,
	it is stable: a profile keeps the port it has, and only takes another when that one is genuinely gone.
	"""
	with _state_change() as state:
		if state is None:
			return

		ports = state.get("ingress")
		ports = dict(ports) if isinstance(ports, dict) else {}
		ports[profile] = int(port)
		state["ingress"] = ports


def _forget_ingress(profile: str) -> None:
	"""Drop one profile's remembered ingress port, if it has one.

	Deleting a profile used to leave the port it took behind, and the map then named a profile the machine no longer
	has - read as fact by anyone opening the file, which is enough on a map whose whole job is to be the record of what
	took which port. The section goes when the last profile does, so an empty machine does not keep an empty shell.
	"""
	with _state_change() as state:
		if state is None:
			return

		ports = state.get("ingress")

		if not isinstance(ports, dict) or profile not in ports:
			return

		ports = dict(ports)
		ports.pop(profile, None)

		if ports:
			state["ingress"] = ports
		else:
			state.pop("ingress", None)


def _entry_for(url: str) -> dict[str, Any]:
	"""This backend's pairing, or an empty one."""
	return dict(_load_pairings(url).get(_backend_key(url)) or {})


def _load_token(url: str = "") -> str:
	"""The token this machine was given when it paired with the Familiar at *url*, or an empty string."""
	return str(_entry_for(url).get("token") or "")


def _config_path() -> Any:
	"""This profile's own Hermes config, which is where a restarted gateway reads its settings from."""
	try:
		from hermes_constants import get_hermes_home

		return Path(get_hermes_home()).resolve() / "config.yaml"
	except Exception:  # noqa: BLE001 - a config we cannot find is a config we do not touch
		return None


def _write_env_setting(key: str, value: str) -> bool:
	"""Write one FAMILIAR_* setting into this profile's own `.env`, where a gateway started from a shell reads it.

	The env-shaped name and not `platforms.familiar.extra.<key>`, and the difference is measured rather than stylistic:
	at gateway config load the plugin's `env_enablement_fn` seeds `extra` from these very env vars and the gateway
	commits that seed ON TOP of the configured block - so an `extra.url` written here is silently replaced by the
	default, and the machine dials its own loopback while the config appears to say otherwise.

	Only this profile's own `.env`, and only this key: whatever else is in the file is left exactly as it was.
	"""
	path = _config_path()

	if path is None:
		return False

	env = path.parent / ".env"

	try:
		lines = env.read_text(encoding="utf-8").splitlines() if env.exists() else []
		kept = [line for line in lines if not re.match(rf"\s*{re.escape(key)}\s*=", line)]
		kept.append(f"{key}={value}")

		written = env.with_name(".env.familiar-tmp")
		written.write_text("\n".join(kept) + "\n", encoding="utf-8")
		env.replace(written)
		logger.info("[familiar] this profile's .env now holds %s", key)

		return True
	except Exception as error:  # noqa: BLE001 - a setting this cannot write is not a reason to stop
		logger.warning("[familiar] could not write %s into this profile's .env: %s", key, error)

		return False


def _write_config_extra(updates: dict[str, str]) -> bool:
	"""Write settings into this profile's own `platforms.familiar.extra`, the way a restarted gateway reads them.

	Written as a file rather than through the CLI because the value must not travel through a command line, and
	replaced rather than edited so a half-written config can never be left behind for a gateway to read at startup.
	Read back before it becomes the config: a gateway that finds YAML it cannot parse does not start.

	Returns whether the write landed, so a caller that needs it (the pairing command, setting the address the machine
	dials) can say so rather than carrying on against a config that did not change.
	"""
	path = _config_path()

	if path is None or not path.exists():
		return False

	try:
		import yaml

		config = yaml.safe_load(path.read_text(encoding="utf-8")) or {}

		if not isinstance(config, dict):
			return False

		platforms = config.setdefault("platforms", {})
		familiar = platforms.setdefault("familiar", {}) if isinstance(platforms, dict) else {}

		if not isinstance(familiar, dict):
			return False

		extra = familiar.setdefault("extra", {})

		if not isinstance(extra, dict):
			return False

		if all(extra.get(key) == value for key, value in updates.items()):
			return True

		extra.update(updates)

		written = path.with_name(f".{path.name}.familiar-tmp")
		written.write_text(yaml.safe_dump(config, sort_keys=False, allow_unicode=True), encoding="utf-8")

		reloaded = yaml.safe_load(written.read_text(encoding="utf-8")) or {}
		kept = (((reloaded.get("platforms") or {}).get("familiar") or {}).get("extra") or {})

		if any(kept.get(key) != value for key, value in updates.items()):
			written.unlink(missing_ok=True)
			logger.warning("[familiar] the profile's config was left alone: the rewritten copy did not read back")

			return False

		os.replace(written, path)

		# The keys are named, never the values: one of these is a token, and a token in a log is a token leaked.
		logger.info("[familiar] this profile's config now holds %s", ", ".join(sorted(updates)))

		return True
	except Exception as error:  # noqa: BLE001 - a config this cannot write is not a reason to stop delivering
		logger.warning("[familiar] could not write settings into this profile's config: %s", error)

		return False


def _write_config_token(token: str) -> None:
	"""Keep a token this machine was GIVEN in this profile's own config, where a restarted gateway looks.

	Why here, and not only in the state: the state is what a live gateway presents, and the config is what a RESTARTED
	one falls back to. Only the state was ever written, so every restart went back to the token in the config - which,
	after a machine is deleted and added again, is the one Familiar already refused. Measured: a machine paired
	successfully, reported its port, and then revoked itself on the next restart, pairing again with a token it had
	been given and still held.
	"""
	_write_config_extra({"token": token})


def _save_token(url: str, token: str) -> None:
	"""Keep this backend's token across restarts, leaving every other backend's exactly where it is.

	Two things at once: a machine that has to pair again after every restart is not paired, and a machine talking to
	two Familiars holds two entries rather than one token overwriting the other.
	"""
	key = _backend_key(url)

	with _state_change(writes_pairing=True) as state:
		if state is None:
			return

		pairings = _migrate(state, url)
		entry = dict(pairings.get(key) or {})
		entry["token"] = token
		# A pairing that succeeds answers any earlier refusal: whatever Familiar turned down is not what this machine
		# presents now.
		entry.pop("refused", None)
		pairings[key] = entry
		_fold_pairings(state, pairings)

	# The state is what a live gateway presents; the config is what a restarted one falls back to. Both, or a restart
	# goes back to whatever the config was told last - which after a delete and re-add is a token already refused.
	_write_config_token(token)


def _clear_token(url: str = "") -> None:
	"""Forget THIS backend's pairing, and only this one. Every other Familiar the machine talks to stays paired."""
	key = _backend_key(url)

	with _state_change(writes_pairing=True) as state:
		if state is None:
			return

		pairings = _migrate(state, url)
		entry = {k: v for k, v in dict(pairings.get(key) or {}).items() if k in PAIRING_KEYS}
		entry.pop("token", None)

		if entry:
			pairings[key] = entry  # a remembered refusal outlives the token it was about
		else:
			pairings.pop(key, None)

	if pairings:
		_save_pairings(pairings)

		return

	# Nothing is paired through this backend any more. The file may still hold ANOTHER section - the port this
	# profile's ingress took - so it is the pairings that go, and the file only once nothing is left in it.
	state = _read_raw()
	state.pop("pairings", None)
	state.pop("token", None)
	state.pop("refused", None)

	if state:
		_write_raw(state)

		return

	try:
		_state_path().unlink(missing_ok=True)
	except Exception as error:  # noqa: BLE001
		logger.warning("[familiar] could not clear this machine's pairing: %s", error)


def _token_hint(token: str) -> str:
	"""A short, non-reversible name for a token, so a refusal can be remembered without keeping the token."""
	return hashlib.sha256(token.encode("utf-8")).hexdigest()[:8]


def _load_refused(url: str = "") -> str:
	"""The token the Familiar at *url* has already refused, as its hint, or an empty string.

	Only ever a token that came from OUTSIDE this install - the environment or the config. The one pairing gave
	us is deleted outright when it stops being accepted, so there is nothing to remember.
	"""
	return str(_entry_for(url).get("refused") or "")


def _forget_refusals() -> None:
    """Forget what this RUN has had refused, because a pairing answers all of it.

    A pairing hands the machine a token Familiar has just issued, so nothing refused before it applies any
    more - and the run's own memory is the part that outlives the stored one: a pairing pops the record kept
    on disk, while the token offered within the same run stayed blocked. Measured: delete a machine, add it
    again, and the machine pairs successfully and then will not offer the token it just collected until
    something restarts it.
    """
    _REFUSED_THIS_RUN.clear()


def _mark_refused(url: str, token: str) -> None:
	"""Remember that this machine must stop presenting a token it was configured with.

	The token itself is somebody's deliberate setting and is not this plugin's to erase: what is written down is
	that presenting it again would fail, scoped to THAT token's hint so a corrected value in the environment is
	honoured the moment it changes. Without it a machine whose instance was deleted elsewhere retries the same
	dead token forever - reporting ``paired: true``, showing no code, and with no way back from the app.
	"""
	try:
		key = _backend_key(url)
		pairings = _load_pairings(url)
		entry = dict(pairings.get(key) or {})
		entry["refused"] = _token_hint(token)
		entry.pop("token", None)
		pairings[key] = entry
		_save_pairings(pairings)
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
#: How far the search for a port this profile can actually have goes past the starting one. A machine runs one
#: ingress per PROFILE, so two profiles must not have to agree on a number - the second one simply moves past
#: the first. The span only has to outlast the number of profiles a machine really runs.
INGRESS_PORT_SPAN = 64

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



class _ChannelRefused(RuntimeError):
	"""Familiar would not take this machine's token.

	Raised where the refusal HAPPENS and acted on where the policy lives: one refusal is not yet news, because a
	connection made in the moment a machine's token is being replaced is refused once and accepted on the next ask.
	Measured: a gateway restarted right after its profile was wired logged exactly one 401, then connected. Treating
	that one as final is what parked it into pairing - and a parked gateway stays parked.
	"""


class _DeliveryError(Exception):
	"""A delivery that did not land. ``retryable`` separates "try again" from "this will never work"."""

	def __init__(self, message: str, *, retryable: bool = False) -> None:
		super().__init__(message)
		self.retryable = retryable


def _setting(extra: dict[str, Any], key: str, env: str, default: str = "") -> str:
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


def _instance_name(extra: dict[str, Any]) -> str:
	return _setting(extra, "instance", "FAMILIAR_INSTANCE") or _default_instance_name()



#: What a delivery IS, in the words the conversation's own copy records it in. ``message`` is said in the
#: conversation and is drawn as one; ``notice`` is something that happened TO it - a gateway lifecycle line, a
#: heartbeat, a job's output - which is kept so the copy stays complete and is never drawn as if a person or the
#: agent had said it. A command's reply is a lane of its own (`command`), decided on the far side, where the fact
#: that a command was ASKED lives.
MESSAGE_KIND = "message"
NOTICE_KIND = "notice"
REPLY_KIND = "reply"
# The turn's own final answer. Set by this plugin rather than by Hermes: see `_turn_final_metadata`.
TURN_FINAL_FLAG = "_turn_final"


def _turn_final_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
	"""Mark the turn's OWN final answer, so the far end does not keep a second copy of it.

	A session holds a turn's answer already. Familiar keeps what a session does NOT hold - the gateway's own notices, a
	command's answer, a job's output - and reads the rest of the conversation back from the instance, so the answer is
	the one delivery it is told not to store.

	Deliberately not inferred from the words at the far end, and not asked of Hermes either: the base adapter brackets
	every final text through ``send_final_ledgered`` (the normal lane and the queued lane both), and that is where the
	fact is attached. An older Hermes without that bracket never calls it, so the delivery arrives unmarked and the far
	end KEEPS it - a duplicate rather than a lost message, which is the only direction this may fail in.
	"""
	merged = dict(metadata or {})
	merged[TURN_FINAL_FLAG] = True
	return merged

#: Hermes' own status wording. This is a floor, not the whole rule: the markers above say it outright whenever the
#: gateway sets them, and these catch the lanes it does not. Discord keeps the same kind of list for exactly the
#: same reason (its bridge for gateways older than its own flag). Anchored at the start and matched on the notice's
#: own wording, never on a leading emoji: an agent's reply is full of emoji, and a rule that read one would swallow
#: the conversation.
_STATUS_NOTICE_PATTERNS = (
	re.compile(r"^\s*(?:\u267b\ufe0f|\u26a0\ufe0f)\s*Gateway\s+\S"),
	re.compile(
		r"^\s*(?:\u26a1\s*Interrupting|\u21aa\s*Redirected|\u23f3\s*(?:Queued|Subagent working|Compressing context)"
		r"|\u23e9\s*Steered)"
	),
	re.compile(r"^\s*\u23f3\s*Working\s+\u2014\s*\d+\s*min"),
	re.compile(r"^\s*\u26a0\ufe0f\s*No activity"),
	re.compile(r"^\s*(?:\U0001f5dc\ufe0f?|\u23f3)?\s*Compressed(?: with fallback)?:\s*\d+"),
	re.compile(r"^\s*Compression (?:refused|aborted|already in progress|skipped)"),
	re.compile(r"^\s*No changes from compression"),
	re.compile(r"^\s*Cronjob Response:"),
	re.compile(r"^\s*\U0001f7e1\s*/new cancelled"),
	re.compile(r"^\s*\U0001f4be\s*(?:Self-improvement review:|Skill\s)"),
	re.compile(r"^\s*\[Background process\s+\S+\s+(?:finished with exit code|is still running~)"),
	re.compile(r"^\s*[\u2705\u274c]\s*Hermes update\s+(?:finished|failed|timed out)"),
	# The compaction's own two handoffs: the note that opens a compacted transcript, and the elision it leaves where a
	# long tool output was. Both say outright that they are not the conversation, and both were read as messages.
	re.compile(r"^\s*\[PRIOR CONTEXT\b"),
	re.compile(r"^\s*\[[^\]]*\boutput demoted at compaction\b"),
	# A session reset: the reader's own /new, reported back into the conversation as the machine's word for it.
	re.compile(r"^\s*\u2728\s*Session reset!"),
)


def _is_status_notice(content: str) -> bool:
	"""Whether these words are the gateway reporting on itself rather than speaking in the conversation."""
	return any(pattern.match(content or "") for pattern in _STATUS_NOTICE_PATTERNS)


def _delivery_kind(content: str, metadata: dict[str, Any] | None) -> str:
	"""Whether this delivery is said in the conversation, or only happened to it.

	The fact travels WITH the delivery so the far end never has to read the words to tell the two apart: a copy that
	mixes them cannot be un-mixed downstream, and a reader who asked for the conversation would be shown machine
	events where its own words should be. A thread draws the two differently, and the count a reader is shown is about
	the conversation alone.

	Four markers say it outright:

	- ``_turn_final`` is the turn's own final answer, which the SESSION already holds. It is the one delivery the far
	  end is told not to keep, because it reads a conversation back from the instance and keeps only what a session
	  does not hold. Set by this plugin at the bracket every final text goes through (see ``_turn_final_metadata``).
	- ``job_id`` is a cron delivery, so a job's output and never a turn.
	- ``_interim_send`` is a mid-turn status send, which by definition is not the turn's final answer. A
	  ``send_message`` call carries neither, so a reply the agent PUSHED into a conversation - the case this lane
	  exists for - stays a message.
	- ``non_conversational`` is the gateway's own flag for a lifecycle send, set for Discord alone today; it is read
	  when it is there, and the wording list is the floor until it is not.
	"""
	markers = metadata or {}

	if markers.get(TURN_FINAL_FLAG):
		return REPLY_KIND

	if markers.get("job_id") or markers.get("_interim_send") or markers.get("non_conversational"):
		return NOTICE_KIND

	return NOTICE_KIND if _is_status_notice(content) else MESSAGE_KIND


def _payload(instance: str, target: str, content: str, metadata: dict[str, Any] | None) -> dict[str, Any]:
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
		"kind": _delivery_kind(content, metadata),
		"content": content[:MAX_MESSAGE_LENGTH],
		"truncated": truncated,
		"jobId": str(job_id) if job_id else None,
		"jobName": _job_name(str(job_id)) if job_id else None,
		"sentAt": datetime.now(UTC).isoformat(timespec="seconds"),
	}


def _user_agent(payload: dict[str, Any]) -> str:
	"""Who is calling, for the far end's logs.

	A delivery names its instance; a channel reply has no instance in it, because the request id it is
	answered under is the whole address. So the name is left out rather than invented - which is also what a
	missing key used to do here, by raising into the one path that owes an answer.
	"""
	instance = str(payload.get("instance") or "").strip()

	return f"hermes-familiar (instance:{instance})" if instance else "hermes-familiar"


def _post(url: str, token: str, payload: dict[str, Any], path: str = NOTIFY_PATH) -> dict[str, Any]:
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
			# WHICH profile is speaking, on EVERY call and not only on the channel.
			#
			# One machine pairs once and then runs a gateway per activated profile, so one token arrives on several
			# connections and Familiar has to work out WHICH agent spoke. With a single profile it does not need to
			# be told; with two it has nothing else to go on, and a call that does not name one is refused outright -
			# deliberately, since filing it under either agent would be a wrong answer nothing on screen could show
			# to be wrong.
			#
			# The refusal it gets back is the same generic one a bad token gets, which is how a machine that simply
			# never named itself read as a machine whose token was refused - for hours, while the token was fine.
			"X-Familiar-Profile": _profile_key(),
		},
	)
	try:
		with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
			raw = response.read().decode("utf-8", errors="replace")
	except urllib.error.HTTPError as error:
		detail = ""
		# A body that will not read is not worth failing the check over: the status is the fact that matters.
		with contextlib.suppress(Exception):
			detail = error.read().decode("utf-8", errors="replace")[:200]
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
	choices: list[str] | None,
	request_id: str,
	session_key: str,
	callback_prefix: str,
	command: str | None = None,
	description: str | None = None,
	expires_in: int | None = None,
) -> dict[str, Any]:
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


try:  # the plugin loader imports this file as a package, and check.py imports it as a plain module
	from .answers import FamiliarAnswerRoutes, _job_name
except ImportError:  # pragma: no cover - the flat layout
	from answers import FamiliarAnswerRoutes, _job_name


class FamiliarAdapter(FamiliarAnswerRoutes, BasePlatformAdapter):
	"""Familiar as a channel: what a person says here reaches the agent, and its asks reach them.

	The reads and actions a client asks this machine for are inherited from ``answers.py``.
	"""

	MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH

	# The three things this half and the answer half both use, reachable from either as ``self.<name>``: the delivery
	# POST, what a token is forgotten BY, and the two paths the channel speaks. Held here as class attributes so
	# neither module has to import the other at module level, which only works in one load order - and this file is
	# imported both ways, as a package and as a plain module.
	#
	# ``_clear_token`` is a METHOD rather than that module function, because forgetting is per BACKEND now: the one
	# this adapter speaks to, and not every Familiar the machine is paired with.
	_post = staticmethod(_post)
	CHANNEL_REPLY_PATH = CHANNEL_REPLY_PATH
	INGRESS_PATH = INGRESS_PATH

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
		# An operator's own setting means exactly that port, and a taken one is theirs to see and fix. The DEFAULT
		# is only a starting point instead: a machine runs one ingress PER PROFILE, so the second profile's gateway
		# must not lose a race for a number it can simply move past. What this profile took last time wins over the
		# default, so the one place a person reads a pairing code does not move between restarts.
		configured_port = str(_setting(extra, "ingress_port", "FAMILIAR_INGRESS_PORT", "") or "").strip()
		self._ingress_port_set = bool(configured_port)

		if self._ingress_port_set:
			try:
				self._ingress_port = int(configured_port)
			except ValueError:
				logger.warning(
					"[familiar] FAMILIAR_INGRESS_PORT is not a port number (%r); starting from %s instead",
					configured_port, DEFAULT_INGRESS_PORT,
				)
				self._ingress_port_set = False

		if not self._ingress_port_set:
			self._ingress_port = _load_ingress_port(_profile_key()) or DEFAULT_INGRESS_PORT

		self._ingress_runner: Any | None = None
		# chat id -> the session key that conversation is routed by. The routing entry is the only thing that
		# knows which SESSION an address is on, and it is the gateway's, so it has to be asked per reply rather
		# than remembered: /new, /reset and a compression rotation all move the key to another session.
		self._session_keys: dict[str, str] = {}
		# confirm_id -> (session_key, chat_id): what answering a confirmation this adapter raised resolves, and
		# where to say so. The pending hold itself lives in the gateway (``tools.slash_confirm``), so this is
		# only the address of the answer.
		self._confirms: dict[str, Any] = {}
		#: What a clarify asked, by its id, so a tap can be read back as the choice it picked.
		self._clarifies: dict[str, Any] = {}
		#: Which conversation an approval was asked in, by its id: resolving one needs the session it belongs to.
		self._approvals: dict[str, str] = {}
		# chat key -> the directory this conversation works in. Kept in the process as well as applied, so every
		# later turn of the conversation gets it even though only the ACT of choosing comes through the ingress.
		self._directories: dict[str, str] = {}
		# The connection this machine holds to Familiar, and the code it is showing while it waits to be paired.
		self._channel_task: Any | None = None
		self._pair_code: str = ""
		# The pairing attempt in flight, if any. One, not one per caller: every attempt mints its own code.
		self._pairing_task: asyncio.Task[None] | None = None
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
		stored = _load_token(self._url)

		if stored:
			return stored

		if not self._token:
			return ""

		# A refusal recorded by an EARLIER run is not a verdict on this one. Measured on a real machine: Familiar
		# refused its own token once - a delivery that raced the issuing of the token it carried - and the machine
		# then held a perfectly good token and never offered it again, pairing forever while every read aimed at it
		# timed out. One refusal is what a race looks like, and a connection attempt is new information.
		#
		# Bounded, so this is not a retry loop: one offering per run per token, and a token refused HERE is added and
		# left alone until the process restarts. Without that it would sit in a 401 loop, which is the thing the
		# record exists to prevent.
		hint = _token_hint(self._token)

		if hint == _load_refused(self._url) and hint in _REFUSED_THIS_RUN:
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

	def _clear_token(self) -> None:
		"""Forget this backend's pairing - the one this adapter speaks to, and no other.

		Per BACKEND rather than per machine: a machine paired with two Familiars holds two tokens, and forgetting one
		of them (its instance was deleted there, or the reader unpaired it) must leave the other exactly where it is.
		"""
		_clear_token(self._url)

	async def connect(self, *, is_reconnect: bool = False) -> bool:
		"""Open the ingress: the one route Familiar posts a person's message to.

		This is the direction that makes Familiar a channel rather than a notifier. The other direction - the
		notifications and the asks - is outbound HTTP, and needs no listener at all.
		"""
		from aiohttp import web

		# The ingress BEFORE the channel, so the port this profile took is settled before anything announces it.
		# The connection is what carries that port to Familiar, and a connection opened first would carry the
		# number this profile MEANT to take - which is exactly the number another profile may have got to first.
		# So the one carrier that always reaches Familiar would name a port nothing here is listening on.
		ingress_up = await self._open_ingress(web)

		# The CHANNEL is the direction that needs nothing on this machine: it dials out, so a machine that has never
		# been paired still connects - and connecting is how it pairs. The INGRESS needs a token to authenticate with,
		# so a machine without one skips it and says why rather than refusing to load at all.
		#
		# Started even when the ingress did NOT come up: the channel is what pairs a machine, and a bind that failed
		# must not also take away the only way to get it fixed.
		if self._url and self._channel_task is None:
			self._channel_task = asyncio.create_task(self._channel_loop())
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

		if not ingress_up:
			return False

		self._mark_connected()

		return True

	def _ingress_candidates(self) -> list[int]:
		"""Every loopback port this adapter will try, in order.

		ONE entry when an operator set the port: their number means their number, and a taken one is a fact they
		asked to be told about rather than something to route around behind their back. Otherwise the starting port
		and the span above it - which is what lets a second profile exist at all.
		"""
		if self._ingress_port_set:
			return [self._ingress_port]

		return [self._ingress_port + step for step in range(INGRESS_PORT_SPAN)]

	async def _open_ingress(self, web: Any) -> bool:
		"""Bind the ingress on a port THIS profile can have, and remember which one that was.

		A machine runs several profiles, each with its own gateway, so the familiar ingress is per profile rather
		than per machine. Losing the bind is therefore not a failure to report but a number to move past - the one
		thing that must not happen is two profiles silently fighting over 8644, which is what `address already in
		use` looks like from outside while the gateway reports no connected platforms and retries forever.
		"""
		app = web.Application()
		app.router.add_post(INGRESS_PATH, self._handle_ingress)
		app.router.add_get(INGRESS_PATH, self._handle_ingress_probe)
		# What a person standing at this machine can ask it: the pairing code, or that it is already paired.
		app.router.add_get(PAIR_PATH, self._handle_pair_probe)

		runner = web.AppRunner(app, access_log=None)
		await runner.setup()

		refused: Exception | None = None

		for candidate in self._ingress_candidates():
			site = web.TCPSite(runner, self._ingress_host, candidate)

			try:
				await site.start()
			except OSError as error:
				refused = error
				continue

			if candidate != self._ingress_port:
				logger.info(
					"[familiar] %s was taken on %s; this profile's ingress takes %s",
					self._ingress_port, self._ingress_host, candidate,
				)

			self._ingress_port = candidate
			self._ingress_runner = runner
			_save_ingress_port(_profile_key(), candidate)

			logger.info("[familiar] ingress listening on http://%s:%s%s", self._ingress_host, candidate, INGRESS_PATH)

			return True

		logger.error("[familiar] ingress could not bind %s:%s - %s", self._ingress_host, self._ingress_port, refused)
		await runner.cleanup()

		return False

	async def _machine_name(self) -> str:
		"""What this machine calls itself when it pairs. The instance is named this until somebody renames it."""
		try:
			return socket.gethostname() or "Hermes"

		except Exception:  # noqa: BLE001 - a machine with no name is still a machine
			return "Hermes"

	def _refuse_token(self) -> None:
		"""Take a refused token as final: remember it, and pair again.

		Reached only after the refusal has been repeated, so a single 401 during a token change is a delay rather than
		a machine that stops offering the credential it holds. Pairing is reachable from BOTH places a token comes from,
		which is what a reader gets who deletes an instance and reconnects to a machine still holding its token.
		"""
		stored = _load_token(self._url)

		if stored:
			logger.warning("[familiar] this Familiar no longer accepts this machine's token; pairing again")
			_clear_token(self._url)
		elif self._token and _token_hint(self._token) != _load_refused(self._url):
			logger.warning("[familiar] Familiar does not accept the token this machine was configured with; pairing again")
			_mark_refused(self._url, self._token)
			_REFUSED_THIS_RUN.add(_token_hint(self._token))

	async def _channel_loop(self) -> None:
		"""Keep a connection to Familiar open, for as long as this machine is running.

		A machine behind NAT cannot be dialled; it can dial. So this holds one outbound connection and everything
		Familiar wants travels down it - no address on this machine, no key, nothing listening.

		A machine that was never paired PAIRS first: it asks for a code and shows it, and the person who can see
		that code is the person sitting here, which is who it is for.
		"""
		delay = RECONNECT_MIN_S
		# Whether a refusal has already been given a second chance since the last good connection. Reset whenever a
		# connection is established, so a machine that works keeps getting one retry per real break.
		retried = False

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
				retried = False
			except _ChannelRefused as error:
				# ONE refusal is not yet news. A machine's token is replaced in the moment a pairing lands, and a
				# connection made a heartbeat earlier is refused and then accepted - measured, not supposed. Ask
				# again before believing it, because believing it parks this gateway in pairing, and a gateway
				# parked in pairing never looks at the token the machine has.
				if not retried:
					retried = True
					logger.warning("[%s] channel refused: %s - asking again once", self.name, error)

					await asyncio.sleep(REFUSAL_RETRY_S)

					continue

				self._refuse_token()
			except asyncio.CancelledError:
				raise
			except Exception as error:  # noqa: BLE001 - a connection that drops is retried, never fatal
				logger.warning("[%s] channel dropped: %s", self.name, error)

			await asyncio.sleep(delay)
			delay = min(delay * 2, RECONNECT_MAX_S)

	def _channel_headers(self, token: str) -> dict[str, str]:
		"""What this connection says about itself: the build it runs, the profile it is, and the port its ingress took.

		Both travel on EVERY connection rather than only when a machine pairs. The version answers "did the update
		land?" for a machine set up over its shell, which never pairs at all; the port is what Familiar aims a
		reader's pairing-code command and its own reachability probe at, and since a machine runs one ingress PER
		PROFILE it cannot be assumed. The connection is the only carrier that is always there.

		The port is named ONLY while something is listening on it, and the listener IS the state: a bind that failed
		leaves the number it tried in `_ingress_port`, and announcing that would hand Familiar a port nothing answers
		on - worse than saying nothing, because the stored port is exactly what the reader's command and the machine's
		probe aim at, and the machine would look healthy while being unable to take a message at all. Saying nothing
		leaves the last known answer standing, which the probe then checks against the machine itself.
		"""
		headers = {
			"Authorization": f"Bearer {token}",
			"Accept": "text/event-stream",
			"X-Familiar-Plugin": PLUGIN_VERSION,
		}

		if self._ingress_runner is not None:
			headers["X-Familiar-Ingress"] = str(self._ingress_port)

		# WHICH profile is holding this connection, and it is the machine's own name for it rather than a label
		# anybody typed. One machine pairs once and then runs a gateway per activated profile, so N connections carry
		# the SAME token and the only thing that tells them apart is this - without it a machine with two agents is
		# indistinguishable from two machines, which is exactly the distinction Familiar has to make.
		headers["X-Familiar-Profile"] = _profile_key()

		return headers

	async def _channel_create(self, frame: dict[str, Any]) -> dict[str, Any]:
		"""Make a profile on this machine, asked from Familiar.

		A profile is a home Hermes runs a gateway in, and the only way to make one used to be Hermes' own command line
		on the machine - which is exactly the friction that keeps profiles unused. This runs that same command here:
		`hermes profile create`, with nothing cloned, because a new agent should start as itself rather than as a copy
		of whichever profile happened to be first. The app offers the one step that puts this channel on it afterwards.

		The name rule is Hermes' own (`_named_profile`), and the answer carries the name actually used, because a
		person typing `Coder` gets `coder` and should be told that rather than discovering it later.
		"""
		name, reason = _named_profile(frame.get("profile"))

		if reason:
			return {"error": reason}

		description = str(frame.get("description") or "").strip()

		argv = ["profile", "create", name]

		if description:
			argv += ["--description", description]

		code, output = await asyncio.to_thread(_run_hermes, argv, 180)

		if code != 0:
			return {"error": _answer_line(output) or f"hermes profile create exited {code}"}

		return {"profile": name, "created": True}

	async def _channel_rename(self, frame: dict[str, Any]) -> dict[str, Any]:
		"""Rename a profile on this machine.

		Refused for the two profiles that are not merely profiles: the machine's own, whose name is the machine's (a
		reader renames THAT in Familiar), and the one this very connection runs in - renaming moves the home a running
		gateway is inside, which is a machine that cannot reach Familiar afterwards and nobody able to say why.
		"""
		current, reason = _named_profile(frame.get("profile"))

		if reason:
			return {"error": reason}

		wanted, reason = _named_profile(frame.get("to"))

		if reason:
			return {"error": reason}

		if current == _profile_key():
			return {"error": "that is this profile: renaming it would move the home this connection runs in"}

		if current == DEFAULT_TARGET:
			return {"error": "that is the machine's own profile: rename the machine in Familiar instead"}

		if wanted == current:
			return {"profile": current, "to": wanted, "renamed": True}

		code, output = await asyncio.to_thread(_run_hermes, ["profile", "rename", current, wanted], 180)

		if code != 0:
			return {"error": _answer_line(output) or f"hermes profile rename exited {code}"}

		return {"profile": current, "to": wanted, "renamed": True}

	async def _channel_delete(self, frame: dict[str, Any]) -> dict[str, Any]:
		"""Delete a profile on this machine, behind its name typed out.

		The confirmation is checked HERE as well as in the app, and the two are not redundant: the app's dialog is what
		a person sees, and this is the half no caller can skip. Hermes' own delete is one command and irreversible.

		The machine's own profile is refused - it is the root home, and it is the thing Familiar is paired with - and
		so is the profile this connection runs in, whose gateway is this conversation.
		"""
		name, reason = _named_profile(frame.get("profile"))

		if reason:
			return {"error": reason}

		if str(frame.get("confirm") or "").strip() != name:
			return {"error": f"deleting a profile needs its name typed to confirm: {name}"}

		if name == _profile_key():
			return {"error": "that is this profile: its gateway is the connection this arrives on"}

		if name == DEFAULT_TARGET:
			return {"error": "that is the machine's own profile: Familiar is paired with it"}

		code, output = await asyncio.to_thread(_run_hermes, ["profile", "delete", name, "-y"], 300)

		if code != 0:
			return {"error": _answer_line(output) or f"hermes profile delete exited {code}"}

		# Its ingress port goes with it. Only on the way out: a delete that failed leaves a profile that still exists.
		_forget_ingress(name)

		return {"profile": name, "deleted": True}

	async def _channel_profiles(self, frame: dict[str, Any]) -> dict[str, Any]:
		"""Answer what profiles this machine has, and which of them Familiar is on.

		A READ, answered from Hermes' own state, like the `api` action. Asked rather than pushed: the app wants this
		while it is showing a machine, and a copy kept here would be a second answer that goes stale the moment somebody
		adds or deletes a profile - while the machine is the one thing that always knows.
		"""
		infos = await asyncio.to_thread(_list_profiles)

		return {"profiles": await asyncio.to_thread(_profiles_report, infos, self._url)}

	async def _channel_activate(self, frame: dict[str, Any]) -> dict[str, Any]:
		"""Put this channel on ANOTHER profile of this machine, in one step.

		A machine runs one gateway per profile and the plugin is found in the home it runs in, so a profile without one
		has no channel at all - and until now that meant installing the plugin once per profile, by hand, on every
		machine. This is that step, asked down the connection the machine already holds.

		Hermes' own installer runs rather than a copy of these files. A copy would have none of what an installed
		plugin has - the entry `plugins list` reads, the metadata an update needs, a doctor that answers - and
		removing one RAISES instead of removing it (measured: `shutil.rmtree` refuses a linked directory, so the
		plugin would be stuck there for good). The installer costs a copy of under a megabyte, and it keeps every
		profile's install a first-class one.
		"""
		profile = str(frame.get("profile") or "").strip()

		if not profile:
			return {"error": "no profile named"}

		if profile == _profile_key():
			return {"error": "that is this profile: the channel is already here"}

		# A profile already pointed at ANOTHER Familiar is not ours to take. `default` is the machine's own profile
		# and belongs to whoever set the machine up; a named one is where a second Familiar lives. Writing our URL
		# over it would not move the profile so much as strand it: that other Familiar keeps a token nothing dials,
		# and its instance goes quiet with no error anywhere - the profile looks activated from here and unreachable
		# from there. Refused rather than warned, because a warning here is read after the machine has gone dark.
		target_home = _home_of(profile)
		target_url = _url_in(target_home) if target_home else ""

		if target_url and target_url != _backend_key(self._url):
			return {
				"error": (
					f"`{profile}` is pointed at {target_url}, which is another Familiar: activating it here would "
					"take it from that one. Use a profile that is free, or move it deliberately with "
					f"`hermes -p {profile} config set FAMILIAR_URL {self._url}`"
				)
			}

		source, revision = _install_provenance()

		if not source or not revision:
			return {
				"error": (
					"this install has no recorded source and revision, so there is no build to put elsewhere - "
					"install it from its repository first"
				)
			}

		code, output = await asyncio.to_thread(
			_run_hermes,
			["-p", profile, "plugins", "install", source, "--ref", revision, "--enable", "--force"],
		)

		if code != 0:
			return {"error": _answer_line(output) or f"the installer exited {code}"}

		doctor_code, doctor_output = await asyncio.to_thread(
			_run_hermes, ["-p", profile, "plugins", "doctor", PLUGIN_NAME], 120)

		# ON THE MACHINE'S OWN CHANNEL, which is the half that install alone does not do. A plugin present and enabled
		# is not a machine that can deliver: the profile's gateway needs the same three settings this one runs with,
		# and it has to be a SERVICE - a gateway started from a shell is a foreground process holding the lock, which
		# took a machine's whole channel down the one time it was tried.
		#
		# The values are this install's own, because it is the same machine talking to the same Familiar: only the
		# label differs, and that is what the profile is. Written through the CLI rather than into a file, for the
		# reason the app writes them that way too - the gateway seeds the plugin's `extra` from these very settings at
		# load, so a hand-edited config is replaced by the default and a machine delivers to a port nothing listens on.
		for argv in (
			["-p", profile, "config", "set", "FAMILIAR_URL", self._url],
			["-p", profile, "config", "set", "FAMILIAR_TOKEN", self._token],
			# And the setting the plugin actually READS. The env-shaped names above are read from the machine's
			# `.env` by a gateway started from a shell, and never by one running as a service: that is every machine
			# where this worked and none where it did not. So the token goes where the plugin falls back to as well, or
			# a profile is installed, enabled, wired - and answers nothing.
			["-p", profile, "config", "set", "platforms.familiar.extra.token", self._token],
			["-p", profile, "config", "set", "FAMILIAR_INSTANCE", _default_instance_name()],
		):
			code, output = await asyncio.to_thread(_run_hermes, argv, 120)

			if code != 0:
				return {"error": _answer_line(output) or f"the machine would not store a setting ({code})"}

		# And a gateway of its own, as a service. `--start-now` because a profile nobody is running answers nothing,
		# which is exactly the state the reader just asked to leave.
		service_code, service_output = await asyncio.to_thread(
			_run_hermes, ["-p", profile, "gateway", "install", "--start-now"], GATEWAY_INSTALL_TIMEOUT_S)

		# RESTARTED, and not merely started. The settings above are read at STARTUP, so a gateway that is already
		# running - which is exactly what a profile activated a second time has - keeps whatever it read then, and
		# goes on presenting a token that may have been rotated since. Measured on a real machine: without this, the
		# profile sat there reporting 401 and asking to be paired, with a perfectly good token in its own settings,
		# and the difference was one restart.
		restart_code, restart_output = await asyncio.to_thread(
			_run_hermes, ["-p", profile, "gateway", "restart"], GATEWAY_INSTALL_TIMEOUT_S)

		gateway = service_code == 0 and restart_code == 0
		gateway_detail = None

		if service_code != 0:
			gateway_detail = _answer_line(service_output) or "the gateway service was not installed"
		elif restart_code != 0:
			gateway_detail = _answer_line(restart_output) or "the gateway service did not restart"

		return {
			"profile": profile,
			"installed": revision,
			# A registration that did not happen is what a job pointed at that profile discovers LATER, as a
			# delivery that never arrives - so the machine's own verdict on it travels with the answer.
			"registered": doctor_code == 0 and "ERROR" not in doctor_output.upper(),
			# The gateway may still be coming up, and that is not a failure of the install: the app asks the machine
			# which profiles are running rather than believing this.
			"gateway": gateway,
			"gatewayDetail": gateway_detail,
		}

	async def _hold_channel(self, token: str) -> None:
		"""Hold the connection open, and answer what comes down it."""
		import aiohttp

		# No total timeout: this connection is meant to stay open for days, and a deadline on it would be a
		# reconnect every time the deadline passed.
		timeout = aiohttp.ClientTimeout(total=None, sock_read=None)

		async with aiohttp.ClientSession(timeout=timeout) as session, session.get(
			f"{self._url}{CHANNEL_STREAM_PATH}",
			headers=self._channel_headers(token),
		) as response:
			if response.status == 401:
				# Not a token this Familiar knows: its instance was deleted, or the token was revoked. What to do
				# about it is the channel loop's decision, not this function's: one refusal is often the moment a
				# machine's token is being replaced, and the very next try is accepted.
				raise _ChannelRefused(f"the channel refused this machine's token (HTTP {response.status})")

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

	async def _pair_body(self) -> dict[str, Any]:
		"""What this machine says when it asks to be paired.

		Four things, and the last two exist because this request is the ONLY chance to say them: until it succeeds
		Familiar has no channel, so it cannot ask this machine anything - and by the time a code is claimed the app has
		already decided what it is creating.

		- ``name`` is Hermes' own name for the machine, and ``version`` is this plugin build, so the app can show both
		  before anything is claimed.
		- ``profile`` is WHICH profile is asking. Every profile of a machine runs its own gateway and any one of them
		  may be the one showing the code, so without it an app is told a machine is asking without being told which
		  of its agents that is.
		- ``paired`` is whether this machine already holds a credential for this Familiar. It is the difference between
		  a new machine and one that is paired again, and the app cannot tell those apart by name: a machine reports
		  ``hachirokuserver`` where the app named it "Hachi Server", so the names never collide and the app's own
		  duplicate guard never fires. Measured: re-adding a paired machine made a second row and replaced the token
		  the first row still held, which is a machine that goes dead with nothing on screen to say why.
		"""
		return {
			"name": await self._machine_name(),
			"version": PLUGIN_VERSION,
			"profile": _profile_key(),
			"paired": bool(_load_token(self._url) or self._token),
		}

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
				json=await self._pair_body(),
			)
			body = await started.json() if started.status == 200 else {}
			code = str(body.get("code") or "")
			secret = str(body.get("secret") or "")

			if not code or not secret:
				raise RuntimeError(f"pairing was refused (HTTP {started.status})")

			self._pair_code = code
			logger.warning("[familiar] PAIR: enter the code %s in Familiar, and this machine connects", code)

			# While this code waits to be claimed, ANOTHER profile of this machine may have its own code claimed -
			# the token is the machine's, and the file it lands in is shared. Waiting only on this stream is what
			# parked a gateway forever: a machine with two profiles showed two codes, one was claimed, the other
			# gateway sat here while the credential it needed was already in its own state directory.
			waiting = asyncio.create_task(self._wait_for_pairing(session, secret))
			arrived = asyncio.create_task(self._wait_for_token())

			done, pending = await asyncio.wait({waiting, arrived}, return_when=asyncio.FIRST_COMPLETED)

			for task in pending:
				task.cancel()

			for task in pending:
				with contextlib.suppress(asyncio.CancelledError, Exception):
					await task

			if arrived in done:
				self._pair_code = ""
				self._take_token(_load_token(self._url))
				logger.info("[familiar] paired by another profile of this machine: using the token it collected")

				return

			# The pairing wait finished first, so read HOW it finished. Leaving it unread is how a stream that failed
			# looked exactly like a stream that was still waiting: the token never arrives, nothing is logged, and the
			# machine pairs again - which is what a machine that was told to pair again does, so nobody looks closer.
			# A task whose exception nobody reads is also reported as an unretrieved exception, naming neither the
			# reason nor the machine.
			with contextlib.suppress(asyncio.CancelledError):
				error = waiting.exception()

				if error is not None:
					logger.warning("[familiar] the pairing wait ended without a token: %s", error)

			return

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
							_save_token(self._url, token)
							_forget_refusals()
							self._take_token(token)
							self._pair_code = ""
							logger.info("[familiar] paired: this machine is reachable through the channel now")

						return

	async def _wait_for_pairing(self, session: Any, secret: str) -> None:
		"""Wait on this profile's own pairing stream, and take the token when it is claimed."""
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
						_save_token(self._url, token)
						_forget_refusals()
						self._take_token(token)
						self._pair_code = ""
						logger.info("[familiar] paired: this machine is reachable through the channel now")

					return

	async def _wait_for_token(self) -> None:
		"""Return while this backend has a token, whoever collected it.

		The state file is shared by every profile of a machine, so a code claimed for ONE profile leaves a token every
		profile can use. A gateway that waits only on its own stream never sees it.

		A token PRESENT is the whole test, not a token that changed: this gateway can begin waiting a heartbeat after
		another profile's pairing already stored one, and comparing against what it saw at the start would then wait
		forever for a change that never comes. Measured on the real machine - the second profile paired again three
		seconds after the token it needed had arrived, because the token was there before its wait began.

		A stale token is not a trap: the connection refuses it, the refusal clears it, and pairing follows.
		"""
		while True:
			if _load_token(self._url):
				return

			await asyncio.sleep(REFUSAL_RETRY_S)

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

	async def send_final_ledgered(
		self,
		event: MessageEvent,
		session_key: str,
		text_content: str,
		metadata: dict[str, Any] | None,
		*,
		reply_to: str | None,
		is_ephemeral_response: bool = False,
	):
		"""The turn's final answer, marked as one: see ``_turn_final_metadata``.

		Hermes brackets every final text through here - the normal lane and the queued lane both - so this is the one
		place the fact can be attached without guessing at the words from the other end of the wire.
		"""
		return await super().send_final_ledgered(
			event,
			session_key,
			text_content,
			_turn_final_metadata(metadata),
			reply_to=reply_to,
			is_ephemeral_response=is_ephemeral_response,
		)

	async def send(
		self,
		chat_id: str,
		content: str,
		reply_to: str | None = None,
		metadata: dict[str, Any] | None = None,
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

		# A tapped choice on a clarify question. Resolved here for a reason that is not stylistic: the gateway
		# resolves a TYPED reply only when it is a number or one of the labels, and reads anything else as prose -
		# so a callback id handed over as text leaves the agent parked AND lands in the conversation as the
		# reader's own words.
		#
		# It is NOT handed over afterwards either. A tap is an answer, not something the reader said: handing the
		# choice to the gateway as a message makes a turn of it, and what a turn records is the runtime's own
		# preamble for an inbound gateway message - so the conversation ends up showing that instead of the choice.
		# The agent receives the choice where it asked for it, as the answer to its own question.
		if text.startswith(CLARIFY_PREFIX):
			said = await self._resolve_clarify(text, chat_id)

			if said:
				logger.info("[%s] A tap answered the clarify as %s", self.name, said)

			return web.json_response({"ok": True})

		# A decision on an approval this adapter raised. Same reasoning as a tap on a question: the callback id is
		# a wire format, and handing it to the gateway as text decides nothing.
		if text.startswith(APPROVAL_PREFIX):
			decision = await self._resolve_approval(text, chat_id)

			if decision:
				logger.info("[%s] A decision answered the approval as %s", self.name, decision)

			return web.json_response({"ok": True})

		# Authorized by ROLE, not by a user id, and this is the whole reason a Familiar command is not answered with
		# a pairing code: this message reached the ingress down the channel THIS machine opened, authenticated by the
		# instance token - a token that exists because the owner paired Familiar with this machine in the first place.
		# Asking Hermes for a second pairing would be asking the owner to vouch for themselves twice.
		#
		# And the failure was worse than a nuisance: the first unauthorized message is answered with a code, and every
		# one after it inside the rate-limit window is DROPPED SILENTLY (`run_inbound.py`), which is why `/status` came
		# back with a pairing prompt while `/context` and `/compress` never answered at all.
		source = self.build_source(
			chat_id=chat_id,
			chat_name=str(payload.get("channelName") or chat_id),
			chat_type="dm",
			user_id=str(payload.get("userId") or "familiar"),
			user_name=str(payload.get("userName") or "Familiar"),
			role_authorized=True,
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

		def _report(task: asyncio.Task) -> None:
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
		choices: list[str] | None = None,
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

		# Remembered before it goes out: the answer comes back as a tap carrying only this id, and what was picked
		# is the question's to say. Bounded, because a clarify that is never answered is never cleared here.
		self._clarifies[str(clarify_id)] = (session_key, list(choices or []))
		while len(self._clarifies) > 32:
			self._clarifies.pop(next(iter(self._clarifies)))

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
		expires_in: int | None = None
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

	async def _apply_action(self, payload: dict[str, Any], chat_id: str) -> bool:
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

	def _apply_model(self, key: str, payload: dict[str, Any]) -> None:
		"""Point the conversation at a model. Empty clears it, which is the instance's own default."""
		store = getattr(self, "_session_store", None)
		model = str(payload.get("model") or "").strip()

		if store is None:
			return

		# None clears it, which is how a conversation goes back to the instance's own default.
		store.set_model_override(key, {"model": model} if model else None)
		logger.info("[%s] model for %s is now %s", self.name, key, model or "(the instance's default)")

	def _apply_directory(self, key: str, payload: dict[str, Any]) -> None:
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

	async def _resolve_approval(self, text: str, chat_id: str) -> str:
		"""Resolve the approval the reader decided, answering with the decision as a word.

		The choice already reads as one - ``once``, ``deny``, or a reason - so what is resolved here is the id: an
		approval is resolved by its own request id out of the queue for the session it was asked in, which is what
		this adapter remembered when it carried the request out.
		"""
		parts = text.split(":", 2)
		request_id = parts[1] if len(parts) > 1 else ""
		choice = parts[2] if len(parts) > 2 else ""
		session_key = self._approvals.pop(request_id, "")

		if not request_id or not choice or not session_key:
			await self.send(chat_id, "That approval is no longer waiting, so nothing was decided.")

			return ""

		try:
			from tools import approval as _approval

			resolved = _approval.resolve_gateway_approval(session_key, choice, request_id=request_id)
		except Exception as error:  # noqa: BLE001 - a failed resolve is still an answer the reader needs
			logger.warning("[%s] Could not resolve the approval: %s", self.name, error)
			resolved = 0

		if not resolved:
			await self.send(chat_id, "That approval is no longer waiting, so nothing was decided.")

			return ""

		return choice

	async def _resolve_clarify(self, text: str, chat_id: str) -> str:
		"""Resolve the clarify the reader tapped, answering with the choice in its own words.

		A tap carries the id the question was asked with and the position picked, because that is the callback
		every adapter's buttons build. Nothing about it is printable, so the choice is looked up from what this
		adapter remembered asking and handed to the gateway's own tool, which is where the agent is blocked and
		where it will read the choice as the answer to its question.

		Empty when there is nothing left to answer: an answered or expired question is not an error, but the reader
		still deserves to be told nothing was done.
		"""
		parts = text.split(":", 2)
		clarify_id = parts[1] if len(parts) > 1 else ""
		picked = parts[2] if len(parts) > 2 else ""
		remembered = self._clarifies.pop(clarify_id, None)
		choices = remembered[1] if remembered else []
		index = int(picked) if picked.isdigit() else -1
		gone = "That question is no longer waiting, so nothing was answered."

		if not remembered or not 0 <= index < len(choices):
			await self.send(chat_id, gone)

			return ""

		try:
			from tools import clarify_gateway as _clarify_gateway

			addressed = _clarify_gateway.resolve_gateway_clarify(clarify_id, choices[index])
		except Exception as error:  # noqa: BLE001 - a failed resolve is still an answer the reader needs
			logger.warning("[%s] Could not resolve the clarify: %s", self.name, error)
			addressed = False

		if not addressed:
			await self.send(chat_id, gone)

			return ""

		return f"{index + 1}. {choices[index]}"

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

		# Remembered for the same reason a clarify is: the decision comes back carrying only the request id.
		self._approvals[str(kwargs.get("request_id") or "")] = session_key
		while len(self._approvals) > 32:
			self._approvals.pop(next(iter(self._approvals)))

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


	async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
		return {"name": chat_id or self._home, "type": "dm"}


# -- Plugin registration -----------------------------------------------------


def _env_enablement() -> dict | None:
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

	seed: dict[str, Any] = {"url": url or DEFAULT_URL}

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
	thread_id: str | None = None,
	media_files: list | None = None,
	force_document: bool = False,
) -> dict[str, Any]:
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
	"""Plugin entry point, called once at startup.

	Three things are declared here. The command line is the one a reader meets first: pairing is what every other
	part of this plugin depends on, and it is the one step that cannot be done from Familiar's side - the code
	belongs to the machine. It is a command rather than a paragraph of shell because reading that code means waiting
	out a gateway restart and knowing a port, and neither is the reader's business.

	The other two, and the second is the whole reason a reader never edits a config: the job draft asks
	a MODEL something, and Hermes gates that per plugin. Registering the auxiliary task IS the grant - a task key a
	plugin registered itself is always allowed - and it gives the task its own entry in the machine's model picker, so
	which model drafts a job is a choice in the UI with the machine's main model as the default.
	"""
	# Imported inside the function, like the adapter's other cross-file import: at module level this file and draft.py
	# would be fine, but the check suite loads these files flat and the package load does not, and one spelling that
	# works in both is worth more than a module-level import.
	try:
		from . import draft as _draft
	except ImportError:  # pragma: no cover - the flat load is the check suite's
		import draft as _draft

	# Tolerant of a host that cannot grant it: this plugin's reason to exist is the CHANNEL, and a machine whose Hermes
	# predates the plugin-LLM API must still get one. Without the grant the draft answers "no model handle" - which is
	# the truth - rather than the channel going down with it.
	if hasattr(ctx, "register_auxiliary_task"):
		ctx.register_auxiliary_task(
			_draft.DRAFT_TASK,
			display_name=_draft.DRAFT_TASK_LABEL,
			description=_draft.DRAFT_TASK_DESCRIPTION,
		)
	_draft.set_llm(getattr(ctx, "llm", None))

	# The command line: `hermes familiar pair`. Imported inside the function for the same reason draft.py is - the
	# check suite loads these files flat and the package load does not - and registered only where the host HAS the
	# API, so a machine whose Hermes predates it still gets its channel.
	try:
		from . import familiar_cli as _cli
	except ImportError:  # pragma: no cover - the flat load the check suite uses
		import familiar_cli as _cli

	if hasattr(ctx, "register_cli_command"):
		ctx.register_cli_command(
			name="familiar",
			help="Pair this machine with a Familiar, and read the code it is showing",
			setup_fn=_cli.register_cli,
			handler_fn=_cli.familiar_command,
			description=(
				"Familiar shows one code and waits for it to be typed. This reads it, waiting for a gateway that is "
				"restarting first - so one command replaces the endpoint, the port and the timing."
			),
		)

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

