"""Check this plugin against the Hermes it is installed into: registration, enablement, and the POST it makes.

Runs against a stub HTTP receiver on loopback, so the assertions are about what actually goes on the wire -
path, bearer header, payload - and how each failure came back. It imports `gateway.*` and `cron.*`, which
live in the Hermes install, so run it with that install's interpreter and from that directory:

    cd "$LOCALAPPDATA/hermes/hermes-agent"                    # ~/.hermes/hermes-agent on Linux
    ./venv/Scripts/python.exe /path/to/hermes-familiar/check.py   # venv/bin/python on Linux

Nothing here touches the real ~/.hermes: HERMES_HOME is pointed at a temp directory first.
"""

import asyncio
import contextlib
import json
import os
import socket
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

PLUGIN_DIR = str(Path(__file__).resolve().parent)

# A temp home, so nothing here reads or writes the real ~/.hermes.
os.environ["HERMES_HOME"] = tempfile.mkdtemp(prefix="familiar-platform-test-")


def _free_port() -> int:
    """A port nothing is using, for the ingress these checks bind.

    Not the DEFAULT one: a machine already running this plugin holds 8644, so asserting against it would fail on
    exactly the machines where the plugin is working - which is how this check first went red.
    """
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


# Every machine below binds this one, and the checks that stand one up do not overlap.
os.environ["FAMILIAR_INGRESS_PORT"] = str(_free_port())
sys.path.insert(0, PLUGIN_DIR)

RECEIVED: list = []
REPLY = {"code": 201, "body": json.dumps({"id": "notif_1"})}


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length).decode("utf-8")
        RECEIVED.append(
            {
                "path": self.path,
                "auth": self.headers.get("Authorization"),
                "agent": self.headers.get("User-Agent"),
                "json": json.loads(raw),
            }
        )
        self.send_response(REPLY["code"])
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(REPLY["body"].encode("utf-8"))

    def log_message(self, *args):  # silence the stub
        pass


server = HTTPServer(("127.0.0.1", 0), Handler)
PORT = server.server_address[1]
threading.Thread(target=server.serve_forever, daemon=True).start()
URL = f"http://127.0.0.1:{PORT}"

os.environ["FAMILIAR_URL"] = URL
os.environ["FAMILIAR_TOKEN"] = "tok_abc123"
os.environ["FAMILIAR_INSTANCE"] = "homelab"

import adapter  # noqa: E402
from gateway.config import Platform, PlatformConfig  # noqa: E402
from gateway.platform_registry import PlatformEntry, platform_registry  # noqa: E402

PASSED = []
FAILED = []


def check(label: str, condition: bool, detail: str = "") -> None:
    (PASSED if condition else FAILED).append(label)
    print(f"  {'PASS' if condition else 'FAIL'}  {label}{'' if condition else f'  <- {detail}'}")


@contextlib.contextmanager
def without_env(*names):
    """Drop env vars for a check: every setting falls back to the environment, so "not configured" has to
    mean the environment is empty too, not just the extra dict."""
    saved = {name: os.environ.pop(name, None) for name in names}
    try:
        yield
    finally:
        for name, value in saved.items():
            if value is not None:
                os.environ[name] = value


def config(**extra) -> PlatformConfig:
    return PlatformConfig(enabled=True, extra=extra)


def send(adapter_obj, chat_id="default", content="Job output here", metadata=None):
    return asyncio.run(adapter_obj.send(chat_id, content, metadata=metadata))


print(f"stub receiver on {URL}\n")

# A real job in this run's cron store, so the name lookup is exercised against the same API a gateway uses.
# HERMES_HOME already points at a temp directory, so nothing here reads or writes the real one.
from cron.jobs import create_job  # noqa: E402

JOB_ID = create_job(prompt="probe", schedule="0 4 * * *", name="Delivery probe")["id"]

# 1. Registration: what Hermes reads off the entry.
print("registration")
class Ctx:
    def __init__(self):
        self.kwargs = None

    def register_platform(self, **kwargs):
        self.kwargs = kwargs


ctx = Ctx()
adapter.register(ctx)
kwargs = ctx.kwargs or {}
check("registers the platform 'familiar'", kwargs.get("name") == "familiar", str(kwargs.get("name")))
check(
    "declares the cron delivery env var, which is what makes `deliver: familiar` legal",
    kwargs.get("cron_deliver_env_var") == "FAMILIAR_HOME_CHANNEL",
    str(kwargs.get("cron_deliver_env_var")),
)
check("ships a standalone sender for a tick with no gateway", callable(kwargs.get("standalone_sender_fn")))
check("is_connected is the credential check the gateway gates enablement on", kwargs.get("is_connected") is adapter.is_connected)
platform_registry.register(PlatformEntry(**kwargs))
check("resolves in the real registry", platform_registry.get("familiar") is not None)
check("Platform('familiar') is now a valid member", Platform("familiar").value == "familiar")

# 1b. What the scheduler asks before it accepts `deliver: familiar` on a job.
print("\nthe cron delivery gate")
from cron.scheduler_delivery import _is_known_delivery_platform, _resolve_home_env_var  # noqa: E402

check("a job may name `familiar` as its delivery target", _is_known_delivery_platform("familiar") is True)
check("an unregistered name is still refused", _is_known_delivery_platform("nope-not-a-platform") is False)
check("the bare platform resolves to the home channel env var", _resolve_home_env_var("familiar") == "FAMILIAR_HOME_CHANNEL")

# 2. Enablement from the environment.
print("\nenablement")
ADAPTER_KWARGS = dict(kwargs)
check("registered entry carries the env var name", platform_registry.get("familiar").cron_deliver_env_var == "FAMILIAR_HOME_CHANNEL")
seed = adapter._env_enablement()
check("seeds extra from env", seed and seed.get("url") == URL and seed.get("token") == "tok_abc123", str(seed))
check("lifts a home channel", seed.get("home_channel") == {"chat_id": "default", "name": "Familiar"}, str(seed.get("home_channel")))
check("is_connected with a token", adapter.is_connected(config(token="tok_abc123", url=URL)))
with without_env("FAMILIAR_TOKEN"):
    # A machine pointed at a Familiar but not yet paired is CONFIGURED: it loads, asks for a code, and is paired by
    # whoever is sitting at it. Requiring a token here would refuse to load the one machine that pairs.
    check("connected without a token too, because a machine that can pair is configured", adapter.is_connected(config(url=URL)))
with without_env("FAMILIAR_URL", "FAMILIAR_TOKEN"):
    check("nowhere to talk to means no seed, so the platform stays disabled", adapter._env_enablement() is None)
with without_env("FAMILIAR_TOKEN"):
    unpair = adapter._env_enablement()
check("a URL alone seeds, with no token on it", unpair is not None and "token" not in unpair, str(unpair))

live = adapter.FamiliarAdapter(config(url=URL, token="tok_abc123"))

# A message from Familiar arriving down the connection: how a machine with no address is spoken to. It is handed to
# the door that already exists rather than a second implementation of it, so the first thing to assert is that a
# frame with nothing in it is refused instead of started.
check(
    "a say carrying no message is refused rather than run",
    asyncio.run(live._channel_say({"message": {}})) == {"error": "no message"},
)

# And that a real one goes to this machine's own ingress, which is stubbed here: the point is that it arrives
# somewhere rather than being silently dropped, because Familiar is waiting on this answer.
_stub_port = int(URL.rsplit(":", 1)[1])
_held_port = live._ingress_port
try:
    live._ingress_port = _stub_port
    _said = asyncio.run(live._channel_say({"message": {"text": "hello", "chatId": "default"}}))
finally:
    live._ingress_port = _held_port
check("a say carrying a message reaches this machine's own ingress", isinstance(_said, dict), str(_said))
check("connect() reports ready", asyncio.run(live.connect()) is True)
check("home target defaults to 'default'", live._home == "default")
with without_env("FAMILIAR_TOKEN"):
    # The channel is the direction that needs NOTHING on this machine: it dials out. So an unpaired machine still
    # connects - that is how it pairs.
    unpaired_machine = adapter.FamiliarAdapter(config(url=URL))

    # One loop for the whole thing: the ingress binds a socket, and a socket belongs to the loop that opened it, so
    # connecting on one loop and disconnecting on another tears down a proactor that is already gone.
    class _Unpaired:
        headers = {"Authorization": "Bearer "}

        async def json(self):
            return {"text": "hello", "channel": "default"}

    async def _unpaired_run():
        started = await unpaired_machine.connect()
        bound = unpaired_machine._ingress_runner is not None
        code = await unpaired_machine._handle_pair_probe(None)

        # Asked with a code already in hand, the same endpoint hands back THAT code and starts nothing: it is what a
        # person sitting at the machine runs, so the answer has to be the code they can type - not a second one, and
        # not "no code" from an endpoint that had one all along.
        unpaired_machine._pair_code = "PROBE1"
        held = json.loads((await unpaired_machine._handle_pair_probe(None)).text)

        refused = await unpaired_machine._handle_ingress(_Unpaired())

        # What pairing supplies, and the fact that the listener takes it in the same breath: seeded from the config
        # the credential is EMPTY on this machine, so a listener that came up before pairing would otherwise keep
        # refusing its own calls until a restart.
        unpaired_machine._take_token("a-token-from-pairing")
        adopted = unpaired_machine._ingress_token

        if unpaired_machine._channel_task:
            unpaired_machine._channel_task.cancel()

        await unpaired_machine.disconnect()

        return started, bound, code, held, refused, adopted

    _started, _bound, _code, _held, _refused, _adopted = asyncio.run(_unpaired_run())
    check("connect() starts without a token, because the channel dials out", _started is True)
    # The code this machine shows is read from the pair endpoint ON the machine, and whoever reads it is sitting at
    # the machine that has not paired yet - the reader the guide sends there. A listener that waited for a token left
    # exactly that reader with nothing to read.
    check("and brings its ingress up with it, so the code can be read on the machine", _bound is True)
    check("the pair endpoint answers there, before any token exists", getattr(_code, "status", None) == 200, str(_code))
    # What it answers is the code in hand, and answering costs nothing: a reader who runs the command ONCE gets the
    # code, rather than a "not yet" they would have no way of knowing to ask again for.
    check("and gives back the code it already had", _held.get("code") == "PROBE1", str(_held))
    # What is NOT open is a MESSAGE: the route authenticates with the token this machine pairs with, and there is
    # none yet, so a caller that can reach loopback is refused rather than turned into a turn.
    check(
        "while a message through it is refused until this machine is paired",
        getattr(_refused, "status", None) == 401,
        str(_refused),
    )
    check(
        "a token learned by pairing becomes the ingress credential",
        _adopted == "a-token-from-pairing",
        _adopted,
    )

# 2b. A token this Familiar has refused, whichever place it came from.
# A machine can hold two: the one pairing gave it, kept in its state file, and the one it was configured with.
# Only the first is this plugin's to erase - the second is somebody's deliberate setting, so a refusal is
# REMEMBERED against it. That is what makes "the instance was deleted, now connect it again" work at all: without
# it the machine offers the same dead token forever, reports itself paired, and shows no code for anyone to type.
print("\nthe token a machine presents")
adapter._clear_token()  # a machine that has not paired
check(
    "a configured token is offered while nothing is paired",
    live._presented_token() == "tok_abc123",
    live._presented_token(),
)
adapter._mark_refused("tok_abc123")
check(
    "once Familiar refuses it, it is not offered again, so the machine pairs",
    live._presented_token() == "",
    repr(live._presented_token()),
)
probe = json.loads(asyncio.run(live._handle_pair_probe(None)).text)
check("and the pair endpoint stops claiming to be paired", probe["paired"] is False, str(probe))
adapter._save_token("a-token-from-pairing")
check(
    "a token from pairing wins over the configured one",
    live._presented_token() == "a-token-from-pairing",
    live._presented_token(),
)
adapter._clear_token()
check("and the check leaves no pairing behind", not adapter._state_path().exists())

# The token has to survive an UPDATE, and an update is `plugins install --force`: it removes this plugin's whole
# directory and lays a new one down. Beside the plugin is therefore the one place this state cannot live.
module_file = adapter.__file__
plugin_dir = Path(module_file).resolve().parent
state_path = adapter._state_path().resolve()
check("the pairing is not kept inside the plugin directory", plugin_dir not in state_path.parents, str(state_path))
check(
    "and lives under Hermes' home instead",
    state_path.parent == Path(os.environ["HERMES_HOME"]).resolve() / adapter.STATE_DIR,
    str(state_path),
)

# An install from before this change kept it beside the plugin, and that file is found and MOVED - once: a machine
# that already paired must not pair again because the plugin learned where to put things.
with tempfile.TemporaryDirectory() as legacy_where:
    adapter.__file__ = str(Path(legacy_where) / "adapter.py")
    Path(adapter.__file__).write_text("", encoding="utf-8")
    (Path(legacy_where) / adapter.STATE_FILE).write_text(json.dumps({"token": "tok_legacy"}), encoding="utf-8")

    adopted = adapter._load_token()
    # Where the plugin SAYS its state is, rather than recomputing it from the environment: the home is resolved once
    # at import, so a check that recomputed it would be checking something the plugin never looks at.
    where_now = adapter._state_path().resolve()
    left_behind = (Path(legacy_where) / adapter.STATE_FILE).exists()

    adapter.__file__ = module_file

check("a pairing kept beside the plugin is still found", adopted == "tok_legacy", adopted)
check(
    "and moved to where this plugin keeps its state, so the next update keeps it",
    where_now.exists() and not left_behind and plugin_dir not in where_now.parents,
    f"at={where_now} exists={where_now.exists()} left_behind={left_behind}",
)
adapter._clear_token()  # and the check leaves nothing behind where it moved it to

# The jobs list is a PAGE of what this machine has scheduled, sorted by the column the reader clicked. The app's
# table asks for what it shows, so a machine with a hundred jobs answers with ten - and the sort travels with the
# question rather than the app holding every row to sort them itself.
print("\nthe jobs a machine lists")


class _FakeCron:
	"""The machine's job store, with enough in it to page and to sort."""

	@staticmethod
	def list(include_disabled: bool = False) -> list:
		_ = include_disabled

		return [
			{"id": "c", "name": "Charlie", "last_run": "2026-01-03T00:00:00+00:00"},
			{"id": "a", "name": "Alpha", "last_run": None},
			{"id": "b", "name": "Bravo", "last_run": "2026-01-02T00:00:00+00:00"},
		]


def _jobs(query: str) -> dict:
	parsed: Dict[str, List[str]] = {}

	for pair in query.split("&"):
		if pair:
			key, _, value = pair.partition("=")
			parsed.setdefault(key, []).append(value)

	return asyncio.run(live._api_list_jobs({}, parsed, None))["body"]


_cron_before = live._cron
live._cron = lambda: {"list": _FakeCron.list}

_first = _jobs("limit=2")
check("a page is what was asked for, not the whole list", len(_first["data"]) == 2, str(_first["data"]))
check("and it says how many there are in all", _first["total"] == 3, str(_first["total"]))
check("and that there is another page", _first["has_more"] is True, str(_first["has_more"]))

_rest = _jobs("limit=2&offset=2")
check("the second page is the rest of it", [job["name"] for job in _rest["data"]] == ["Bravo"], str(_rest["data"]))
check("and the last page knows it is the last", _rest["has_more"] is False, str(_rest["has_more"]))

# A sort asked for changes nothing: a table holding one page cannot sort the list, so the order stays the machine's.
_asked_to_sort = _jobs("sort=name&order=desc")
check(
	"asking for a sort does not reorder the machine's own",
	[job["name"] for job in _asked_to_sort["data"]] == ["Charlie", "Alpha", "Bravo"],
	str([job["name"] for job in _asked_to_sort["data"]]),
)

_searched = _jobs("search=brav")
check("and a search is a page of the matches", [job["name"] for job in _searched["data"]] == ["Bravo"], str(_searched["data"]))

live._cron = _cron_before

# 3. A delivery, and what the receiver sees.
print("\ndelivery")
result = send(live, metadata={"job_id": JOB_ID})
check("reports success", result.success is True, str(result.error))
check("keeps the id Familiar returned", result.message_id == "notif_1", str(result.message_id))
# A reply is a MESSAGE, not a job delivery: since the channel carries the chat, what send() posts must not
# land in the notification list an operator reads cron output from.
check("posts a reply to the message path, not the notification list",
      RECEIVED[-1]["path"] == "/api/hermes/message", RECEIVED[-1]["path"])
check("sends the bearer token", RECEIVED[-1]["auth"] == "Bearer tok_abc123", str(RECEIVED[-1]["auth"]))
payload = RECEIVED[-1]["json"]
check("instance", payload["instance"] == "homelab", str(payload))
check("target", payload["target"] == "default", str(payload))
check("content", payload["content"] == "Job output here", str(payload))
check("jobId rides from the route metadata", payload["jobId"] == str(JOB_ID), str(payload))
check(
    "jobName is looked up and sent, so a client can title the notification with it",
    payload["jobName"] == "Delivery probe",
    str(payload.get("jobName")),
)
check("truncated is false for a short brief", payload["truncated"] is False, str(payload))
check("sentAt is an ISO instant", payload["sentAt"].endswith("+00:00") and "T" in payload["sentAt"], str(payload["sentAt"]))

send(live, chat_id="ops", metadata=None)
check("an explicit target is used as given", RECEIVED[-1]["json"]["target"] == "ops", str(RECEIVED[-1]["json"]["target"]))
check("no job means null, not a missing field", RECEIVED[-1]["json"]["jobId"] is None, str(RECEIVED[-1]["json"]))

send(adapter.FamiliarAdapter(config(url=URL, token="tok", home_channel="inbox")), chat_id="")
check(
    "extra.home_channel is the fallback when no HomeChannel is lifted",
    RECEIVED[-1]["json"]["target"] == "inbox",
    str(RECEIVED[-1]["json"]["target"]),
)

long_text = "x" * 30_000
send(live, content=long_text)
check(
    "a long brief is cut at the cap the receiving end enforces, not sent as a burst",
    len(RECEIVED[-1]["json"]["content"]) == 20_000,
    str(len(RECEIVED[-1]["json"]["content"])),
)
check("and says so", RECEIVED[-1]["json"]["truncated"] is True)

# 4b. The build, so "did my update land?" is answered by looking rather than by guessing.
manifest_version = next(
    line.split(":", 1)[1].strip()
    for line in (Path(PLUGIN_DIR) / "plugin.yaml").read_text(encoding="utf-8").split("\n")
    if line.startswith("version:")
)
check(
    "a delivery says which build this instance is running",
    RECEIVED[-1]["json"]["pluginVersion"] == manifest_version,
    "%s vs %s" % (RECEIVED[-1]["json"].get("pluginVersion"), manifest_version),
)

# 4. The conversation's session: the one fact only this process can produce.
print("\nsession")


class _FakeEntry:
    def __init__(self, key, session_id):
        self.session_key = key
        self.session_id = session_id


class _FakeStore:
    """The routing index, which is the only thing that knows which SESSION an address is on."""

    def __init__(self):
        self.current = "20261005_120000_abcdef12"
        self.switched_to = []
        self.models = []

    def get_or_create_session(self, source):
        return _FakeEntry("agent:main:familiar:dm:" + str(source.chat_id), self.current)

    def switch_session(self, key, target):
        self.switched_to.append(target)
        self.current = target
        return _FakeEntry(key, target)

    def peek_session_id(self, key):
        return self.current

    def set_model_override(self, key, override):
        self.models.append((key, override))
        return None


store = _FakeStore()
live.set_session_store(store)
source = live.build_source(
    chat_id="default", chat_name="default", chat_type="dm", user_id="familiar", user_name="Familiar")

check("the ingress names the session the address is on",
      asyncio.run(live._resolve_session(source)) == "20261005_120000_abcdef12")
check("a reply carries it, so a client can follow where its conversation went",
      live._current_session_id("default") == "20261005_120000_abcdef12")
store.current = "20261005_130000_99999999"
check("and it is read per reply, so a rotation DURING the turn is already reflected",
      live._current_session_id("default") == "20261005_130000_99999999")
check("a declared session is adopted, which is how a fork is carried on",
      asyncio.run(live._resolve_session(source, declared="20261005_090000_forked01")) == "20261005_090000_forked01")
check("by pointing the key at it, the move /resume makes", store.switched_to == ["20261005_090000_forked01"])
asyncio.run(live._resolve_session(source, declared="20261005_090000_forked01"))
check("and one already on the key is left alone", store.switched_to == ["20261005_090000_forked01"])

# 4b. What Familiar changes about a conversation, which is a setting and not something said.
print("\nsettings the app pushes")


class _FakeRequest:
    """Enough of an aiohttp request for the ingress: a bearer header and a JSON body."""

    def __init__(self, payload, token="tok_abc123"):
        self.headers = {"Authorization": f"Bearer {token}"}
        self._payload = payload

    async def json(self):
        return self._payload


def _no_turn(*_args, **_kwargs):
    raise AssertionError("a setting became a turn")


live._message_handler = _no_turn
accepted = asyncio.run(
    live._handle_ingress(_FakeRequest({"action": "model", "channel": "default", "model": "claude-sonnet-4"}))
)
check("a setting is accepted without being said in the conversation", accepted.status == 200, str(accepted.status))
check(
    "and lands on the CONVERSATION's key, not on a session id",
    store.models and store.models[-1][0] == "agent:main:familiar:dm:default",
    str(store.models[-1] if store.models else None),
)
check(
    "as the override the gateway keeps, which is what survives /new",
    store.models[-1][1] == {"model": "claude-sonnet-4"},
    str(store.models[-1][1] if store.models else None),
)

asyncio.run(live._handle_ingress(_FakeRequest({"action": "model", "channel": "default", "model": ""})))
check(
    "an empty model clears it, which is how a conversation goes back to the instance's own default",
    store.models[-1][1] is None,
    str(store.models[-1][1]),
)

asyncio.run(
    live._handle_ingress(_FakeRequest({"action": "directory", "channel": "default", "directory": "/srv/work"}))
)
from tools.terminal_tool import _task_env_overrides  # noqa: E402

check(
    "a working directory is applied to the CONVERSATION, which is what survives a new session",
    _task_env_overrides.get("agent:main:familiar:dm:default", {}).get("cwd") == "/srv/work",
    str(_task_env_overrides.get("agent:main:familiar:dm:default")),
)
check(
    "and says it is the session's own workspace, not wherever the gateway was launched",
    _task_env_overrides.get("agent:main:familiar:dm:default", {}).get("cwd_source") == "session",
    str(_task_env_overrides.get("agent:main:familiar:dm:default")),
)

asyncio.run(live._handle_ingress(_FakeRequest({"action": "something-not-invented-here", "channel": "default"})))
check(
    "an action this plugin does not know is ignored rather than guessed at",
    store.models[-1][0] == "agent:main:familiar:dm:default",
    str(store.models[-1]),
)

# 4c. The read: what a machine with no address answers about its own conversations.
#
# A machine behind NAT has no address and no key, so nothing that reads an instance over HTTP applies to it. The
# app asks down the connection instead - the same paths it would have asked an addressed instance - and this
# machine answers them from Hermes' own store, in the API's own shape, because the app parses one shape.
#
# The conversations here are REAL rows in this run's temp HERMES_HOME, so the store opened is the one a gateway
# would open, and what is asserted is what the machine actually reports about itself.
print("\nthe read")

from hermes_constants import get_hermes_home  # noqa: E402
from hermes_state_registry import acquire, release_or_close  # noqa: E402

READ_ID = "20261006_101500_readers01"
OTHER_ID = "20261006_101600_readers02"


def _seed_sessions() -> None:
    db = acquire(Path(get_hermes_home()) / "state.db")
    try:
        for session_id, title in ((READ_ID, "Read me"), (OTHER_ID, "Somebody else")):
            db.create_session(session_id, "familiar", model="deepseek-flash")
            db.set_session_title(session_id, title)
        db.append_message(READ_ID, "user", "what is in this directory?")
        db.append_message(READ_ID, "assistant", "three files and a README")
    finally:
        release_or_close(db)


_seed_sessions()


def asked(frame: dict) -> dict:
    """Ask this machine one path the way Familiar does, and hand back the answer it posted.

    Through `_answer_request`, which is the real door: the answer travels back to the stub receiver exactly as it
    travels back to Familiar, so what is asserted is what goes on the wire and not what a method returns.
    """
    before = len(RECEIVED)
    asyncio.run(live._answer_request(frame, "tok_abc123"))

    return RECEIVED[-1] if len(RECEIVED) == before + 1 else {}


def result_of(frame: dict) -> dict:
    """The result an answered request carried, and the proof it was posted at all."""
    posted = asked(frame)
    check(
        f"an answer for {frame['method']} {frame['path'].split('?')[0]} is posted to the reply path",
        posted.get("path") == "/api/channel/reply",
        str(posted.get("path")),
    )

    return (posted.get("json") or {}).get("result") or {}


listed = result_of({"id": "read-1", "action": "api", "method": "GET", "path": "/api/sessions"})
check(
    "a reply that names no instance still says who called",
    RECEIVED[-1]["agent"] == "hermes-familiar",
    str(RECEIVED[-1]["agent"]),
)
body = listed.get("body") or {}
check(
    "a list of conversations comes back in the API's own shape",
    listed.get("status") == 200 and body.get("object") == "list",
    str(listed)[:160],
)
row = next((item for item in body.get("data") or [] if item.get("id") == READ_ID), None)
check("carrying the conversation this machine actually has", row is not None, str(body.get("data"))[:200])
check("with its own title", (row or {}).get("title") == "Read me", str(row))
check("and its flags as booleans, not as SQLite's 0/1", isinstance((row or {}).get("pinned"), bool), str(row))
check("and no page beyond this one", body.get("has_more") is False, str(body.get("has_more")))

# A search is the store's own filter, and it is applied BEFORE the window: page two of a search is page two of the
# MATCHES, which is the whole reason the filtering is asked for here instead of being left to the client.
found = result_of({
    "id": "read-search", "action": "api", "method": "GET", "path": "/api/sessions?search=else&limit=5"})
found_body = found.get("body") or {}
check(
    "a search narrows the list to what matches, by title",
    [item.get("id") for item in found_body.get("data") or []] == [OTHER_ID],
    str(found_body.get("data"))[:200],
)
check(
    "and a search that comes back short of the window is EXHAUSTED, so it can say how many there are",
    found_body.get("total") == 1 and found_body.get("has_more") is False,
    f"total={found_body.get('total')} has_more={found_body.get('has_more')}",
)

# The store's search matches an ID as well as a title, which is how a conversation is found by a handle from a log
# line or a link. Both fixtures' ids contain "readers", so this also pins that the match really is on ids.
by_id = result_of({
    "id": "read-by-id", "action": "api", "method": "GET", "path": "/api/sessions?search=readers01&limit=5"})
check(
    "and by id, not only by title",
    [item.get("id") for item in ((by_id.get("body") or {}).get("data") or [])] == [READ_ID],
    str((by_id.get("body") or {}).get("data"))[:200],
)

# "read" matches BOTH fixtures - one by title, both by id - so the page still has to be a page.
both = result_of({"id": "read-both", "action": "api", "method": "GET", "path": "/api/sessions?search=read&limit=5"})
check(
    "a search that matches several still pages over them",
    len((both.get("body") or {}).get("data") or []) == 2 and (both.get("body") or {}).get("total") == 2,
    str(both.get("body"))[:200],
)

browse = result_of({"id": "read-count", "action": "api", "method": "GET", "path": "/api/sessions?limit=5"})
check(
    "a listing with no search is counted with the store's own count, so a numbered pager has a real total",
    (browse.get("body") or {}).get("total") == 2,
    str((browse.get("body") or {}).get("total")),
)

crowded = result_of({"id": "read-crowded", "action": "api", "method": "GET", "path": "/api/sessions?search=e&limit=1"})
crowded_body = crowded.get("body") or {}
check(
    "a search that FILLS the window is counted anyway, by reading the matches its own filter admits",
    crowded_body.get("total") == 2 and crowded_body.get("has_more") is True,
    f"total={crowded_body.get('total')} has_more={crowded_body.get('has_more')}",
)

# The ceiling is the one place this machine answers "not counted": past it, counting a search means reading more
# rows than a page is worth, and a number nobody read is worse than no number. Lowered here rather than seeded,
# because what is being pinned is the ANSWER past the ceiling, not the reading of five thousand rows.
_ceiling = adapter._SEARCH_COUNT_CEILING
adapter._SEARCH_COUNT_CEILING = 1
try:
    capped = result_of({
        "id": "read-capped", "action": "api", "method": "GET", "path": "/api/sessions?search=e&limit=1"})
finally:
    adapter._SEARCH_COUNT_CEILING = _ceiling

capped_body = capped.get("body") or {}
check(
    "and past the ceiling it says it did not count rather than guessing",
    capped_body.get("total") is None and capped_body.get("has_more") is True,
    f"total={capped_body.get('total')} has_more={capped_body.get('has_more')}",
)

one = result_of({"id": "read-2", "action": "api", "method": "GET", "path": f"/api/sessions/{READ_ID}"})
session = (one.get("body") or {}).get("session") or {}
check(
    "one conversation is answered as the API answers it",
    one.get("status") == 200 and (one.get("body") or {}).get("object") == "hermes.session",
    str(one)[:160],
)
check("with a system prompt reported as present or not, never sent", "has_system_prompt" in session, str(session)[:160])
check("and the instance's own count of what is in it", session.get("message_count") == 2, str(session.get("message_count")))

messages = result_of({"id": "read-3", "action": "api", "method": "GET", "path": f"/api/sessions/{READ_ID}/messages"})
transcript = messages.get("body") or {}
check(
    "a transcript is answered under the session it resolved to",
    transcript.get("session_id") == READ_ID and transcript.get("object") == "list",
    str(transcript)[:160],
)
check(
    "with its messages in the API's shape",
    [message.get("role") for message in transcript.get("data") or []] == ["user", "assistant"],
    str(transcript.get("data"))[:200],
)
check(
    "and how the page was read, which is what a caller pages with",
    (transcript.get("pagination") or {}).get("order") == "latest"
    and (transcript.get("pagination") or {}).get("returned") == 2,
    str(transcript.get("pagination")),
)

page = result_of({
    "id": "read-4", "action": "api", "method": "GET", "path": f"/api/sessions/{READ_ID}/messages?order=oldest&limit=1"})
check(
    "an explicit page is honoured",
    (page.get("body") or {}).get("pagination") == {"limit": 1, "offset": 0, "order": "oldest", "returned": 1},
    str((page.get("body") or {}).get("pagination")),
)

missing = result_of({"id": "read-5", "action": "api", "method": "GET", "path": "/api/sessions/nobody-has-this-one"})
check(
    "a conversation this machine does not have is a 404, not an empty one",
    missing.get("status") == 404,
    str(missing)[:160],
)
check(
    "saying which one it could not find",
    ((missing.get("body") or {}).get("error") or {}).get("code") == "session_not_found",
    str(missing.get("body")),
)

renamed = result_of({
    "id": "read-6", "action": "api", "method": "PATCH", "path": f"/api/sessions/{READ_ID}",
    "body": {"title": "Renamed here"}})
check(
    "a rename is carried, and answered with the conversation as it now is",
    (((renamed.get("body") or {}).get("session")) or {}).get("title") == "Renamed here",
    str(renamed)[:160],
)

db = acquire(Path(get_hermes_home()) / "state.db")
try:
    check(
        "and it landed on this machine, not only in the answer",
        (db.get_session(READ_ID) or {}).get("title") == "Renamed here",
        str((db.get_session(READ_ID) or {}).get("title")),
    )
finally:
    release_or_close(db)

taken = result_of({
    "id": "read-7", "action": "api", "method": "PATCH", "path": f"/api/sessions/{READ_ID}",
    "body": {"title": "Somebody else"}})
check(
    "a name another conversation already holds is refused",
    taken.get("status") == 400 and ((taken.get("body") or {}).get("error") or {}).get("code") == "invalid_title",
    str(taken)[:200],
)

wrong_field = result_of({
    "id": "read-8", "action": "api", "method": "PATCH", "path": f"/api/sessions/{READ_ID}",
    "body": {"nonsense": 1}})
check(
    "a field this machine does not know is refused by name rather than ignored",
    wrong_field.get("status") == 400
    and ((wrong_field.get("body") or {}).get("error") or {}).get("code") == "unsupported_session_field",
    str(wrong_field)[:200],
)

# A path this machine does not answer AT ALL: DELETE on a session is carried (below), so the refusal has to
# be asked somewhere nothing is served, or the check would be testing the feature it names.
refused = result_of({"id": "read-9", "action": "api", "method": "DELETE", "path": f"/api/sessions/{READ_ID}/nothing-here"})
check("a path this machine does not answer is refused rather than faked", refused.get("status") == 501, str(refused)[:160])
check(
    "naming what was asked and that it cannot answer it",
    "DELETE" in json.dumps(refused.get("body") or {}) and "does not answer" in json.dumps(refused.get("body") or {}),
    str(refused.get("body")),
)

# 4d. The rest of the surface: branching, deleting, the model picker, and the machine's own schedule.
#
# Same door, same shape: what the app asks an addressed instance over HTTP is answered here from this machine's
# own state, so a machine with no address can do everything a conversation needs - start a topic from a session,
# forget one, fill the model picker, and hold its own schedule.
print("\nwhat else a paired machine answers")

forked = result_of({"id": "more-1", "action": "api", "method": "POST",
	"path": f"/api/sessions/{READ_ID}/fork", "body": {"title": "Branched here"}})
fork_body = forked.get("body") or {}
fork_session = fork_body.get("session") or {}
check(
    "a fork is created and answered as the API answers it",
    forked.get("status") == 201 and fork_body.get("object") == "hermes.session",
    str(forked)[:160],
)
check(
    "under an id of its own, minted where the machine mints one",
    bool(fork_session.get("id")) and fork_session.get("id") != READ_ID,
    str(fork_session.get("id")),
)
check(
    "carrying the conversation it branched from",
    fork_session.get("parent_session_id") == READ_ID,
    str(fork_session.get("parent_session_id")),
)
check("with the name that was asked for", fork_session.get("title") == "Branched here", str(fork_session.get("title")))
fork_id = str(fork_session.get("id") or "")

_db = acquire(Path(get_hermes_home()) / "state.db")
try:
    _source = _db.get_session(READ_ID) or {}
    _branch = _db.get_messages(fork_id) if fork_id else []
    check(
        "and the source is ENDED as branched, which is what makes it a branch rather than a copy",
        _source.get("end_reason") == "branched",
        str(_source.get("end_reason")),
    )
    check("while the branch holds the messages it copied", len(_branch) == 2, str(len(_branch)))
finally:
    release_or_close(_db)

deleted = result_of({"id": "more-2", "action": "api", "method": "DELETE", "path": f"/api/sessions/{fork_id}"})
check(
    "a conversation can be forgotten, and the answer says what it did",
    deleted.get("status") == 200 and (deleted.get("body") or {}).get("deleted") is True,
    str(deleted)[:160],
)
gone = result_of({"id": "more-3", "action": "api", "method": "GET", "path": f"/api/sessions/{fork_id}"})
check("and it is gone afterwards", gone.get("status") == 404, str(gone)[:120])

picked = result_of({"id": "more-4", "action": "api", "method": "GET", "path": "/api/model/options"})
check(
    "the model picker's inventory is answered from this machine",
    picked.get("status") == 200 and isinstance((picked.get("body") or {}).get("providers"), list),
    str(picked)[:200],
)

made = result_of({"id": "more-5", "action": "api", "method": "POST", "path": "/api/jobs",
	"body": {"name": "Made down the connection", "schedule": "0 4 * * *", "prompt": "say hello", "deliver": "familiar"}})
job = (made.get("body") or {}).get("job") or {}
check("a job can be created on this machine", made.get("status") == 200 and bool(job.get("id")), str(made)[:200])
check(
    "as THIS channel's job, so `origin` means where Familiar is",
    (job.get("origin") or {}).get("platform") == "familiar",
    str(job.get("origin")),
)
job_id = str(job.get("id") or "")

listed = result_of({"id": "more-6", "action": "api", "method": "GET", "path": "/api/jobs"})
check(
    "and the machine's own schedule lists it",
    job_id and job_id in json.dumps(listed.get("body") or {}),
    str(listed)[:160],
)

patched = result_of({"id": "more-7", "action": "api", "method": "PATCH", "path": f"/api/jobs/{job_id}",
	"body": {"name": "Renamed down the connection"}})
check(
    "a job's own fields can be changed",
    ((patched.get("body") or {}).get("job") or {}).get("name") == "Renamed down the connection",
    str(patched)[:200],
)

refused_field = result_of({"id": "more-8", "action": "api", "method": "PATCH", "path": f"/api/jobs/{job_id}",
	"body": {"script": "/tmp/probe.sh"}})
check(
    "a field nobody may set is refused rather than stored",
    refused_field.get("status") == 400,
    str(refused_field)[:200],
)

paused = result_of({"id": "more-9", "action": "api", "method": "POST", "path": f"/api/jobs/{job_id}/pause"})
check(
    "a job can be paused from here",
    paused.get("status") == 200 and not ((paused.get("body") or {}).get("job") or {}).get("enabled"),
    str(paused)[:200],
)
resumed = result_of({"id": "more-10", "action": "api", "method": "POST", "path": f"/api/jobs/{job_id}/resume"})
check(
    "and put back on the schedule",
    resumed.get("status") == 200 and bool(((resumed.get("body") or {}).get("job") or {}).get("enabled")),
    str(resumed)[:200],
)

bad_id = result_of({"id": "more-11", "action": "api", "method": "POST", "path": "/api/jobs/not-a-job-id/run"})
check("an id that is not a job id is refused, rather than run", bad_id.get("status") == 400, str(bad_id)[:160])

missing_job = result_of({"id": "more-12", "action": "api", "method": "GET", "path": "/api/jobs/aaaaaaaaaaaa"})
check("a job this machine does not have is a 404", missing_job.get("status") == 404, str(missing_job)[:160])

removed = result_of({"id": "more-13", "action": "api", "method": "DELETE", "path": f"/api/jobs/{job_id}"})
check(
    "and a job can be taken off the schedule",
    removed.get("status") == 200 and (removed.get("body") or {}).get("ok") is True,
    str(removed)[:160],
)

# 5. The asks: a question, a permission, and the gateway's own hold on a command it will not undo.
print("\nasks")

SESSION_KEY = "agent:main:familiar:dm:default"

asyncio.run(
    live.send_clarify("default", "Which store?", ["postgres", "sqlite"], "cl_1", SESSION_KEY)
)
clarify = RECEIVED[-1]
check("a clarify goes to the ask path", clarify["path"] == "/api/hermes/ask", clarify["path"])
check("as a clarify", clarify["json"]["kind"] == "clarify", str(clarify["json"].get("kind")))
check("carrying the id its answer comes back with", clarify["json"]["requestId"] == "cl_1", str(clarify["json"].get("requestId")))

# A TAPPED CHOICE. The callback id is a wire format and reaches the gateway as text only by accident: the
# gateway resolves a TYPED reply when it is a number or one of the labels and reads anything else as prose, so
# an id handed over would leave the agent parked AND put the id in the conversation as the reader's own words.
# It is resolved here instead, against a clarify this process actually registered, and answered with the words
# of the choice - which is what the conversation is then told.
from tools import clarify_gateway as _clarify_gateway  # noqa: E402 - the resolution under test

_clarify_gateway.register("cl_tap", SESSION_KEY, "Which store?", ["postgres", "sqlite"])
live._clarifies["cl_tap"] = (SESSION_KEY, ["postgres", "sqlite"])
_said = asyncio.run(live._resolve_clarify("cl:cl_tap:1", "default"))
check("a tapped choice resolves the clarify it was asked on", _said == "2. sqlite", repr(_said))
check("and the agent reads the CHOICE where it asked, not a message of its own",
      _clarify_gateway.wait_for_response("cl_tap", 0.1) == "sqlite", "the agent did not read the choice")
check("and the question stops waiting", _clarify_gateway.get_pending_for_session(SESSION_KEY) is None)
check("a tap that arrives too late says so rather than deciding anything",
      asyncio.run(live._resolve_clarify("cl:cl_tap:1", "default")) == "", "a stale tap resolved something")
check("and an approval decision is not treated as something the reader said",
      asyncio.run(live._resolve_approval("appr:nope:once", "default")) == "", "a stale approval resolved something")
check("and the choices the reader picks from", clarify["json"]["choices"] == ["postgres", "sqlite"], str(clarify["json"].get("choices")))
check("under the prefix every adapter shares", clarify["json"]["callbackPrefix"] == "cl", str(clarify["json"].get("callbackPrefix")))
check("an ask says it too, so a quiet instance is still identifiable", clarify["json"]["pluginVersion"] == manifest_version, str(clarify["json"].get("pluginVersion")))

asyncio.run(
    live.send_exec_approval(
        "default", "systemctl restart nginx", session_key=SESSION_KEY, description="Restarts the web server", request_id="appr_1"
    )
)
approval = RECEIVED[-1]
check("an approval is its own kind", approval["json"]["kind"] == "approval", str(approval["json"].get("kind")))
check(
    "what would run and why it is asked are not the same thing",
    approval["json"]["command"] == "systemctl restart nginx" and approval["json"]["description"] == "Restarts the web server",
    str(approval["json"]),
)

# The gateway's own hold, registered where it actually lives: the remaining time is read from that entry, not
# assumed, so the countdown a reader sees is the one that is running.
from tools import slash_confirm as slash_confirm_mod  # noqa: E402


async def _hold(choice: str):
    return f"the hold ran with {choice}"


slash_confirm_mod.register(SESSION_KEY, "cf_1", "/new", _hold)
asyncio.run(
    live.send_slash_confirm(
        "default", "Confirm /new", "This starts a fresh session.", SESSION_KEY, "cf_1"
    )
)
confirm = RECEIVED[-1]
check("a confirmation is its own kind, not a message", confirm["json"]["kind"] == "confirm", str(confirm["json"].get("kind")))
check("under its own prefix, so the answer is not read as something said", confirm["json"]["callbackPrefix"] == "cf", str(confirm["json"].get("callbackPrefix")))
check("with the two answers this app offers", confirm["json"]["choices"] == ["once", "cancel"], str(confirm["json"].get("choices")))
check("and no 'always': that is a persisted setting, not a choice in a chat", "always" not in confirm["json"]["choices"])
check(
    "carrying the time left on the hold, read from the hold itself",
    isinstance(confirm["json"]["expiresIn"], int)
    and 0 < confirm["json"]["expiresIn"] <= slash_confirm_mod.DEFAULT_TIMEOUT_SECONDS,
    str(confirm["json"].get("expiresIn")),
)

before_answer = len(RECEIVED)
asyncio.run(live._resolve_confirm("cf:cf_1:once", "default"))
check("answering it resolves the hold rather than becoming a turn", slash_confirm_mod.get_pending(SESSION_KEY) is None)
check("and says what the gateway did", RECEIVED[-1]["json"]["content"] == "the hold ran with once", str(RECEIVED[-1]["json"].get("content")))
check("as a reply, on the message path", RECEIVED[-1]["path"] == "/api/hermes/message" and len(RECEIVED) == before_answer + 1)

asyncio.run(live._resolve_confirm("cf:nobody-knows-this:once", "default"))
check(
    "an answer to a hold that is gone says nothing was done, instead of a spinner",
    "nothing was done" in RECEIVED[-1]["json"]["content"],
    str(RECEIVED[-1]["json"].get("content")),
)

asyncio.run(live.send_slash_confirm("default", "Confirm /clear", "Clears the transcript.", SESSION_KEY, "cf_unknown"))
check(
    "a hold this process cannot see still goes out, and says it has no clock rather than inventing one",
    RECEIVED[-1]["json"]["kind"] == "confirm" and RECEIVED[-1]["json"]["expiresIn"] is None,
    str(RECEIVED[-1]["json"].get("expiresIn")),
)

# 5. Failures.
print("\nfailures")
REPLY.update({"code": 401, "body": json.dumps({"message": "invalid token"})})
refused = send(live)
check("401 fails the delivery", refused.success is False, str(refused))
check("401 is not worth retrying", refused.retryable is False, str(refused.retryable))
check("the token is not echoed in the error", "tok_abc123" not in (refused.error or ""), str(refused.error))

REPLY.update({"code": 503, "body": ""})
faulted = send(live)
check("503 fails the delivery", faulted.success is False)
check("503 is retryable", faulted.retryable is True, str(faulted.retryable))

REPLY.update({"code": 201, "body": json.dumps({"id": "notif_2"})})
dead = adapter.FamiliarAdapter(config(url="http://127.0.0.1:1", token="tok"))
unreachable = send(dead)
check("an unreachable receiver fails", unreachable.success is False)
check("and is retryable", unreachable.retryable is True, str(unreachable.retryable))

with without_env("FAMILIAR_TOKEN"):
    tokenless = send(adapter.FamiliarAdapter(config(url=URL)))
check("no token refuses before a request is made", tokenless.success is False and "FAMILIAR_TOKEN" in (tokenless.error or ""), str(tokenless.error))

# 5. The standalone sender, for a cron tick with no co-resident gateway.
print("\nstandalone sender (no gateway)")
before = len(RECEIVED)
standalone = asyncio.run(
    adapter._standalone_send(config(url=URL, token="tok_abc123", instance="homelab"), "default", "Headless tick output")
)
check("reports success", standalone.get("success") is True, str(standalone))
check("keeps the id", standalone.get("message_id") == "notif_2", str(standalone))
check("posts the same contract", RECEIVED[-1]["path"] == "/api/hermes/notifications" and RECEIVED[-1]["auth"] == "Bearer tok_abc123")
check("delivers one message", len(RECEIVED) == before + 1)
check("carries no job id, because this signature has no metadata", RECEIVED[-1]["json"]["jobId"] is None, str(RECEIVED[-1]["json"]))
with without_env("FAMILIAR_TOKEN"):
    tokenless_standalone = asyncio.run(adapter._standalone_send(config(url=URL), "default", "x"))
check("refuses without a token too", "error" in tokenless_standalone, str(tokenless_standalone))

# 12. A conversation: which sessions one is, and what it has cost.
#
# Asked the way Familiar asks it - a READ down the channel, answered out of this machine's own store. Not on the
# ingress port: a paired machine has no address, so every read the app makes arrives here as a path.
print("\nconversation (which sessions are one conversation)")

import urllib.parse  # noqa: E402

ROOT_ID = "20261006_101700_chainroot1"
MID_ID = "20261006_101800_chainmid12"
TIP_ID = "20261006_101900_chaintip12"
AFTER_RESET_ID = "20261006_102000_afterreset"
SOLO_ID = "20261006_102100_solocost12"
MSG_PARENT_ID = "20261006_102200_msgparent"
MSG_CHILD_ID = "20261006_102300_msgchild12"


def _seed_conversation() -> None:
    """One conversation in three sessions, and a fourth that is NOT part of it.

    The link means the same conversation only when the PARENT ended by compressing. The fourth session points at
    the tip, whose parent ended by reset - which the instance's own account calls a separate conversation - so it
    must stand on its own.
    """
    db = acquire(Path(get_hermes_home()) / "state.db")
    try:
        db.create_session(ROOT_ID, "familiar", model="deepseek-flash")
        db.end_session(ROOT_ID, "compression")
        db.create_session(MID_ID, "familiar", model="deepseek-flash", parent_session_id=ROOT_ID)
        db.end_session(MID_ID, "compression")
        db.create_session(TIP_ID, "familiar", model="deepseek-flash", parent_session_id=MID_ID)
        db.end_session(TIP_ID, "session_reset")
        db.create_session(AFTER_RESET_ID, "familiar", model="deepseek-flash", parent_session_id=TIP_ID)
        db.create_session(SOLO_ID, "familiar", model="deepseek-flash")

        # A pair with something said in each half: the parent's own messages must be its own, and the child's
        # the child's. Messages go in BEFORE the parent is closed - the store refuses to write to a session that
        # compression ended, which is the guard that makes this pair the honest case to check.
        db.create_session(MSG_PARENT_ID, "familiar", model="deepseek-flash")
        db.append_message(MSG_PARENT_ID, "user", "the first thing said in this conversation")
        db.append_message(MSG_PARENT_ID, "assistant", "and the answer to it")
        db.end_session(MSG_PARENT_ID, "compression")
        db.create_session(MSG_CHILD_ID, "familiar", model="deepseek-flash", parent_session_id=MSG_PARENT_ID)
        db.append_message(MSG_CHILD_ID, "user", "and the thing said after it was renewed")

        usage = {
            ROOT_ID: (5, 100, 0.25),
            MID_ID: (7, 200, 0.5),
            TIP_ID: (1, 50, None),
            AFTER_RESET_ID: (2, 10, 0.02),
            SOLO_ID: (3, 30, None),
        }

        for session_id, (messages, tokens, cost) in usage.items():
            db._conn.execute(
                "UPDATE sessions SET message_count = ?, input_tokens = ?, estimated_cost_usd = ? WHERE id = ?",
                (messages, tokens, cost, session_id),
            )

        db._conn.commit()
    finally:
        release_or_close(db)


_seed_conversation()


def _read(path: str) -> tuple:
    """One read down the channel, answered by the machine's own route table: status and body."""
    answer = asyncio.run(live._channel_api({"method": "GET", "path": path}))

    return answer.get("status"), (answer.get("body") or {})


def _conversations(ids: str) -> tuple:
    status, body = _read(f"/familiar/conversation?ids={urllib.parse.quote(ids)}")

    return status, body.get("conversations") or {}


_from_mid = _conversations(MID_ID)[1][MID_ID]
check(
    "a session in the middle of a chain answers with the whole conversation, oldest first",
    [row["id"] for row in _from_mid["sessions"]] == [ROOT_ID, MID_ID, TIP_ID],
    str([row["id"] for row in _from_mid["sessions"]]),
)
check(
    "and the row that ends the chain is the one the conversation carried on into",
    [row["end_reason"] for row in _from_mid["sessions"]] == ["compression", "compression", "session_reset"],
    str([row["end_reason"] for row in _from_mid["sessions"]]),
)
check(
    "its total sums what each session SPENT, and counts the transcript only where it is",
    # 100 + 200 + 50 tokens are spent once each and add up. The messages do NOT: a compaction carries the
    # transcript into the child, so the counts of it belong to the session holding it - the newest.
    _from_mid["total"]["sessions"] == 3
    and _from_mid["total"]["input_tokens"] == 350
    and _from_mid["total"]["message_count"] == 1,
    str(_from_mid["total"]),
)
check(
    "and the cost is what the sessions reported, with a session that reported none counted as none",
    abs(_from_mid["total"]["estimated_cost_usd"] - 0.75) < 1e-9,
    str(_from_mid["total"]["estimated_cost_usd"]),
)

check(
    "every session of the conversation says when it was last used, the way the reads beside this one do",
    all(row.get("last_active") for row in _from_mid["sessions"]),
    str([row.get("last_active") for row in _from_mid["sessions"]]),
)

# A session of a conversation answers with its OWN transcript. Opening an old session is how a reader finds where
# it stopped - which is where they would branch from - and resolving every one of them to the newest session made
# all of them the same read.
def _messages_of(session_id: str) -> list:
    answer = asyncio.run(live._channel_api({
        "method": "GET", "path": f"/api/sessions/{session_id}/messages?limit=50&order=oldest"}))

    return ((answer.get("body") or {}).get("data") or []), answer.get("status")

_parent_messages, _parent_status = _messages_of(MSG_PARENT_ID)
check(
    "the session a conversation was renewed FROM answers with its own messages, not the new one's",
    _parent_status == 200
    and len(_parent_messages) == 2
    and all(str(row.get("session_id")) == MSG_PARENT_ID for row in _parent_messages),
    str([row.get("session_id") for row in _parent_messages]),
)
check(
    "and it ends where that session ended, which is what a reader branches from",
    "the answer to it" in str(_parent_messages[-1].get("content")) if _parent_messages else False,
    str(_parent_messages[-1].get("content"))[:60] if _parent_messages else "-",
)

_child_messages, _ = _messages_of(MSG_CHILD_ID)
check(
    "the session it was renewed INTO answers with its own, too",
    len(_child_messages) == 1 and all(str(row.get("session_id")) == MSG_CHILD_ID for row in _child_messages),
    str([row.get("session_id") for row in _child_messages]),
)

_missing, _missing_status = _messages_of("20261006_102500_emptysession")
check(
    "and a session this machine does not have is refused by name",
    _missing_status == 404,
    str(_missing_status),
)

_from_root = _conversations(ROOT_ID)[1][ROOT_ID]
check(
    "asking from the session the conversation STARTED in says the same thing",
    [row["id"] for row in _from_root["sessions"]] == [ROOT_ID, MID_ID, TIP_ID],
    str([row["id"] for row in _from_root["sessions"]]),
)

_after = _conversations(AFTER_RESET_ID)[1][AFTER_RESET_ID]
check(
    "a session whose parent was RESET is its own conversation, not a continuation of the chain",
    [row["id"] for row in _after["sessions"]] == [AFTER_RESET_ID],
    str([row["id"] for row in _after["sessions"]]),
)
check(
    "and it counts only itself",
    _after["total"]["sessions"] == 1 and _after["total"]["message_count"] == 2,
    str(_after["total"]),
)

_solo = _conversations(SOLO_ID)[1][SOLO_ID]
check(
    "a conversation where nobody reported a cost reports NO cost, because a total nobody gave is not zero",
    _solo["total"]["estimated_cost_usd"] is None,
    str(_solo["total"]),
)

_unknown = _conversations("no-such-session-anywhere")[1]["no-such-session-anywhere"]
check(
    "an unknown session answers with nothing rather than a guess",
    _unknown == {"sessions": [], "total": None},
    str(_unknown),
)

_asked = _conversations(f"{ROOT_ID},{MID_ID},{TIP_ID}")[1]
check(
    "one call answers about several conversations, keyed by what was asked",
    sorted(_asked.keys()) == sorted([ROOT_ID, MID_ID, TIP_ID]),
    str(list(_asked.keys())),
)

_nothing, _ = _read("/familiar/conversation")
check("a call that asks about nothing is refused, and says which", _nothing == 400, str(_nothing))

_too_many, _ = _read(f"/familiar/conversation?ids={','.join(f's{n}' for n in range(adapter.CONVERSATION_MAX_IDS + 1))}")
check(
    "and one that asks about more conversations than a page holds",
    _too_many == 400,
    str(_too_many),
)

_unknown_path, _ = _read("/familiar/not-a-route")
check(
    "a path this machine does not answer is refused by NAME, not with an empty success",
    _unknown_path == 501,
    str(_unknown_path),
)

print(f"\n{len(PASSED)} passed, {len(FAILED)} failed")
if FAILED:
    print("failed:")
    for label in FAILED:
        print(f"  - {label}")
server.shutdown()
sys.exit(1 if FAILED else 0)
