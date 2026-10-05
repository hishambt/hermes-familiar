"""Check this plugin against the Hermes it is installed into: registration, enablement, and the POST it makes.

Runs against a stub HTTP receiver on loopback, so the assertions are about what actually goes on the wire -
path, bearer header, payload - and how each failure came back. It imports `gateway.*` and `cron.*`, which
live in the Hermes install, so run it with that install's interpreter and from that directory:

    cd "$LOCALAPPDATA/hermes/hermes-agent"
    ./venv/Scripts/python.exe "C:/Work/Personal/Familiar/hermes-familiar/check.py"

Nothing here touches the real ~/.hermes: HERMES_HOME is pointed at a temp directory first.
"""

import asyncio
import contextlib
import json
import os
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

PLUGIN_DIR = str(Path(__file__).resolve().parent)

# A temp home, so nothing here reads or writes the real ~/.hermes.
os.environ["HERMES_HOME"] = tempfile.mkdtemp(prefix="familiar-notify-test-")
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
    check("not connected without one, in extra or the env", not adapter.is_connected(config(url=URL)))
saved_token = os.environ.pop("FAMILIAR_TOKEN")
check("no token in the environment means no seed, so the platform stays disabled", adapter._env_enablement() is None)
os.environ["FAMILIAR_TOKEN"] = saved_token

live = adapter.FamiliarAdapter(config(url=URL, token="tok_abc123"))
check("connect() reports ready", asyncio.run(live.connect()) is True)
check("home target defaults to 'default'", live._home == "default")
with without_env("FAMILIAR_TOKEN"):
    check("connect() refuses without a token", asyncio.run(adapter.FamiliarAdapter(config(url=URL)).connect()) is False)

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
check("and the choices the reader picks from", clarify["json"]["choices"] == ["postgres", "sqlite"], str(clarify["json"].get("choices")))
check("under the prefix every adapter shares", clarify["json"]["callbackPrefix"] == "cl", str(clarify["json"].get("callbackPrefix")))

asyncio.run(
    live.send_exec_approval(
        "default", "rm -rf /tmp/probe", session_key=SESSION_KEY, description="Deletes a directory", request_id="appr_1"
    )
)
approval = RECEIVED[-1]
check("an approval is its own kind", approval["json"]["kind"] == "approval", str(approval["json"].get("kind")))
check(
    "what would run and why it is asked are not the same thing",
    approval["json"]["command"] == "rm -rf /tmp/probe" and approval["json"]["description"] == "Deletes a directory",
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

print(f"\n{len(PASSED)} passed, {len(FAILED)} failed")
if FAILED:
    print("failed:")
    for label in FAILED:
        print(f"  - {label}")
server.shutdown()
sys.exit(1 if FAILED else 0)
