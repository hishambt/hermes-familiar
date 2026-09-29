"""Check this plugin against the Hermes it is installed into: registration, enablement, and the POST it makes.

Runs against a stub HTTP receiver on loopback, so the assertions are about what actually goes on the wire -
path, bearer header, payload - and how each failure came back. It imports `gateway.*` and `cron.*`, which
live in the Hermes install, so run it with that install's interpreter and from that directory:

    cd "$LOCALAPPDATA/hermes/hermes-agent"
    ./venv/Scripts/python.exe "C:/Work/Personal/Familiar/hermes-familiar-notify/check.py"

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
result = send(live, metadata={"job_id": "02c072f93038"})
check("reports success", result.success is True, str(result.error))
check("keeps the id Familiar returned", result.message_id == "notif_1", str(result.message_id))
check("posts to the documented path", RECEIVED[-1]["path"] == "/api/hermes/notifications", RECEIVED[-1]["path"])
check("sends the bearer token", RECEIVED[-1]["auth"] == "Bearer tok_abc123", str(RECEIVED[-1]["auth"]))
payload = RECEIVED[-1]["json"]
check("instance", payload["instance"] == "homelab", str(payload))
check("target", payload["target"] == "default", str(payload))
check("content", payload["content"] == "Job output here", str(payload))
check("jobId rides from the route metadata", payload["jobId"] == "02c072f93038", str(payload))
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

# 4. Failures.
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
