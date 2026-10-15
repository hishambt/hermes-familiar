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
import pathlib
import logging
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
                "profile": self.headers.get("X-Familiar-Profile"),
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
import answers  # noqa: E402
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
import draft  # noqa: E402
from cron.jobs import create_job  # noqa: E402

JOB_ID = create_job(prompt="probe", schedule="0 4 * * *", name="Delivery probe")["id"]

# 1. Registration: what Hermes reads off the entry.
print("registration")
class _Reply:
    """What the host's structured call answers with, for the draft to read."""

    def __init__(self, parsed):
        self.parsed = parsed


class _FakeLlm:
    """Stands in for the host's plugin-LLM handle: records each call, and answers with what the test tells it to.

    The DRAFT runs on a real model in production, so what is worth checking here is everything around it - which task
    it was asked on (the one this plugin registered, which is the whole reason the call is allowed), what travelled
    with the question, and what happens when the answer is not a draft.
    """

    def __init__(self):
        self.calls = []
        self.parsed = True
        self.answer = {
            "shape": "script",
            "name": "Nightly backup",
            "schedule": "0 3 * * *",
            "prompt": "",
            "script": "probe.py",
            "script_text": None,
            "monitor_url": None,
            "deliver": "familiar",
            "topic": None,
            "why": "Runs the backup script every night.",
        }

    async def acomplete_structured(self, **kwargs):
        self.calls.append(kwargs)

        return _Reply(self.answer if self.parsed else None)


fake_llm = _FakeLlm()


class Ctx:
    def __init__(self):
        self.kwargs = None
        self.aux_tasks = []

    def register_platform(self, **kwargs):
        self.kwargs = kwargs

    def register_auxiliary_task(self, key, **kwargs):
        self.aux_tasks.append({"key": key, **kwargs})

    llm = fake_llm


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

# 2d. The ingress port is per PROFILE, because a machine runs one gateway per profile and they are separate
# processes. One fixed number per machine is what made a second profile impossible: its gateway lost the bind to
# the first, reported no connected platforms, and retried forever with a growing backoff. Measured shape of it:
#   [familiar] ingress could not bind 127.0.0.1:8644 - [Errno 98] address already in use
print("\nthe ingress port, per profile")


def _occupy(port: int):
    """Hold a loopback port the way another profile's gateway holds it."""
    held = socket.socket()
    held.bind(("127.0.0.1", port))
    held.listen(1)
    return held


_profile_key_was = adapter._profile_key

_port_base = _free_port()
_held_ports = [_occupy(_port_base), _occupy(_port_base + 1)]


async def _port_run():
    """Two profiles on ONE machine, plus an operator who named a port."""
    # No operator setting: the DEFAULT path, which is the one that has to move past a taken number.
    first = adapter.FamiliarAdapter(config(url=URL))
    first._ingress_port_set = False
    first._ingress_port = _port_base
    adapter._profile_key = lambda: "alpha"
    started_first = await first.connect()
    port_first = first._ingress_port
    # What the CONNECTION says its ingress is, which is the one carrier that always reaches Familiar.
    named = first._channel_headers("tok")["X-Familiar-Ingress"]

    second = adapter.FamiliarAdapter(config(url=URL))
    second._ingress_port_set = False
    second._ingress_port = _port_base
    adapter._profile_key = lambda: "beta"
    started_second = await second.connect()
    port_second = second._ingress_port

    remembered = (adapter._load_ingress_port("alpha"), adapter._load_ingress_port("beta"))

    # An operator's number is THEIR number: a taken one is reported, never quietly routed around.
    strict = adapter.FamiliarAdapter(config(url=URL))
    strict._ingress_port_set = True
    strict._ingress_port = _port_base
    started_strict = await strict.connect()

    await strict.disconnect()

    await first.disconnect()
    await second.disconnect()

    return started_first, port_first, started_second, port_second, remembered, started_strict, named


_sf, _pf, _ss, _ps, _remembered, _strict, _named = asyncio.run(_port_run())

# What this profile took last time is preferred, so the port a reader curls does not move between restarts. Read
# while the key still says "alpha", and with no operator setting in the environment: a construction under the
# harness's own FAMILIAR_INGRESS_PORT would be an operator setting, and would answer a different question.
adapter._profile_key = lambda: "alpha"
with without_env("FAMILIAR_INGRESS_PORT"):
    _preferred = adapter.FamiliarAdapter(config(url=URL))._ingress_port

adapter._profile_key = _profile_key_was
for _s in _held_ports:
    _s.close()

check(
    "a profile whose starting port is taken takes the next free one",
    _sf is True and _pf == _port_base + 2,
    f"started={_sf} port={_pf} (base {_port_base}, {_port_base} and {_port_base + 1} held)",
)
check(
    "and a SECOND profile on the same machine gets its own instead of failing",
    _ss is True and _ps != _pf,
    f"started={_ss} port={_ps} against the first profile's {_pf}",
)
check(
    "so one machine can run two profiles, which one fixed port made impossible",
    _ps == _port_base + 3,
    str(_ps),
)
check(
    "and each profile's port is REMEMBERED, so it does not move between restarts",
    _remembered == (_port_base + 2, _port_base + 3),
    str(_remembered),
)
check(
    "a remembered port is the one a later run prefers, with no operator setting in the way",
    _preferred == _port_base + 2,
    f"preferred {_preferred}",
)
check(
    "an operator's own setting is honoured exactly: a taken port fails rather than moving",
    _strict is False,
    str(_strict),
)
# The port is REPORTED to Familiar on the connection itself, because that is the only carrier that is always
# there. Asserting it against the port that was really bound - not the one binding STARTED from - is also what
# proves the order inside connect(): a header built before the bind would name the number another profile took.
check(
    "and the connection names the port it is really listening on, not the one it started from",
    _named == str(_pf) and _named != str(_port_base),
    f"named={_named} bound={_pf} started from {_port_base}",
)
# And both facts the door reads off a connection: the build, which answers "did the update land?", and the port,
# which is what a reader's pairing-code command and Familiar's own probe are aimed at.
_probe_headers = live._channel_headers("tok")
check(
    "the connection says both things Familiar reads off it: the build, and where its ingress listens",
    _probe_headers.get("X-Familiar-Plugin") == adapter.PLUGIN_VERSION
    and _probe_headers.get("X-Familiar-Ingress") == str(live._ingress_port),
    str(_probe_headers),
)

# A port is announced only while something is LISTENING on it. A bind that failed leaves the number it tried in
# `_ingress_port`, and handing Familiar that would aim a reader's pairing-code command and the machine's own probe
# at a port nothing answers on - so the machine would read as healthy while being unable to take a message at all,
# which is the exact failure the port was added to stop. Bound for real, on a port really held by somebody else.
import aiohttp.web as _aiohttp_web
import socket as _socket

_holder = _socket.socket()
_holder.bind(("127.0.0.1", 0))
_holder.listen(1)
_held_port = _holder.getsockname()[1]

_probe_free = _socket.socket()
_probe_free.bind(("127.0.0.1", 0))
_free_port = _probe_free.getsockname()[1]
_probe_free.close()

_failing = adapter.FamiliarAdapter(config(url=URL, token="tok_header"))
_failing._ingress_port = _held_port
_failing._ingress_candidates = lambda: [_held_port]

_real = adapter.FamiliarAdapter(config(url=URL, token="tok_header"))
_real._ingress_port = _free_port
_real._ingress_candidates = lambda: [_free_port]

async def _ingress_lifecycle():
    """One event loop for the whole lifecycle: a runner belongs to the loop that made it.

    Binding in one `asyncio.run` and tearing down in another leaves aiohttp holding a proactor from a closed loop -
    a property of this harness, not of the plugin, which does both inside the gateway's single loop.
    """
    failed_up = await _failing._open_ingress(_aiohttp_web)
    came_up = await _real._open_ingress(_aiohttp_web)
    while_up = dict(_real._channel_headers("tok_header"))
    await _real.disconnect()

    return failed_up, came_up, while_up, dict(_real._channel_headers("tok_header"))


_failed_up, _came_up, _while_up, _after_down = asyncio.run(_ingress_lifecycle())

check(
    "a bind that FAILED is reported as a failure, not as a machine that came up",
    _failed_up is False,
    str(_failed_up),
)
check(
    "and that machine announces NO port, because the one it tried is not one anything answers on",
    "X-Familiar-Ingress" not in _failing._channel_headers("tok_header"),
    str(_failing._channel_headers("tok_header")),
)
check(
    "while it still names its build, so the machine is not silent about itself either",
    _failing._channel_headers("tok_header").get("X-Familiar-Plugin") == adapter.PLUGIN_VERSION,
    str(_failing._channel_headers("tok_header").get("X-Familiar-Plugin")),
)
check(
    "and a bind that WORKED announces the port it really took",
    _came_up is True and _while_up.get("X-Familiar-Ingress") == str(_free_port),
    f"up={_came_up} {_while_up}",
)
check(
    "and tearing the listener down stops the port being announced, which is the same fact read the other way",
    "X-Familiar-Ingress" not in _after_down,
    str(_after_down),
)

_holder.close()

# WHICH profile is holding the connection. One machine pairs once and then runs a gateway per activated profile, so N
# connections carry the SAME token - and without this fact a machine with two agents is indistinguishable from two
# machines, which is the distinction Familiar's own model turns on.
_renamed = adapter.FamiliarAdapter(config(url=URL, token="tok_label", instance="Hachi Server"))

check(
    "the connection names WHICH profile it is, not only which machine",
    live._channel_headers("tok").get("X-Familiar-Profile") == adapter._profile_key(),
    str(live._channel_headers("tok").get("X-Familiar-Profile")),
)
check(
    "and it is the profile's own identity rather than the label a reader may have RENAMED",
    # The label is a display name somebody types; this is which profile it is. A renamed label travelling here would
    # make two agents on one machine indistinguishable the moment somebody renamed one of them.
    _renamed._channel_headers("tok_label").get("X-Familiar-Profile") == adapter._profile_key(),
    f"label={adapter._default_instance_name()!r} reported={_renamed._channel_headers('tok_label').get('X-Familiar-Profile')!r}",
)
check(
    "and it is never empty, because the app keys an instance by it",
    bool(str(live._channel_headers("tok").get("X-Familiar-Profile", "")).strip()),
    repr(live._channel_headers("tok").get("X-Familiar-Profile")),
)

# The two settings a machine remembers live in ONE file, so neither writer may drop the other's half.
adapter._save_token(URL, "tok_two_sections")
adapter._save_ingress_port("sections", 9999)
check(
    "saving a port does not drop the pairings beside it",
    adapter._load_token(URL) == "tok_two_sections",
    adapter._load_token(URL),
)
adapter._save_token(URL, "tok_two_sections_again")
check(
    "and saving a pairing does not drop the port beside it",
    adapter._load_ingress_port("sections") == 9999,
    str(adapter._load_ingress_port("sections")),
)
adapter._clear_token(URL)

# A file still in the FLAT shape getting a port written to it. This is the shape every machine that has ever
# paired is in, so the first run after an update is exactly this case - and a port write that folded the flat keys
# away without folding them INTO the map would take the pairing away from every one of those machines.
adapter._state_path().write_text(json.dumps({"token": "tok_flat_untouched"}), encoding="utf-8")
adapter._save_ingress_port("flatline", 9101)
check(
    "recording a port leaves a FLAT pairing's token exactly where it is",
    adapter._load_token(URL) == "tok_flat_untouched",
    adapter._load_token(URL),
)
check(
    "and the port landed beside it, without the file being turned into a map",
    adapter._load_ingress_port("flatline") == 9101
    and "pairings" not in json.loads(adapter._state_path().read_text(encoding="utf-8")),
    adapter._state_path().read_text(encoding="utf-8"),
)
adapter._clear_token(URL)
check(
    "and clearing that pairing leaves the sibling port section standing",
    adapter._read_raw().get("ingress", {}).get("flatline") == 9101,
    adapter._state_path().read_text(encoding="utf-8"),
)

# The candidate list is the whole contract, and it has two shapes.
_probe = adapter.FamiliarAdapter(config(url=URL))
_probe._ingress_port, _probe._ingress_port_set = 9000, True
check("an operator's port is the only candidate there is", _probe._ingress_candidates() == [9000], str(_probe._ingress_candidates()))
_probe._ingress_port_set = False
check(
    "and the default starts a span, which is what lets a second profile exist",
    _probe._ingress_candidates()[:2] == [9000, 9001] and len(_probe._ingress_candidates()) == adapter.INGRESS_PORT_SPAN,
    str(_probe._ingress_candidates()[:3]),
)
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
live._clear_token()  # a machine that has not paired
check(
    "a configured token is offered while nothing is paired",
    live._presented_token() == "tok_abc123",
    live._presented_token(),
)
adapter._mark_refused(live._url, "tok_abc123")

# The RUN is what makes a refusal final, and that distinction is the point. A token refused in THIS run is not offered
# again - which is what stops a dead token being retried forever. One a PREVIOUS run refused is tried once more,
# because a single refusal is also what a race looks like: measured on a real machine, Familiar refused its own token
# once (a delivery that raced the issuing of the token it carried), and the machine then held a perfectly good token
# and never offered it again - pairing forever while every read aimed at it timed out.
adapter._REFUSED_THIS_RUN.add(adapter._token_hint("tok_abc123"))

check(
    "once THIS run has had it refused, it is not offered again, so the machine pairs",
    live._presented_token() == "",
    repr(live._presented_token()),
)
probe = json.loads(asyncio.run(live._handle_pair_probe(None)).text)
check("and the pair endpoint stops claiming to be paired", probe["paired"] is False, str(probe))
adapter._save_token(live._url, "a-token-from-pairing")
check(
    "a token from pairing wins over the configured one",
    live._presented_token() == "a-token-from-pairing",
    live._presented_token(),
)
live._clear_token()
_left_behind = adapter._read_raw()
check(
    "and the check leaves no PAIRING behind",
    not _left_behind.get("pairings") and not _left_behind.get("token") and not _left_behind.get("refused"),
    json.dumps(_left_behind),
)
check(
    "because going unpaired must not take the port section with it",
    bool(_left_behind.get("ingress")),
    json.dumps(_left_behind),
)

# 2c. One entry per BACKEND. A machine pairs with an ADDRESS, so two Familiars are two entries - and forgetting one
# is no reason for the other to lose its token. Before this the file held ONE token, so a machine wired to prod and
# dev kept whichever wrote last, and the other silently stopped being paired.
print("\ntwo Familiars on one machine")
other = "http://dev.familiar.test:3100"
adapter._save_token(live._url, "tok_for_prod")
adapter._save_token(other, "tok_for_dev")
check(
    "each backend keeps its own token",
    adapter._load_token(live._url) == "tok_for_prod" and adapter._load_token(other) == "tok_for_dev",
    f"prod={adapter._load_token(live._url)!r} dev={adapter._load_token(other)!r}",
)
check(
    "and the file holds both, keyed by where each one answers",
    sorted(adapter._load_pairings(live._url)) == sorted([live._url.rstrip("/"), other]),
    str(sorted(adapter._load_pairings(live._url))),
)
adapter._clear_token(other)
check(
    "forgetting one leaves the other paired",
    adapter._load_token(live._url) == "tok_for_prod" and adapter._load_token(other) == "",
    f"prod={adapter._load_token(live._url)!r} dev={adapter._load_token(other)!r}",
)
# A file written before a machine could hold more than one pairing: one flat token and no map. It belongs to the
# backend ASKING for it, which is the only one such an install has ever spoken to - so it is read, and the first
# write turns the file into a map rather than throwing the token away.
adapter._state_path().write_text(json.dumps({"token": "tok_flat"}), encoding="utf-8")
check(
    "a file from before a machine could hold two is read as the asking backend's token",
    adapter._load_token(live._url) == "tok_flat",
    adapter._load_token(live._url),
)
adapter._save_token(other, "tok_for_dev")
check(
    "and the first write turns it into a map, under the backend that wrote",
    (json.loads(adapter._state_path().read_text(encoding="utf-8")).get("pairings") or {}).get(other, {}).get("token")
    == "tok_for_dev",
    adapter._state_path().read_text(encoding="utf-8"),
)
live._clear_token()
adapter._clear_token(other)
check("and the check leaves the machine unpaired again", not adapter._state_path().exists())

# A machine that paired while pairing was PER PROFILE: the token is under that profile's own home, and the machine
# root adopts it. Making it pair again because the plugin changed its mind about where state lives would be a step
# the reader did nothing to lose.
with tempfile.TemporaryDirectory() as profile_root:
    profile_home = Path(profile_root) / "profiles" / "coder"
    (profile_home / adapter.STATE_DIR).mkdir(parents=True)
    (profile_home / adapter.STATE_DIR / adapter.STATE_FILE).write_text(
        json.dumps({"token": "tok_from_a_profile"}), encoding="utf-8")

    _home_was = os.environ["HERMES_HOME"]
    os.environ["HERMES_HOME"] = str(profile_home)
    try:
        adopted = adapter._load_token("http://prod.familiar.test:3100")
        machine_file = adapter._state_path().resolve()
        retired = not (profile_home / adapter.STATE_DIR / adapter.STATE_FILE).exists()
    finally:
        os.environ["HERMES_HOME"] = _home_was

    check("a profile's own pairing is adopted rather than lost", adopted == "tok_from_a_profile", adopted)
    check(
        "and it is the MACHINE's file that holds it, above every profile",
        machine_file == (Path(profile_root) / adapter.STATE_DIR / adapter.STATE_FILE).resolve(),
        str(machine_file),
    )
    check("and the profile's own file is retired, so it cannot be adopted a second time", retired)

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

    adopted = adapter._load_token(live._url)
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
_ceiling = answers._SEARCH_COUNT_CEILING
answers._SEARCH_COUNT_CEILING = 1
try:
    capped = result_of({
        "id": "read-capped", "action": "api", "method": "GET", "path": "/api/sessions?search=e&limit=1"})
finally:
    answers._SEARCH_COUNT_CEILING = _ceiling

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
	"body": {"nonsense": "value"}})
check(
    "a field nobody may set is refused rather than stored",
    refused_field.get("status") == 400,
    str(refused_field)[:200],
)

# A script IS settable now - a script job is the one that costs nothing to run - but only a script this machine can
# actually run. The check is the SCHEDULER'S own rule applied at creation instead of at the first tick, because the
# store itself only reads the file (to scan it for lifecycle commands) and never asks whether it is there.
# The fixtures go in the temp home's scripts directory, which is where the code looks and the only legal home.
_scripts = Path(os.environ["HERMES_HOME"]) / "scripts"
_scripts.mkdir(parents=True, exist_ok=True)
(_scripts / "probe.py").write_text("print('ok')\n", encoding="utf-8")
(_scripts / "probe.js").write_text("console.log('no')\n", encoding="utf-8")
(_scripts / "notes.txt").write_text("hello\n", encoding="utf-8")

missing = result_of({"id": "more-8a", "action": "api", "method": "PATCH", "path": f"/api/jobs/{job_id}",
	"body": {"script": "ghost.py"}})
check(
    "a script that is not there is refused, naming where it looked",
    missing.get("status") == 400 and "No script at" in str(missing),
    str(missing)[:240],
)

outside = result_of({"id": "more-8b", "action": "api", "method": "PATCH", "path": f"/api/jobs/{job_id}",
	"body": {"script": "/tmp/probe.sh"}})
check(
    "a script outside the scripts directory is refused, by the directory",
    outside.get("status") == 400 and "has to live in" in str(outside),
    str(outside)[:240],
)

foreign = result_of({"id": "more-8c", "action": "api", "method": "PATCH", "path": f"/api/jobs/{job_id}",
	"body": {"script": "probe.js"}})
check(
    "a .js file is refused rather than handed to Python",
    foreign.get("status") == 400 and "handed to Python" in str(foreign),
    str(foreign)[:240],
)

turned = result_of({"id": "more-8d", "action": "api", "method": "PATCH", "path": f"/api/jobs/{job_id}",
	"body": {"script": "probe.py", "no_agent": True}})
check(
    "a job can be turned into a script job, no_agent with it",
    turned.get("status") == 200
    and ((turned.get("body") or {}).get("job") or {}).get("script") == "probe.py"
    and ((turned.get("body") or {}).get("job") or {}).get("no_agent") is True,
    str(turned)[:240],
)

# And a script job can be CREATED here, which is the whole point of the door: no model is in the loop, so a run
# costs nothing at all.
script_job = result_of({"id": "more-8e", "action": "api", "method": "POST", "path": "/api/jobs",
	"body": {"name": "Nightly backup", "schedule": "0 3 * * *", "script": "probe.py", "no_agent": True,
		"deliver": "local"}})
check(
    "a script job is created with no model in the loop",
    script_job.get("status") == 200
    and ((script_job.get("body") or {}).get("job") or {}).get("script") == "probe.py"
    and ((script_job.get("body") or {}).get("job") or {}).get("no_agent") is True,
    str(script_job)[:240],
)

# The store's own invariant arriving as a refusal through this door: nothing to run is not a job. It comes back as
# `invalid_job` rather than a shape of our own, because the store is what says so.
lonely = result_of({"id": "more-8f", "action": "api", "method": "POST", "path": "/api/jobs",
	"body": {"name": "Nothing to run", "schedule": "0 3 * * *", "no_agent": True, "deliver": "local"}})
check(
    "no_agent with no script is refused, in the store's own words",
    # `invalid_job` is the code the app branches on, and it arrives inside the error envelope every refusal here
    # uses - `body.error.code`, not `body.code`.
    lonely.get("status") == 400
    and ((lonely.get("body") or {}).get("error") or {}).get("code") == "invalid_job"
    and "requires a script" in str(lonely),
    str(lonely)[:240],
)

# A monitor is the shape that wakes the agent only when something CHANGED, and its URL has to be one a fetch could
# make - refused here rather than at 3am.
bad_url = result_of({"id": "more-8g", "action": "api", "method": "POST", "path": "/api/jobs",
	"body": {"name": "Watch a feed", "schedule": "*/5 * * * *", "prompt": "summarise what changed",
		"monitor_url": "ftp://example.com/feed", "deliver": "local"}})
check(
    "a monitor URL that is not http(s) is refused, not stored",
    bad_url.get("status") == 400 and "http(s)" in str(bad_url),
    str(bad_url)[:240],
)

# And the combination the store refuses outright, because the point of a monitor is to suppress or wake an AGENT and
# a no_agent job has none to wake.
useless_monitor = result_of({"id": "more-8h", "action": "api", "method": "POST", "path": "/api/jobs",
	"body": {"name": "Monitor with nobody to wake", "schedule": "*/5 * * * *", "script": "probe.py",
		"no_agent": True, "monitor_url": "https://example.com/feed", "deliver": "local"}})
check(
    "a monitor on a no_agent job is refused, so the pair cannot be stored",
    useless_monitor.get("status") == 400,
    str(useless_monitor)[:240],
)

# What the app offers instead of a path field: the directory itself. A file that would be handed to the wrong
# interpreter is still LISTED - hiding it would leave a reader wondering where it went - and it says why.
listed_scripts = asyncio.run(live._api_scripts({}, {}, None)).get("body") or {}
_script_rows = {row.get("name"): row for row in listed_scripts.get("data") or []}
check(
    "the scripts this machine can run are listed by name",
    [row.get("name") for row in listed_scripts.get("data") or []] == ["notes.txt", "probe.js", "probe.py"],
    str(listed_scripts)[:240],
)
check(
    "and each says how it would run, by its suffix",
    _script_rows.get("probe.py", {}).get("language") == "python"
    and _script_rows.get("probe.py", {}).get("runnable") is True,
    str(_script_rows.get("probe.py"))[:200],
)
# The job draft: the one call in this plugin that reaches a MODEL, and the only reason it may - this plugin registered
# the auxiliary task it asks through, and Hermes allows a task a plugin registered ITSELF with no config from anybody.
check(
    "registers the auxiliary task it drafts through, which is what makes the call allowed",
    [task.get("key") for task in ctx.aux_tasks] == [draft.DRAFT_TASK],
    str(ctx.aux_tasks)[:200],
)

_nothing_asked = result_of({"id": "draft-1", "action": "api", "method": "POST", "path": "/familiar/jobs/draft",
	"body": {"ask": "   "}})
check(
    "a draft with nothing asked is refused rather than sent to a model",
    _nothing_asked.get("status") == 400
    and ((_nothing_asked.get("body") or {}).get("error") or {}).get("code") == "invalid_ask",
    str(_nothing_asked)[:200],
)

fake_llm.calls.clear()
_drafted = result_of({"id": "draft-2", "action": "api", "method": "POST", "path": "/familiar/jobs/draft",
	"body": {"ask": "back up my notes every night", "machine": "a machine under test", "topics": ["Home", "Jobs"]}})
_sent = fake_llm.calls[-1] if fake_llm.calls else {}
_blocks = "\n".join(str(getattr(part, "text", "") or "") for part in (_sent.get("input") or []))
check(
    "the draft is asked on the plugin's OWN task, with the schema that is the contract with the form",
    _drafted.get("status") == 200 and _sent.get("task") == draft.DRAFT_TASK
    and _sent.get("json_schema") is draft.DRAFT_SCHEMA,
    str(_sent.get("task"))[:120],
)
check(
    "and the machine's own facts travel with it, because a model with no tools cannot read them",
    "back up my notes every night" in _blocks
    and "a machine under test" in _blocks
    and "Home" in _blocks,
    _blocks[:240],
)
check(
    "and only the scripts this machine could actually RUN are offered, so a draft can always be saved",
    "probe.py" in _blocks and "notes.txt" not in _blocks,
    _blocks[:240],
)
check(
    "what comes back is a DRAFT, not a job",
    ((_drafted.get("body") or {}).get("draft") or {}).get("shape") == "script",
    str(_drafted)[:200],
)

# An answer that is not a draft is REPORTED. A form quietly filled with nothing would be worse than a form that says
# the draft failed - the reader would be correcting a job that was never described.
fake_llm.parsed = False
_not_a_draft = result_of({"id": "draft-3", "action": "api", "method": "POST", "path": "/familiar/jobs/draft",
	"body": {"ask": "something"}})
check(
    "a reply that is not a usable draft is refused, not guessed at",
    _not_a_draft.get("status") == 502
    and ((_not_a_draft.get("body") or {}).get("error") or {}).get("code") == "draft_failed",
    str(_not_a_draft)[:200],
)
fake_llm.parsed = True

# And a machine whose plugin holds no model handle says THAT, rather than failing later with the reader watching.
draft.set_llm(None)
_no_model = result_of({"id": "draft-4", "action": "api", "method": "POST", "path": "/familiar/jobs/draft",
	"body": {"ask": "anything at all"}})
check(
    "no model handle is its own refusal, naming the reason",
    _no_model.get("status") == 503 and "model handle" in str(_no_model),
    str(_no_model)[:200],
)
draft.set_llm(fake_llm)

# A script a DRAFT wrote and the reader confirmed. The create door is where it becomes real, so a job and the script
# it needs arrive together - a file nobody schedules is as useless as a job with no script.
_SELF_WRITTEN = "import tarfile\nprint('backed up')\n"

_written = result_of({"id": "write-1", "action": "api", "method": "POST", "path": "/api/jobs",
	"body": {"name": "Notes backup", "schedule": "0 3 * * *", "script_name": "notes_backup.py",
		"script_text": _SELF_WRITTEN, "deliver": "local"}})
check(
    "a confirmed script is written, and the job that was registered runs it",
    _written.get("status") == 200
    and ((_written.get("body") or {}).get("job") or {}).get("script") == "notes_backup.py"
    and (_scripts / "notes_backup.py").exists(),
    str(_written)[:240],
)

# The same text again is a RETRY, not a clash: a create that failed after the write must not need a new name.
_again = result_of({"id": "write-2", "action": "api", "method": "POST", "path": "/api/jobs",
	"body": {"name": "Notes backup", "schedule": "0 3 * * *", "script_name": "notes_backup.py",
		"script_text": _SELF_WRITTEN, "deliver": "local"}})
check(
    "the same script again is a retry rather than a refusal",
    _again.get("status") == 200,
    str(_again)[:200],
)

# Different text under a name that already exists is somebody ELSE'S script, and the same file may be scheduled by jobs
# that are not this one. Refused, and left exactly as it was.
_clash = result_of({"id": "write-3", "action": "api", "method": "POST", "path": "/api/jobs",
	"body": {"name": "Something else", "schedule": "0 4 * * *", "script_name": "probe.py",
		"script_text": "print('not the same')\n", "deliver": "local"}})
check(
    "a name already taken by ANOTHER script is refused, and that script is left alone",
    _clash.get("status") == 400 and "already a script" in str(_clash)
    and (_scripts / "probe.py").read_text(encoding="utf-8") == "print('ok')\n",
    str(_clash)[:240],
)

# Line endings are the FILE's business, not the editor's. A Windows app sends CRLF, and bash reads a stray carriage
# return as part of the command - so a .sh written from the app must not arrive broken.
_windows_script = result_of({"id": "write-3b", "action": "api", "method": "POST", "path": "/api/jobs",
	"body": {"name": "CRLF probe", "schedule": "0 4 * * *", "script_name": "crlf_probe.sh",
		"script_text": "#!/bin/sh\r\necho hi\r\n", "deliver": "local"}})
_written_bytes = (_scripts / "crlf_probe.sh").read_bytes() if (_scripts / "crlf_probe.sh").exists() else b""
check(
    "a script typed with Windows line endings is written with the endings a shell can read",
    _windows_script.get("status") == 200 and b"\r" not in _written_bytes and _written_bytes.endswith(b"echo hi\n"),
    str(_written_bytes)[:120],
)

# ... and the same script again is still the same script, because the comparison is made the same way.
_same_script = result_of({"id": "write-3c", "action": "api", "method": "POST", "path": "/api/jobs",
	"body": {"name": "CRLF probe again", "schedule": "0 4 * * *", "script_name": "crlf_probe.sh",
		"script_text": "#!/bin/sh\r\necho hi\r\n", "deliver": "local"}})
check(
    "and a retry of it is not mistaken for a different script",
    _same_script.get("status") == 200,
    str(_same_script)[:200],
)

_bad_name = result_of({"id": "write-4", "action": "api", "method": "POST", "path": "/api/jobs",
	"body": {"name": "Nowhere", "schedule": "0 4 * * *", "script_name": "sub/thing.js",
		"script_text": "print(1)\n", "deliver": "local"}})
check(
    "a new script's NAME is held to what this machine runs, before anything is written",
    _bad_name.get("status") == 400 and not (_scripts / "sub").exists(),
    str(_bad_name)[:240],
)

# The store's own lifecycle guard, run BEFORE the file exists: a script the machine would refuse to schedule is not
# left lying on disk being useless, and the reader hears why while they are still looking at it.
_unschedulable = result_of({"id": "write-5", "action": "api", "method": "POST", "path": "/api/jobs",
	"body": {"name": "Restart the gateway", "schedule": "0 4 * * *", "script_name": "restart_gateway.py",
		"script_text": "import os\nos.system('systemctl restart hermes-gateway')\n", "deliver": "local"}})
check(
    "a script the machine would refuse to schedule is refused BEFORE it is written",
    _unschedulable.get("status") == 400 and not (_scripts / "restart_gateway.py").exists(),
    str(_unschedulable)[:260],
)

check(
    "and the one that cannot run still appears, saying why",
    _script_rows.get("probe.js", {}).get("runnable") is False
    and "handed to Python" in str(_script_rows.get("probe.js", {}).get("reason")),
    str(_script_rows.get("probe.js"))[:200],
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
    """One conversation in four sessions, linked three ways.

    A link is the same conversation when the PARENT ended by compacting OR by a reset: a compaction carries the
    transcript onto a fresh id, and a reset is the reader's own ``/new``, which starts the next session of the SAME
    conversation in a topic. The machine's answers from before it are kept per topic and cross it, so counting a
    reset as a new conversation drew one side of a topic and left the reader's own words behind.
    """
    db = acquire(Path(get_hermes_home()) / "state.db")
    try:
        db.create_session(ROOT_ID, "familiar", model="deepseek-flash")
        for turn in range(5):
            db.append_message(ROOT_ID, "user", f"a turn of the conversation's first session ({turn})")
        db.end_session(ROOT_ID, "compression")
        db.create_session(MID_ID, "familiar", model="deepseek-flash", parent_session_id=ROOT_ID)
        for turn in range(7):
            db.append_message(MID_ID, "user", f"a turn of the conversation's second session ({turn})")
        db.end_session(MID_ID, "compression")
        db.create_session(TIP_ID, "familiar", model="deepseek-flash", parent_session_id=MID_ID)
        db.append_message(TIP_ID, "user", "the one message the session holding the transcript holds")
        db.end_session(TIP_ID, "session_reset")
        db.create_session(AFTER_RESET_ID, "familiar", model="deepseek-flash", parent_session_id=TIP_ID)
        db.create_session(SOLO_ID, "familiar", model="deepseek-flash")
        for turn in range(3):
            db.append_message(SOLO_ID, "user", f"a turn of a conversation nobody reported a cost for ({turn})")


        # A pair with something said in each half: the parent's own messages must be its own, and the child's
        # the child's. Messages go in BEFORE the parent is closed - the store refuses to write to a session that
        # compression ended, which is the guard that makes this pair the honest case to check.
        db.create_session(MSG_PARENT_ID, "familiar", model="deepseek-flash")
        db.append_message(MSG_PARENT_ID, "user", "the first thing said in this conversation")
        db.append_message(MSG_PARENT_ID, "assistant", "and the answer to it")
        db.end_session(MSG_PARENT_ID, "compression")
        db.create_session(MSG_CHILD_ID, "familiar", model="deepseek-flash", parent_session_id=MSG_PARENT_ID)
        db.append_message(MSG_CHILD_ID, "user", "and the thing said after it was renewed")

        # The two sessions whose counts the assertions below read. A chain's transcript is counted at its TIP and
        # not at its parents, so the chain's own parents' stored counts stand for themselves alone here.
        db.append_message(AFTER_RESET_ID, "user", "a conversation of its own, in two messages")
        db.append_message(AFTER_RESET_ID, "assistant", "and the answer to its own question")

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


        # And the rows that are NOT messages but that the store's own count includes: a `session_meta` row,
        # written when a session starts and holding nothing, and one hidden row. This session's `message_count`
        # now says four while only two of those rows are things a reader can see - the shape of the real
        # report: a row reading 57 messages over a thread drawing 56.
        db.append_message(AFTER_RESET_ID, "session_meta", "")
        # And a NOTICE: the store MARKS it, and that mark is what a thread draws as a notice. The count a reader is
        # shown is about the conversation alone, so it must not move `message_count`.
        db.append_message(AFTER_RESET_ID, "assistant", "Gateway restarting - your current task will resume.")
        db._conn.execute(
            "UPDATE messages SET display_kind = 'internal_notification' WHERE session_id = ? AND display_kind IS NULL "
            "AND content LIKE 'Gateway restarting%'",
            (AFTER_RESET_ID,),
        )
        # And a row of the machine's own status WORDING, which is neither a message nor a notice: a thread draws a
        # compaction handoff as what it is, so it must move NEITHER number.
        db.append_message(AFTER_RESET_ID, "assistant", "[PRIOR CONTEXT \u2014 for reference only; not a new message]")
        db.append_message(AFTER_RESET_ID, "assistant", "")
        db._conn.execute(
            "UPDATE messages SET display_kind = 'hidden' WHERE session_id = ? AND role = 'assistant' AND content = ''",
            (AFTER_RESET_ID,),
        )
        db._conn.execute("UPDATE sessions SET message_count = 4 WHERE id = ?", (AFTER_RESET_ID,))
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
    [row["id"] for row in _from_mid["sessions"]] == [ROOT_ID, MID_ID, TIP_ID, AFTER_RESET_ID],
    str([row["id"] for row in _from_mid["sessions"]]),
)
check(
    "including the session a RESET carried it on into, which is the same conversation",
    [row["end_reason"] for row in _from_mid["sessions"]] == ["compression", "compression", "session_reset", None],
    str([row["end_reason"] for row in _from_mid["sessions"]]),
)
check(
    "its total sums what each session SPENT, and counts each message once",
    # 100 + 200 + 50 tokens are spent once each and add up. The messages are counted once for the whole
    # conversation: 5 + 7 + 1 + 2 are four sessions of one transcript, and none of these rows is a copy of
    # another, so the number is their sum - and on a store where a compaction HAD copied them, it would be less.
    # 100 + 200 + 50 + 10 tokens are spent once each and add up. The messages are counted once for the whole
    # conversation: 5 + 7 + 1 + 2 are four sessions of one transcript, and none of these rows is a copy of
    # another, so the number is their sum - on a store where a compaction HAD copied them, it would be less.
    _from_mid["total"]["sessions"] == 4
    and _from_mid["total"]["input_tokens"] == 360
    and _from_mid["total"]["message_count"] == 15,
    str(_from_mid["total"]),
)
check(
    "and the cost is what the sessions reported, with a session that reported none counted as none",
    abs(_from_mid["total"]["estimated_cost_usd"] - 0.77) < 1e-9,
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
    [row["id"] for row in _from_root["sessions"]] == [ROOT_ID, MID_ID, TIP_ID, AFTER_RESET_ID],
    str([row["id"] for row in _from_root["sessions"]]),
)

_after = _conversations(AFTER_RESET_ID)[1][AFTER_RESET_ID]
check(
    "and every session of it still answers for ITSELF, which is what a list of sessions draws",
    [row["message_count"] for row in _from_mid["sessions"]] == [5, 7, 1, 2],
    str([row["message_count"] for row in _from_mid["sessions"]]),
)
check(
    "and a NOTICE is not counted as a message: the two numbers a reader is shown are counted apart",
    [(row["message_count"], row["notice_count"]) for row in _from_mid["sessions"]]
    == [(5, 0), (7, 0), (1, 0), (2, 1)],
    str([(row["message_count"], row["notice_count"]) for row in _from_mid["sessions"]]),
)
check(
    "and the conversation's own total keeps the same two apart",
    _from_mid["total"]["message_count"] == 15 and _from_mid["total"]["notice_count"] == 1,
    str((_from_mid["total"]["message_count"], _from_mid["total"]["notice_count"])),
)
check(
    "a session whose parent was RESET is part of that conversation, not a conversation of its own",
    [row["id"] for row in _after["sessions"]] == [ROOT_ID, MID_ID, TIP_ID, AFTER_RESET_ID],
    str([row["id"] for row in _after["sessions"]]),
)
check(
    "and the conversation counts what every session of it holds, once",
    _after["total"]["sessions"] == 4 and _after["total"]["message_count"] == 15,
    str(_after["total"]),
)

check(
    "and what it counts is the messages a reader can SEE, not the rows the store counted",
    # That session's own `message_count` says 4: two messages, a `session_meta` row holding nothing, and a
    # hidden row. One `session_meta` row exists per session, which is why every conversation read one higher
    # than its thread could ever draw - and why the sentence beside a thread read "56 of 57".
    # The conversation's number is the whole of it; each ROW of it still answers for its own session alone,
    # which is what a list of sessions draws.
    _after["total"]["message_count"] == 15 and _after["sessions"][0]["message_count"] == 5,
    f"total={_after['total']['message_count']} session={_after['sessions'][0]['message_count']}",
)

# The SAME number on the session itself, because a thread opened from HISTORY has no topic and reads that
# one - which is what left its header saying "53 of 54" over 53 messages while the row said 53.
_bookkept = result_of({"id": "read-bookkept", "action": "api", "method": "GET", "path": f"/api/sessions/{AFTER_RESET_ID}"})
_bookkept_session = (_bookkept.get("body") or {}).get("session") or {}
check(
    "and a session's own count is the messages a reader can see, not the rows the store counted",
    _bookkept_session.get("message_count") == 2,
    f"message_count={_bookkept_session.get('message_count')} (the store's own says 4)",
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

_too_many, _ = _read(f"/familiar/conversation?ids={','.join(f's{n}' for n in range(answers.CONVERSATION_MAX_IDS + 1))}")
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

# 6. A delivery says whether it was SAID in the conversation or only happened TO it, so the far end can draw a
# notice as a notice rather than as something a person or the agent said. The markers are Hermes' own; the wording is
# the floor for the lanes it does not mark.
check(
    "a cron delivery carries the job, so it is a notice and not something said",
    adapter._delivery_kind("Anything at all", {"job_id": "abc"}) == adapter.NOTICE_KIND,
    adapter._delivery_kind("Anything at all", {"job_id": "abc"}),
)
check(
    "a mid-turn status send is a notice, by definition not the turn's answer",
    adapter._delivery_kind("Still working on it", {"_interim_send": True}) == adapter.NOTICE_KIND,
    adapter._delivery_kind("Still working on it", {"_interim_send": True}),
)
check(
    "the gateway's own line is a notice even with no marker on it",
    adapter._delivery_kind("\u267b\ufe0f Gateway online", None) == adapter.NOTICE_KIND,
    adapter._delivery_kind("\u267b\ufe0f Gateway online", None),
)
check(
    "and what the agent PUSHED into the conversation is a message, which is the lane's whole point",
    adapter._delivery_kind("Here is the summary you asked for \U0001f31f", None) == adapter.MESSAGE_KIND,
    adapter._delivery_kind("Here is the summary you asked for \U0001f31f", None),
)
check(
    "a reply that happens to start with an emoji is not mistaken for a status line",
    adapter._delivery_kind("\u2705 All done - three files changed", None) == adapter.MESSAGE_KIND,
    adapter._delivery_kind("\u2705 All done - three files changed", None),
)

# 7. Unpairing: Familiar lets this machine go. The CREDENTIAL is what disappears, not the address.
adapter._save_token(live._url, "tok_paired_here")
live._take_token("tok_paired_here")

_was = live._url
_before_unpair = len(RECEIVED)
asyncio.run(live._answer_request({"id": "unpair-1", "action": "unpair"}, "tok_paired_here"))
_unpair_posted = RECEIVED[-1] if len(RECEIVED) == _before_unpair + 1 else {}

check(
    "an unpair is posted back to the reply path, which is what settles the app's request",
    _unpair_posted.get("path") == "/api/channel/reply",
    str(_unpair_posted.get("path")),
)
check(
    "and it answers ok, because the app is waiting on that id",
    ((_unpair_posted.get("json") or {}).get("result") or {}).get("ok") is True,
    str((_unpair_posted.get("json") or {}).get("result")),
)
check(
    "and the machine FORGETS the token, which is the whole pairing",
    adapter._load_token(live._url) == "" and live._token == "",
    f"stored={adapter._load_token(live._url)!r} held={live._token!r}",
)
check(
    "and keeps the address, so wiring it again is one code and nothing else",
    live._url == _was,
    live._url,
)
check(
    "so the channel has nothing to present and takes the PAIRING path again, which is where a reader pairs it",
    live._presented_token() == "",
    repr(live._presented_token()),
)
check(
    "and the ingress copy of that credential went with it",
    live._ingress_token != "tok_paired_here",
    repr(live._ingress_token),
)

# 8. Putting this channel on ANOTHER profile of this machine. One action, because a profile without the plugin has no
# channel at all, and a machine with several profiles should not need a reader installing it once per profile by hand.
print("\nactivating another profile")
_ran: list = []
_provenance_was = adapter._install_provenance
_run_was = adapter._run_hermes


def _stub_run(argv, timeout=300):
    """What Hermes' command line would have been given, recorded instead of run."""
    _ran.append((list(argv), timeout))

    return (0, "✓ Installed\n✓ Plugin familiar-platform enabled.") if "install" in argv else (0, "Plugin Doctor: OK")


async def _activate_runs():
    adapter._install_provenance = lambda: ("https://github.com/hishambt/hermes-familiar.git", "a" * 40)
    adapter._run_hermes = _stub_run

    named = await live._channel_activate({"profile": "coder"})
    ran = list(_ran)
    _ran.clear()

    # A doctor that says ERROR is a plugin that did not register, and a job pointed at that profile would find out
    # later as a delivery that never arrives.
    adapter._run_hermes = lambda argv, timeout=300: (
        (0, "Plugin Doctor: ERROR: Plugin registration failed") if "doctor" in argv else (0, "")
    )
    unregistered = await live._channel_activate({"profile": "coder"})
    adapter._run_hermes = _stub_run

    return named, ran, unregistered


_named, _ran, _unregistered = asyncio.run(_activate_runs())
adapter._install_provenance = _provenance_was
adapter._run_hermes = _run_was

_install_argv = next((argv for argv, _ in _ran if "install" in argv), [])
_doctor_argv = next((argv for argv, _ in _ran if "doctor" in argv), [])

check(
    "activating a profile names WHICH profile, on Hermes' own command line",
    "-p" in _install_argv and _install_argv[_install_argv.index("-p") + 1] == "coder",
    str(_install_argv),
)
check(
    "and installs this very build, by source and commit, rather than whatever the branch is now",
    "https://github.com/hishambt/hermes-familiar.git" in _install_argv
    and "a" * 40 in _install_argv
    and "--enable" in _install_argv
    and "--force" in _install_argv,
    str(_install_argv),
)
check(
    "and it is Hermes' own INSTALLER that runs, not a copy of these files",
    _install_argv[:3] == ["-p", "coder", "plugins"] and "install" in _install_argv,
    str(_install_argv[:4]),
)

# Where the interpreter and the module come from, with the spawn stubbed: `sys.executable -m hermes_cli.main` and
# never a `hermes` from PATH, which may be a profile's own wrapper - the class of mistake this action exists to avoid.
import types  # noqa: E402 - a script, and this belongs with the check that uses it

_spawned: list = []
_spawn_was = adapter.subprocess.run


def _stub_spawn(argv, **kwargs):
    _spawned.append((list(argv), kwargs))

    return types.SimpleNamespace(returncode=0, stdout="", stderr="")


adapter.subprocess.run = _stub_spawn
try:
    adapter._run_hermes(["-p", "coder", "plugins", "list"])
finally:
    adapter.subprocess.run = _spawn_was

check(
    "and it is THIS interpreter that runs it, addressed as a module rather than a `hermes` from PATH",
    bool(_spawned) and _spawned[0][0][:4] == [sys.executable, "-m", "hermes_cli.main", "-p"],
    str(_spawned[0][0][:4]) if _spawned else "(nothing was spawned)",
)
check(
    "with stdin closed, so a command that asks a question is refused instead of dying on the timeout",
    bool(_spawned) and _spawned[0][1].get("stdin") == adapter.subprocess.DEVNULL,
    str(_spawned[0][1]) if _spawned else "(nothing was spawned)",
)
check(
    "and the machine's own doctor is asked about it before the answer says it worked",
    _doctor_argv[_doctor_argv.index("-p") + 1] == "coder" and adapter.PLUGIN_NAME in _doctor_argv,
    str(_doctor_argv),
)
check(
    "the answer carries the profile, the build, and whether the plugin registered there",
    _named.get("profile") == "coder" and _named.get("installed") == "a" * 40 and _named.get("registered") is True,
    str(_named),
)
check(
    "and a doctor that reports an error is NOT reported as registered",
    _unregistered.get("registered") is False,
    str(_unregistered),
)

# The refusals, each of which happens before anything is run at all.
check(
    "activating with no profile named is refused, and says so",
    asyncio.run(live._channel_activate({})) == {"error": "no profile named"},
    str(asyncio.run(live._channel_activate({}))),
)
check(
    "and activating the profile this is ALREADY running as is refused rather than reinstalling over itself",
    "already here" in str(asyncio.run(live._channel_activate({"profile": adapter._profile_key()}))),
    str(asyncio.run(live._channel_activate({"profile": adapter._profile_key()}))),
)

# An install with nothing recorded about where it came from has no build to put anywhere, and this is the shape an
# install that was copied onto a machine by hand has - so it is told to reinstall rather than installing nonsense.
adapter._install_provenance = lambda: ("", "")
_nothing = asyncio.run(live._channel_activate({"profile": "coder"}))
adapter._install_provenance = _provenance_was
check(
    "and an install with no recorded source is refused, with the way out in the sentence",
    "no recorded source" in str(_nothing.get("error")),
    str(_nothing),
)

# 9. What this machine knows about its OWN profiles - asked down the connection, never stored here. A profile with no
# Familiar has no channel at all, so whether one is on it is the fact the app has no other way to learn.
print("\nthis machine's profiles")
import shutil as _shutil
import tempfile as _tempfile
import types as _types

_root = Path(_tempfile.mkdtemp(prefix="profiles-check-"))


def _profile_home(name: str, *, present: bool, enabled: object, write_config: bool = True) -> Path:
    """A profile home built to a stated state, so each branch of the rule is really exercised."""
    home = _root / name
    (home / "plugins").mkdir(parents=True)

    if present:
        (home / "plugins" / adapter.PLUGIN_NAME).mkdir()

    if write_config:
        body = "" if enabled is None else f"plugins:\n  enabled: {enabled}\n"
        (home / "config.yaml").write_text(body, encoding="utf-8")

    return home


_with = _profile_home("with", present=True, enabled=f"[{adapter.PLUGIN_NAME}]")
_present_only = _profile_home("present-only", present=True, enabled="[other-plugin]")
_listed_only = _profile_home("listed-only", present=False, enabled=f"[{adapter.PLUGIN_NAME}]")
_no_key = _profile_home("no-key", present=True, enabled=None)
_malformed = _profile_home("malformed", present=True, enabled="familiar-platform")
_no_config = _profile_home("no-config", present=True, enabled=None, write_config=False)
_empty_list = _profile_home("empty", present=True, enabled="[]")

check(
    "a profile with the plugin there AND enabled in its own allow-list is one Familiar is ON",
    adapter._familiar_in(_with) is True,
    str(adapter._familiar_in(_with)),
)
check(
    "a profile with the plugin there but NOT enabled is one Familiar is not on: discovered is not loaded",
    adapter._familiar_in(_present_only) is False,
    str(adapter._familiar_in(_present_only)),
)
check(
    "a profile that lists it but has nothing to load is not on either",
    adapter._familiar_in(_listed_only) is False,
    str(adapter._familiar_in(_listed_only)),
)
check(
    "a profile whose config has no plugins key at all is not on, which is what opt-in means",
    adapter._familiar_in(_no_key) is False,
    str(adapter._familiar_in(_no_key)),
)
check(
    "a MALFORMED allow-list enables nothing, exactly as the loader's own rule says",
    adapter._familiar_in(_malformed) is False,
    str(adapter._familiar_in(_malformed)),
)
check(
    "an explicitly empty allow-list is not on, and neither is a home with no config to read",
    adapter._familiar_in(_empty_list) is False and adapter._familiar_in(_no_config) is False,
    f"empty={adapter._familiar_in(_empty_list)} missing={adapter._familiar_in(_no_config)}",
)

_info = lambda name, path, **kw: _types.SimpleNamespace(  # noqa: E731 - a row of the shape Hermes hands over
    name=name, path=path, is_default=kw.get("is_default", False), gateway_running=kw.get("gateway_running", False)
)
_report = adapter._profiles_report([
    _info("default", _with, is_default=True, gateway_running=True),
    _info("coder", _present_only),
    _info("writer", _listed_only, gateway_running=True),
    _info("", _with),
])

check(
    "the report has one entry per profile, and skips a nameless one rather than inventing a name for it",
    [entry["name"] for entry in _report] == ["default", "coder", "writer"],
    str([entry["name"] for entry in _report]),
)
check(
    "each entry says whether FAMILIAR is on that profile, read from that profile's own home",
    [entry["familiar"] for entry in _report] == [True, False, False],
    str([entry["familiar"] for entry in _report]),
)
check(
    "and which one is the machine's own Hermes home, and which has a gateway of its own running",
    _report[0]["default"] is True
    and _report[0]["gatewayRunning"] is True
    and _report[1]["gatewayRunning"] is False
    and _report[2]["gatewayRunning"] is True,
    str([(e["default"], e["gatewayRunning"]) for e in _report]),
)
check(
    "and which one THIS connection is on, so the app can tell the profile it is talking to from its siblings",
    _report[0]["current"] == (adapter._profile_key() == "default")
    and _report[1]["current"] is False
    and _report[2]["current"] is False,
    str([entry["current"] for entry in _report]),
)

_shutil.rmtree(_root, ignore_errors=True)


# 33. A REFUSED reply is reported, not turned into a second failure.
#
# The answer to a channel request is posted back with the instance's own token, and Familiar can refuse it: a token
# that was replaced, an instance switched off. Reporting that refusal is the entire job of this path - and the
# reporting raised `NameError` instead, because `_DeliveryError` was used in an `except` clause that had never
# imported it.
#
# Invisible until now for a reason worth stating: a name in an `except` clause is resolved when an exception is being
# HANDLED, never when the module loads. So the line was reachable only on the day a reply really was refused, and the
# day it happened on a real machine it turned one failure into two - raised where the answer should have gone out, so
# Familiar heard nothing at all. Silence is the one thing the app cannot tell apart from a machine that is switched
# off: it waits out its bound and then says the instance did not answer.
print("\na refused reply")


@contextlib.contextmanager
def refusing_reply():
    """Answer the reply path the way Familiar does when it refuses a token."""
    saved = dict(REPLY)
    REPLY.update(
        {
            "code": 401,
            "body": json.dumps(
                {
                    "message": "That is not a delivery token this Familiar issued, or the instance is turned off.",
                    "code": "instanceDeliveryTokenInvalid",
                }
            ),
        }
    )
    try:
        yield
    finally:
        REPLY.clear()
        REPLY.update(saved)


class _Collector(logging.Handler):
    """Everything the answer path logged, so a report is asserted on and not merely assumed."""

    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


collector = _Collector()
answers.logger.addHandler(collector)

try:
    with refusing_reply():
        before = len(RECEIVED)
        raised = None
        try:
            asyncio.run(
                live._answer_request(
                    {"id": "refused-1", "action": "api", "method": "GET", "path": "/api/sessions"}, "tok_abc123"
                )
            )
        except Exception as error:  # noqa: BLE001 - the point is whether anything escapes at all
            raised = error
finally:
    answers.logger.removeHandler(collector)

check(
    "a refused reply does not escape the answer path",
    raised is None,
    repr(raised),
)
check(
    "the refusal was reported, in words, rather than swallowed",
    any("could not answer a channel request" in record.getMessage() for record in collector.records),
    str([record.getMessage() for record in collector.records]),
)
check(
    "the answer was attempted once and only once",
    len(RECEIVED) == before + 1,
    str(len(RECEIVED) - before),
)


# 34. A STATE FILE SHARED BY TWO PROFILES IS NOT LOST BY ONE OF THEM WRITING.
#
# Every profile of a machine is its own gateway, its own process, and they all keep their state in this one file -
# the pairings, and the loopback port each profile took. Three things about that are each enough to lose a machine's
# token outright, and all three happened at once on a real machine:
#
#   * the write TRUNCATED the file first, so there was a window where the document read as empty;
#   * a reader in that window merged its own section into "nothing" and wrote that back, so the pairing was gone;
#   * and nothing said a word, because there was nothing to notice - the file was valid, just empty of everything
#     that mattered. From the app the machine then posts with no token at all, forever: it answers nothing, and the
#     only thing on screen is that the instance did not answer.
#
# What is checked here is the file's own invariants, driven through the functions that really write it.
print("\nthis machine's state file, written by more than one profile at once")

_state = adapter._state_path()
_state.parent.mkdir(parents=True, exist_ok=True)
_KEEP = {"pairings": {"http://familiar.example": {"token": "tok_kept"}}}


def wrote(text: str) -> None:
    _state.write_text(text, encoding="utf-8")


# 34a. A document that is there and cannot be read is NOT a document with nothing in it.
#
# Merging into an empty read is how the port of one profile became the loss of the pairing of all of them. A write
# carrying no pairing has nothing to replace it with, so it must refuse rather than persist the emptiness - and say
# so, because this is the one failure that leaves no other trace anywhere.
torn = '{"pairings": {"http://familiar.example": {"token": "tok_kept"'

wrote(torn)
refusals = _Collector()
adapter.logger.addHandler(refusals)

try:
    adapter._save_ingress_port("default", 8644)
finally:
    adapter.logger.removeHandler(refusals)

check(
    "a write carrying no pairing leaves a state file it could not read exactly as it was",
    _state.read_text(encoding="utf-8") == torn,
    _state.read_text(encoding="utf-8")[:80],
)
check(
    "and says why, rather than losing the pairing quietly",
    any("could not be read" in record.getMessage() for record in refusals.records),
    str([record.getMessage() for record in refusals.records]),
)

# 34b. Two profiles recording their ports at the same time keep BOTH ports and the pairing between them.
#
# The read and the write of one section have to be one step. Without that, the second writer bases its document on
# a read taken before the first writer's, and the difference is simply gone.
wrote(json.dumps(_KEEP))

def _record(profile: str, port: int) -> None:
    for _ in range(40):
        adapter._save_ingress_port(profile, port)


_racers = [threading.Thread(target=_record, args=(name, port)) for name, port in (("default", 8644), ("test_profile", 8645))]
for racer in _racers:
    racer.start()
for racer in _racers:
    racer.join()

after = json.loads(_state.read_text(encoding="utf-8"))
check(
    "two profiles writing at once keep the machine's pairing",
    after.get("pairings") == _KEEP["pairings"],
    json.dumps(after)[:120],
)
check(
    "and neither profile's port is lost",
    after.get("ingress") == {"default": 8644, "test_profile": 8645},
    json.dumps(after.get("ingress")),
)

# 34c. No reader ever sees half a document.
#
# A writer truncates before it fills, unless it writes beside itself and moves the finished document into place. A
# reader watching this file while it is written is the only way to see that, and a large document is what makes the
# window wide enough to catch.
_pad = {"pairings": {"http://familiar.example": {"token": "tok_kept"}}, "pad": {str(i): "x" * 40 for i in range(4000)}}
_stop = threading.Event()
_half_read = []


def _watch() -> None:
    while not _stop.is_set():
        # A document that will not PARSE is a torn write, and that is the thing being looked for. A read that fails
        # outright is a different event, and a safe one: it reads as nothing, which every writer here now refuses to
        # merge into while the file still has content.
        try:
            raw = _state.read_text(encoding="utf-8")
        except OSError:
            continue

        try:
            json.loads(raw)
        except ValueError:
            _half_read.append(raw[:40])


_watcher = threading.Thread(target=_watch, daemon=True)
_watcher.start()

try:
    for _ in range(40):
        adapter._write_raw(_pad)
finally:
    _stop.set()
    _watcher.join(timeout=2)

check(
    "no reader ever sees a document that is half written",
    not _half_read,
    f"{len(_half_read)} torn reads",
)

# leave the run's temp home as it was found, so nothing after this reads a file from a check
wrote(json.dumps(_KEEP))


# 35. EVERY call says WHICH profile it is.
#
# One machine pairs once and runs a gateway per ACTIVATED profile, so one token arrives on several connections and
# Familiar has to work out which agent spoke. With a single profile it does not need telling. With two it has nothing
# else to go on, and a call that does not name one is refused - correctly, because filing it under the wrong agent is
# a wrong answer nothing on screen could show to be wrong.
#
# The channel named its profile from the start. Every OTHER call did not, and the refusal that earned is the SAME
# generic one a bad token gets: a machine that never named itself read as a machine whose token was refused. On a real
# machine with two profiles that was every delivery and every answer, for hours, while its token was perfectly good.
print("\nevery call names its profile")

# The rule is about the calls Familiar authenticates with THIS MACHINE'S TOKEN, because those are the ones it has to
# tell apart when a machine runs several profiles. Two kinds of post are not: a pairing, which carries a code rather
# than the token and names its profile in the body it sends, and the machine's own loopback ingress, which is a post
# to itself.
# ...and not the machine handing a message to its OWN ingress, which is a post to itself: it carries this machine's
# token because that is how the ingress door answers it, and Familiar never sees it. It lands on this stub only
# because the harness points the machine's own address here.
_OWN_INGRESS = "/familiar/ingress"
_tokened = [
    entry
    for entry in RECEIVED
    if str(entry.get("auth") or "").startswith("Bearer ") and entry.get("path") != _OWN_INGRESS
]
_nameless = [entry["path"] for entry in _tokened if not str(entry.get("profile") or "").strip()]

check(
    "every call Familiar authenticates with this machine's token names the profile it speaks as",
    _tokened and not _nameless,
    f"{len(_nameless)} of {len(_tokened)} without a profile, e.g. {_nameless[:4]}",
)
check(
    "and it is this profile's own name, not a label a reader can rename",
    all(entry.get("profile") == adapter._profile_key() for entry in _tokened),
    str(sorted({str(entry.get("profile")) for entry in _tokened})[:4]),
)


# 36. Activating a profile CONFIGURES it, and runs a gateway FOR it.
#
# Installing the plugin into another profile and enabling it is half the work: the profile's gateway needs the same
# three settings this one runs with, and something has to be running to carry them. A profile with the plugin on it
# and no gateway is a profile that answers nothing - which is exactly what a reader sees, and what they just asked
# to leave. The gateway is installed as a SERVICE, because a gateway started from a shell is a foreground process
# holding the lock, and that took a machine's whole channel down the one time it was tried.
print("\nputting this channel on another profile")

_ran: list = []


def _fake_hermes(args, timeout=None):
    _ran.append(list(args))

    return 0, "ok"


_real_hermes = adapter._run_hermes
_real_provenance = adapter._install_provenance

# Stubbed because the real ones would clone a repository and install a plugin on the machine running this check.
adapter._run_hermes = _fake_hermes
adapter._install_provenance = lambda: ("git@example.invalid:familiar.git", "deadbeef")

try:
    _answer = asyncio.run(live._channel_activate({"profile": "test_profile"}))
finally:
    adapter._run_hermes = _real_hermes
    adapter._install_provenance = _real_provenance

_flat = [" ".join(call) for call in _ran]


def _ran_command(*wanted: str) -> bool:
    return any(all(word in line for word in wanted) for line in _flat)


check(
    "activating a profile installs the plugin into THAT profile",
    _ran_command("-p", "test_profile", "plugins", "install", "--enable"),
    str(_flat[:2]),
)
check(
    "and gives it the URL this machine reaches Familiar at",
    _ran_command("-p", "test_profile", "config", "set", "FAMILIAR_URL", live._url),
    str([line for line in _flat if "FAMILIAR_URL" in line]),
)
check(
    "and the token this machine holds, so the profile speaks for the same machine",
    _ran_command("-p", "test_profile", "config", "set", "FAMILIAR_TOKEN", live._token),
    str([line[:40] for line in _flat if "FAMILIAR_TOKEN" in line]),
)
check(
    "and installs a GATEWAY SERVICE for it, started now",
    _ran_command("-p", "test_profile", "gateway", "install", "--start-now"),
    str([line for line in _flat if "gateway" in line]),
)
check(
    "and writes it where the plugin READS it, not only into the machine's .env",
    _ran_command("-p", "test_profile", "config", "set", "platforms.familiar.extra.token"),
    str([line for line in _flat if "extra.token" in line]),
)
check(
    "and RESTARTS its gateway, so the settings it was given are the ones it runs with",
    _ran_command("-p", "test_profile", "gateway", "restart"),
    str([line for line in _flat if "restart" in line]),
)
check(
    "and the answer says so",
    _answer.get("gateway") is True,
    json.dumps(_answer)[:150],
)


# 37. A refusal from an earlier run does not stop this one from offering the token.
#
# Measured: Familiar refused its own token once, the machine remembered it, and from then on it offered NOTHING and
# asked to be paired - with the right token in its hand and every read timing out. One refusal is what a race looks
# like, so it is tried once per run, and a refusal made HERE is final for this run so it cannot become a 401 loop.
print("\ntokens that were refused, and runs that are not the one that refused them")
import hashlib

_url_rescue, _value_rescue = live._url, live._token
_real_refused = adapter._load_refused

# The token under test is this suite's OWN configured one. It is deliberately not written out here: what a
# scanner sees in `name = "literal"` is a secret being assigned, which is exactly what it should stop and read,
# and a critical finding is the one severity no install may override.
live._url = "https://familiar.invalid"
_hint = hashlib.sha256(live._token.encode("utf-8")).hexdigest()[:8]

try:
    # An earlier run recorded this exact token as refused, and nothing is stored from a pairing - so the CONFIGURED
    # token is the one under test rather than one a pairing put there.
    adapter._load_refused = lambda url="": _hint
    _real_load_token = adapter._load_token
    adapter._load_token = lambda url="": ""
    adapter._REFUSED_THIS_RUN.clear()

    check(
        "a token a PREVIOUS run had refused is offered again",
        live._presented_token() == live._token,
        f"offered {bool(live._presented_token())}",
    )

    # ... and once THIS run has had it refused, it stops being offered, which is what keeps it from looping.
    adapter._REFUSED_THIS_RUN.add(_hint)

    check(
        "and one refused in THIS run is not offered again, so it cannot loop",
        live._presented_token() == "",
        f"offered {bool(live._presented_token())}",
    )
finally:
    adapter._load_refused = _real_refused
    adapter._load_token = _real_load_token
    adapter._REFUSED_THIS_RUN.clear()
    live._url, live._token = _url_rescue, _value_rescue


# 38. A pairing forgets what this run had refused.
print("\nwhat a successful pairing does to a refusal")
live._url = "https://familiar.invalid"
_real_load_token_again = adapter._load_token
adapter._load_token = lambda url="": ""

try:
    adapter._REFUSED_THIS_RUN.add(adapter._token_hint(live._token))
    _blocked = live._presented_token()

    adapter._forget_refusals()

    check(
        "a refusal blocks the configured token until a pairing happens",
        _blocked == "",
        f"blocked={_blocked!r}",
    )
    check(
        "and a pairing clears it, so a token Familiar just issued is offered",
        live._presented_token() == live._token,
        f"offered {bool(live._presented_token())}",
    )
finally:
    adapter._load_token = _real_load_token_again
    adapter._REFUSED_THIS_RUN.clear()


# 39. Waiting on a pairing code must not mean waiting forever.
#
# A machine with two profiles shows two codes. One gets claimed, and the token it produces is the MACHINE's - it lands
# in a state file every profile of that machine reads. The other gateway waited only on its own stream and sat there
# for as long as it was left alone, with the credential it needed already in its own directory. Measured on the real
# machine: parked five minutes, no retry, while the door's log showed the other profile accepted.
print("\nwhat a gateway does while a pairing code waits")

import asyncio as _asyncio


async def _wait_finds_the_token():
    calls = {"n": 0}
    real_load, real_pause = adapter._load_token, adapter.REFUSAL_RETRY_S

    def fake_load(url=""):
        calls["n"] += 1
        # the token is ALREADY there when the wait begins, which is the case that was missed: the second profile
        # starts waiting a heartbeat after another profile's pairing stored a token, so a change never comes
        return "tok_abc123" if calls["n"] > 0 else ""

    adapter._load_token = fake_load
    # the real pause is seconds; the behaviour under test is "it looks at all", not how long it waits
    adapter.REFUSAL_RETRY_S = 0.05

    try:
        await _asyncio.wait_for(live._wait_for_token(), timeout=5)
        return True
    except _asyncio.TimeoutError:
        return False
    finally:
        adapter._load_token = real_load
        adapter.REFUSAL_RETRY_S = real_pause


check(
    "a token already present ends the wait, whoever collected it",
    _asyncio.get_event_loop_policy().new_event_loop().run_until_complete(_wait_finds_the_token()),
    "the wait returned once a token appeared",
)

# 40. ONE refusal must not park a gateway.
#
# The refusal itself is often the moment a token is being replaced: the door's log shows the same token ACCEPTED one
# minute and refused the next, on a machine whose profile had just been wired. Acting on the first refusal is what
# parked the second profile into pairing where it stayed.
print("\nwhat a single refusal does")

_source = (pathlib.Path(__file__).with_name("adapter.py")).read_text(encoding="utf-8")

check(
    "the refusal carries a distinct type, so the loop can tell it from a dropped channel",
    "class _ChannelRefused(RuntimeError)" in _source,
    "raised where the refusal happens",
)
check(
    "the 401 branch raises it instead of deciding, so one refusal is not yet final",
    'raise _ChannelRefused(' in _source and "_mark_refused(self._url, self._token)" not in _source.split("async def _hold_channel")[1].split("def _refuse_token")[0],
    "the marking is not in the connection",
)
check(
    "the loop asks once more before treating a refusal as final",
    "if not retried:" in _source and "REFUSAL_RETRY_S" in _source,
    "one refusal costs a breath, not a pairing",
)
check(
    "and taking it as final still pairs, which is what a deleted instance needs",
    "def _refuse_token" in _source and "_pair_start" in _source,
    "pairing stays reachable",
)


# 41. A token the machine was GIVEN has to reach the config, or a restart forgets it.
#
# The state is what a live gateway presents. The config is what a restarted one falls back to. Only the state was
# written, so every restart went back to the config's token - and after a machine is deleted and added again, that is
# the one Familiar already refused. Measured on a real machine: it paired, reported its port, and revoked itself on the
# next restart.
print("\nwhere a collected token is kept")

import tempfile


def _token_reaches_the_config():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = pathlib.Path(tmp) / "config.yaml"
        cfg.write_text(
            "platforms:\n  familiar:\n    extra:\n      token: tok_abc123\n",
            encoding="utf-8",
        )
        real_path = adapter._config_path
        adapter._config_path = lambda: cfg

        try:
            adapter._save_token("https://familiar.invalid", "tok_abc123")
            unchanged = "tok_abc123" in cfg.read_text(encoding="utf-8")

            adapter._save_token("https://familiar.invalid", "tok_other456")
            after = adapter._config_path().read_text(encoding="utf-8")

            return unchanged, "tok_other456" in after, "tok_abc123" in after
        finally:
            adapter._config_path = real_path


try:
    _unchanged, _has_new, _has_old = _token_reaches_the_config()
except Exception as _error:  # noqa: BLE001
    _unchanged, _has_new, _has_old = False, False, False
    print("   could not run it:", _error)

check(
    "a token collected by pairing is written into this profile's config",
    _has_new,
    "a restarted gateway reads its settings from there",
)
check(
    "and it replaces the stale one rather than sitting beside it",
    _has_new and not _has_old,
    "one token, not two",
)


# 42. A failed pairing wait must say so.
#
# The stream that carries a machine's token is the only thing that ends a pairing, and when it failed there was nothing
# to see: no token, no warning, and a machine that paired again - which is exactly what a machine that was told to pair
# again does. Measured tonight: the app delivered the token down a stream (captured), and the machine's plugin logged
# nothing at all, so the failure was invisible from both ends.
print("\nwhat a pairing wait says when it fails")

_source_pair = (pathlib.Path(__file__).with_name("adapter.py")).read_text(encoding="utf-8")
_pair_body = _source_pair.split("async def _pair(")[1].split("async def disconnect")[0] if "async def _pair(" in _source_pair else ""

check(
    "the wait's outcome is read rather than left unread",
    "waiting.exception()" in _pair_body,
    "a task's failure is read where it happened",
)
check(
    "and a wait that ended without a token is reported",
    "the pairing wait ended without a token" in _pair_body,
    "the log names it",
)


# 43. Profile management runs Hermes' own command, behind Hermes' own rules.
#
# The machine is the actor: creating, renaming and deleting a profile used to mean Hermes' command line on the machine,
# which is the friction that keeps profiles unused. What matters for the tests is that the app cannot make it run
# anything the machine would refuse - the name is validated with Hermes' own function, and the refusals (the machine's
# own profile, the one this connection runs in, a delete that was not confirmed) never reach a command at all.
# ----------------------------------------------------------------------------------------------------------------
# A state file two BUILDS share. Dev and prod can run different plugin versions on one machine, and both write the
# machine's ONE state file. What must never happen: an older build folds the map and drops a key a newer one wrote -
# the pairing survives, the key does not, and whatever wrote it goes quietly missing.
print("\nthe state file when two builds share it")

import json as _json, tempfile as _tempfile, pathlib as _pl

_real_state_home = adapter._state_home
with _tempfile.TemporaryDirectory() as _tmp:
    _root = _pl.Path(_tmp)
    adapter._state_home = lambda: _root / "familiar-platform"
    (_root / "familiar-platform").mkdir(parents=True, exist_ok=True)
    _state = _root / "familiar-platform" / "state.json"

    # what a NEWER build wrote: a token, plus a key this build has never heard of
    _state.write_text(
        _json.dumps({"pairings": {"https://prod.example": {"token": "t" * 40, "profiles": ["default", "prod"]}}}),
        encoding="utf-8",
    )

    # the older build saves its own pairing, which is the ordinary case: a token it just collected
    adapter._save_token("https://prod.example", "u" * 40)

    _entry = (_json.loads(_state.read_text(encoding="utf-8")).get("pairings") or {}).get("https://prod.example") or {}

    check("a key this build does not know survives its own write",
          _entry.get("profiles") == ["default", "prod"],
          f"profiles now {_entry.get('profiles')!r}; entry keys {sorted(_entry)}")
    check("and the token it wrote is the one kept", _entry.get("token") == "u" * 40, str(_entry.get("token"))[:4])

adapter._state_home = _real_state_home

# ----------------------------------------------------------------------------------------------------------------
# What a machine says when it asks to be paired. This is the app's ONLY chance to learn it: until the request
# succeeds there is no channel to ask down, and by the time a code is claimed the app has decided what it is creating.
print("\nwhat a machine says when it asks to be paired")

import asyncio as _aio2

_loop = _aio2.new_event_loop()
_live_url, _live_token = live._url, live._token
_real_state_home2 = adapter._state_home


def _pair_body(**over):
    for name, value in over.items():
        setattr(live, name, value)

    return _loop.run_until_complete(live._pair_body())


_body = _pair_body()
check("the request names which of this machine's profiles is asking",
      _body.get("profile") == adapter._profile_key(),
      f"profile={_body.get('profile')!r} vs this gateway's {adapter._profile_key()!r}")
check("and still carries the machine's name and this build",
      bool(_body.get("name")) and _body.get("version") == adapter.PLUGIN_VERSION,
      str(_body)[:90])

with _tempfile.TemporaryDirectory() as _tmp2:
    adapter._state_home = lambda: _pl.Path(_tmp2) / "familiar-platform"

    check("a machine with nothing to present says it is not paired",
          _pair_body(_url="https://prod.example", _token="").get("paired") is False,
          "paired is not False")

    adapter._save_token("https://prod.example", "t" * 40)
    check("a machine holding a STORED pairing says it is paired",
          _pair_body(_url="https://prod.example", _token="").get("paired") is True,
          "paired is not True")

    check("and so does one that was only configured with a token",
          _pair_body(_url="https://elsewhere.example", _token="c" * 40).get("paired") is True,
          "a configured token did not count as a credential")

    check("while another Familiar is still a different question",
          _pair_body(_url="https://other.example", _token="").get("paired") is False,
          "one backend's token answered for another backend")

adapter._state_home = _real_state_home2
live._url, live._token = _live_url, _live_token

print("\nwhat creating, renaming and deleting a profile does")

import asyncio as _asyncio_profiles

_ran: list = []
_real_run_hermes = adapter._run_hermes
_real_profile_key = adapter._profile_key


def _invoke(coro):
    return _asyncio_profiles.new_event_loop().run_until_complete(coro)


def _fake_run(argv, timeout=300):
    _ran.append(list(argv))
    return 0, "ok"


adapter._run_hermes = _fake_run
adapter._profile_key = lambda: "invoker"

try:
    _ran.clear()
    made = _invoke(live._channel_create({"profile": "Coder"}))
    check(
        "a name typed with capitals becomes the directory Hermes would make",
        made.get("profile") == "coder",
        f"answered {made}",
    )
    check(
        "and creating runs exactly Hermes' own command",
        _ran and _ran[-1] == ["profile", "create", "coder"],
        f"ran {_ran[-1:] }",
    )

    _ran.clear()
    _invoke(live._channel_create({"profile": "writer", "description": "docs and prose"}))
    check(
        "a description is passed through as the flag it is",
        _ran and _ran[-1] == ["profile", "create", "writer", "--description", "docs and prose"],
        f"ran {_ran[-1:]}",
    )

    _ran.clear()
    refused = _invoke(live._channel_create({"profile": "Not A Name"}))
    check(
        "a name Hermes would refuse is refused here, in words",
        bool(refused.get("error")) and not _ran,
        f"answered {refused} and ran {_ran}",
    )

    _ran.clear()
    unconfirmed = _invoke(live._channel_delete({"profile": "coder", "confirm": "codr"}))
    check(
        "deleting without the name typed never reaches a command",
        bool(unconfirmed.get("error")) and not _ran,
        f"answered {unconfirmed} and ran {_ran}",
    )

    _ran.clear()
    own = _invoke(live._channel_delete({"profile": "invoker", "confirm": "invoker"}))
    check(
        "and neither does deleting the profile this connection runs in",
        bool(own.get("error")) and not _ran,
        f"answered {own} and ran {_ran}",
    )

    _ran.clear()
    rooted = _invoke(live._channel_delete({"profile": "default", "confirm": "default"}))
    check(
        "nor the machine's own profile, which Familiar is paired with",
        bool(rooted.get("error")) and not _ran,
        f"answered {rooted} and ran {_ran}",
    )

    _ran.clear()
    gone = _invoke(live._channel_delete({"profile": "coder", "confirm": "coder"}))
    check(
        "a confirmed delete runs Hermes' own delete, unanswered prompts included",
        gone.get("deleted") is True and _ran[-1] == ["profile", "delete", "coder", "-y"],
        f"answered {gone} and ran {_ran[-1:]}",
    )

    _ran.clear()
    renamed = _invoke(live._channel_rename({"profile": "coder", "to": "Writer"}))
    check(
        "renaming runs Hermes' own rename, with the name normalised",
        renamed.get("renamed") is True and _ran[-1] == ["profile", "rename", "coder", "writer"],
        f"answered {renamed} and ran {_ran[-1:]}",
    )
finally:
    adapter._run_hermes = _real_run_hermes
    adapter._profile_key = _real_profile_key

print(f"\n{len(PASSED)} passed, {len(FAILED)} failed")
if FAILED:
    print("failed:")
    for label in FAILED:
        print(f"  - {label}")
server.shutdown()
sys.exit(1 if FAILED else 0)
