"""The commands this plugin puts on Hermes' own command line.

One command, ``hermes familiar pair``, and the reason it exists is that the recipe without it is a paragraph of
shell. Reading a machine's pairing code needs three things a reader should not have to know: the JSON endpoint, the
port, and the fact that a gateway restart is asynchronous so the first attempt can be refused. All three live in
here instead, and the reader types one line.

The waiting is the point. Pairing writes a token the gateway reads at STARTUP, so it restarts - and while it
restarts the ingress does not exist, which means a connection to it is REFUSED before any code of ours runs. No
endpoint can absorb that: there is no server to run logic in. It has to be waited out from this side, which is
where a command belongs that already says it is waiting.

Imports stay stdlib-only and relative-safe on purpose: the check suite loads these files flat and the package load
does not, and one spelling that works in both is worth more than a module-level import.
"""

from __future__ import annotations

import argparse
import json
import socket
import time
import urllib.error
import urllib.request
from typing import Any

#: How long to wait for the gateway, and how often to look. A restart is asynchronous and the ingress is the last
#: thing to appear, so this is a wait with a deadline rather than a retry with a count: the port either opens or the
#: gateway did not come back, and those are different things to tell a reader.
WAIT_S = 30.0
TICK_S = 0.5

#: Where the ingress is when nobody moved it, matching DEFAULT_INGRESS_PORT in the adapter.
DEFAULT_PORT = 8644


def _adapter() -> Any:
    """The adapter module, imported the way this plugin is loaded."""
    try:
        from . import adapter as _adapter  # type: ignore[no-redef]
    except ImportError:  # pragma: no cover - the flat load the check suite uses
        import adapter as _adapter  # type: ignore[no-redef]

    return _adapter


def _port(profile: str | None) -> int:
    """The port THIS profile's ingress took, or the default a machine that never reported one is read at."""
    adapter = _adapter()

    for candidate in (profile, adapter._profile_key()):
        if not candidate:
            continue

        recorded = adapter._load_ingress_port(candidate)

        if recorded:
            return int(recorded)

    return DEFAULT_PORT


def _wait_for_port(host: str, port: int, deadline: float) -> bool:
    """Whether something is listening on the port before the deadline.

    A refused connection is the ordinary state of a gateway that is restarting, so it is not an error here: it is
    what this waits out. Anything already listening is enough - the ingress answers its own probe, and the port
    being open is what the reader's next step needs.
    """
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.settimeout(1.0)

            try:
                probe.connect((host, port))

                return True
            except OSError:
                time.sleep(TICK_S)

    return False


def _read_code(host: str, port: int, timeout: float) -> dict[str, Any] | None:
    """What the machine answers on its pairing endpoint, or None if it could not be read."""
    request = urllib.request.Request(f"http://{host}:{port}/familiar/pair")

    try:
        with urllib.request.urlopen(request, timeout=timeout) as answer:  # noqa: S310 - loopback, by construction
            return json.loads(answer.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, ValueError, OSError):
        return None


def pair_command(args: argparse.Namespace) -> int:
    """Point this profile at a Familiar when it is told one, wait for the gateway, print the code."""
    adapter = _adapter()
    profile = getattr(args, "profile", None) or adapter._profile_key()
    address = (getattr(args, "url", None) or "").strip()

    if address:
        # The address is the one thing only the reader has: the machine cannot know WHICH Familiar it is being added
        # to. Written here rather than by a separate `config set`, which is one fewer command to explain - and the
        # write goes into this profile's own config, where a restarted gateway reads it.
        # NOT `platforms.familiar.extra.url`, measured: at gateway config load the plugin's `env_enablement_fn`
        # seeds `extra` from the FAMILIAR_* env vars and the gateway commits that seed ON TOP of the configured
        # block, so an `extra.url` set here is silently replaced by the default (http://127.0.0.1:3100) and the
        # machine dials its own loopback while the setting looks right. The env-shaped name is the spelling that
        # survives, and it is the one the machine's own `config set FAMILIAR_URL` writes.
        if not adapter._write_env_setting("FAMILIAR_URL", address):
            print(f"Could not write the address into {profile}'s config. Is Hermes configured for this profile?")

            return 1

        print(f"Pointed {profile} at {address}.")

        # The running gateway dials the address it read at STARTUP, so what was just written is not what it is using.
        code, output = adapter._run_hermes(["-p", profile, "gateway", "restart"], 180)

        if code != 0:
            print(f"The gateway did not restart: {(output or '').strip().splitlines()[-1] if output else 'no output'}")

            return 1

        print("Restarted the gateway so it reads it.")


    host = "127.0.0.1"
    port = _port(profile)
    deadline = time.monotonic() + WAIT_S

    print(f"Waiting for the gateway to come up on {host}:{port} ...")

    if not _wait_for_port(host, port, deadline):
        print(
            f"Nothing is listening on {host}:{port}.\n"
            f"The gateway restarts when it reads the plugin, and this waited {WAIT_S:.0f}s for it.\n"
            f"Check it with:  hermes -p {profile} gateway status"
        )

        return 1

    # The port is open, so the read can take the remaining time: the plugin answers its own pairing request only
    # once it has a code, and it waits for one itself before replying.
    answer = _read_code(host, port, timeout=max(1.0, deadline - time.monotonic()))

    if answer is None:
        print(
            f"The ingress on {host}:{port} did not answer. "
            f"Check it with: hermes -p {profile} plugins doctor familiar-platform"
        )

        return 1

    if answer.get("paired"):
        print(f"This machine is already paired with a Familiar. Nothing to do - the token is in {profile}'s config.")

        return 0

    code = answer.get("code")

    if not code:
        # Reached only when Familiar could not be reached to mint one: the plugin's own answer, which is the truth
        # rather than a stall.
        print(
            "No code is being shown. The machine could not reach the Familiar this profile points at.\n"
            f"Check the address with: hermes -p {profile} config get FAMILIAR_URL"
        )

        return 1

    print(
        f"\nThe code this machine is showing:\n\n    {code}\n\n"
        f"Type it into Familiar's \"Connect a machine\" dialog.\n"
        f"Plugin {answer.get('version', '?')} on profile {profile}."
    )

    return 0


def familiar_command(args: argparse.Namespace) -> int:
    """``hermes familiar <action>``: what this plugin puts on the command line."""
    action = getattr(args, "familiar_action", None)
    handlers = {"pair": pair_command}

    handler = handlers.get(action or "")

    if handler is None:
        print(f"hermes familiar: {action or '(no action)'} - try one of: {', '.join(sorted(handlers))}")

        return 2

    return handler(args)


def register_cli(subparser: argparse.ArgumentParser) -> None:
    """Add this plugin's actions to the ``hermes familiar`` subparser."""
    subs = subparser.add_subparsers(dest="familiar_action", metavar="ACTION")

    pair = subs.add_parser("pair", help="Show the code this machine is presenting to pair with a Familiar")
    pair.add_argument(
        "url",
        nargs="?",
        default=None,
        help="The Familiar to point this profile at. Given once, on the first pairing; later runs only read the code.",
    )
    pair.add_argument("--profile", default=None, help="The Hermes profile to read the code for (default: this one)")

    subparser.set_defaults(func=familiar_command)
