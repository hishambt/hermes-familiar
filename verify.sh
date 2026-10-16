#!/usr/bin/env sh
# Everything that has to hold before a change to this plugin is called done.
#
#   ./verify.sh
#
# Ruff is the linter, configured in pyproject.toml: tabs and long prose are this repo's style and are stated there
# rather than argued about. It is installed beside the interpreter that runs the check, so it is found from
# `sys.executable` rather than from PATH - which is the difference between this working and it being "not found" on a
# machine where the venv is not activated.
#
# check.py asserts what this plugin DOES against a real Hermes: registration, enablement, the pairing, the channel,
# every answer it posts. It imports `gateway.*` and `cron.*`, so it runs with that install's interpreter and from that
# directory - see its own docstring. Set HERMES_AGENT_DIR if yours is somewhere other than the default.
set -e

HERE=$(cd "$(dirname "$0")" && pwd)

# The Hermes install this plugin runs inside: its venv holds the interpreter that can import `gateway.*`, and ruff
# beside that interpreter. Given by HERMES_AGENT_DIR, or worked out from whichever `hermes` is on PATH - asked rather
# than assumed, because the obvious guess (`$HOME/...`) is wrong on a machine whose shell home is not its profile.
if [ -z "$HERMES_AGENT_DIR" ]; then
	HERMES_BIN=$(command -v hermes || true)
	if [ -n "$HERMES_BIN" ]; then
		HERMES_AGENT_DIR=$(cd "$(dirname "$HERMES_BIN")/.." 2>/dev/null && pwd -P || true)
	fi
fi
if [ -z "$HERMES_AGENT_DIR" ] || [ ! -d "$HERMES_AGENT_DIR" ]; then
	echo "HERMES_AGENT_DIR is not set and no hermes install could be found." >&2
	echo "Set it to the directory holding venv/ - e.g. HERMES_AGENT_DIR=~/.hermes/hermes-agent ./verify.sh" >&2
	exit 1
fi

PYTHON="$HERMES_AGENT_DIR/venv/Scripts/python.exe"
[ -x "$PYTHON" ] || PYTHON="$HERMES_AGENT_DIR/venv/bin/python"
[ -x "$PYTHON" ] || { echo "no interpreter at $HERMES_AGENT_DIR/venv" >&2; exit 1; }

# Found through the interpreter rather than guessed: `ruff` and `ruff.exe` sit beside the python that installed it,
# and which of the two exists is the platform's business, not this script's.
RUFF="$("$PYTHON" -c 'import os, sys
d = os.path.dirname(sys.executable)
for n in ("ruff", "ruff.exe", "ruff.cmd"):
    p = os.path.join(d, n)
    if os.path.exists(p):
        print(p)
        break')"
if [ -z "$RUFF" ]; then
	echo "ruff is not installed beside $PYTHON - run: $PYTHON -m pip install ruff" >&2
	exit 1
fi

echo "== ruff =="
"$RUFF" check "$HERE"

echo "== the plugin's own checks =="
(cd "$HERMES_AGENT_DIR" && "$PYTHON" "$HERE/check.py")

echo "== all clear =="
