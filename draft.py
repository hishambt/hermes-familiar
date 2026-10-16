"""The one place this plugin asks a MODEL something.

A scheduled job is made of fields and the reader usually has a sentence. Nothing on the machine turns one into the
other, so this asks - through Hermes' own plugin-LLM API, on a task this plugin REGISTERED, which is what makes the
call allowed: a task key a plugin registered itself is always permitted, so nobody edits a config to grant it, and the
task gets its own entry in the machine's model picker instead.

The answer is a DRAFT, never a job. It fills a form the reader corrects, so the schema below is the contract between
this call and that form - and a reply that does not match it is an ERROR here rather than a guess somewhere else.

The schema is also where the machine's own rules are enforced early: a script job always carries a script, a monitor
never appears on a job with no agent to wake, and only http(s) is worth checking. The instance refuses those anyway -
this just stops the refusal from being the reader's first sight of it.
"""

from __future__ import annotations

from typing import Any
from collections.abc import Mapping, Sequence

#: The auxiliary task this plugin registers and then calls. Hermes allows a task its own plugin registered, and gives
#: it an ``auxiliary.<key>`` block with a picker entry and an ``AUXILIARY_<KEY>_*`` env bridge - so which model drafts
#: a job is a choice in the UI, and the default is the machine's own main model.
DRAFT_TASK = "familiar_platform_job_draft"
DRAFT_TASK_LABEL = "Familiar job draft"
DRAFT_TASK_DESCRIPTION = (
    "Turns one sentence into a scheduled job's fields: what runs, when, and where its output goes. "
    "Used by the Familiar client's job form, on the machine the job will run on."
)

#: Which model the draft runs on, resolved by Hermes on the first call. Kept here rather than passed around, because
#: the plugin's entry point is the only place the context exists and the API handlers are where it is needed.
_llm: Any = None


class DraftUnavailable(Exception):
    """Nothing can draft on this machine: the plugin has no model handle."""


class DraftInvalid(Exception):
    """A reply came back that is not a usable draft, and saying so beats guessing."""


def set_llm(llm: Any) -> None:
    """Hold the plugin's LLM handle, which the entry point is handed once at startup."""
    global _llm
    _llm = llm


def available() -> bool:
    """Whether a draft can be attempted at all. A machine whose plugin has no handle says so rather than failing late."""
    return _llm is not None


#: The four shapes the machine accepts, and the fields each one uses. Nothing else is a job.
DRAFT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "shape",
        "name",
        "schedule",
        "prompt",
        "script",
        "script_name",
        "script_text",
        "monitor_url",
        "deliver",
        "topic",
        "why",
    ],
    "properties": {
        "shape": {
            "type": "string",
            "enum": ["agent", "script", "scriptThenAgent", "watch"],
            "description": (
                "What runs each time. 'agent' - the model does the work every run. 'script' - a script does the whole "
                "job and its output IS the message, with no model in the loop and no cost per run. 'scriptThenAgent' - "
                "the script runs first and the agent is told what it said. 'watch' - a URL is checked first and the "
                "agent runs only when what it returns changed."
            ),
        },
        "name": {
            "type": "string",
            "description": "What the job is called on the machine. Short, and specific enough to recognise in a list.",
        },
        "schedule": {
            "type": "string",
            "description": (
                "When it runs: a cron expression such as '0 4 * * *', an interval such as 'every 30m', or a plain "
                "phrase the machine parses such as 'every monday 9am'."
            ),
        },
        "prompt": {
            "type": "string",
            "description": (
                "What the agent is told. For 'scriptThenAgent', what to DO with the script's output. Empty string "
                "when the shape needs no agent."
            ),
        },
        "script": {
            "type": ["string", "null"],
            "description": (
                "The NAME of a script that already exists on the machine, taken from the list given to you. null when "
                "the shape runs no script, or when script_text is used instead."
            ),
        },
        "script_name": {
            "type": ["string", "null"],
            "description": (
                "The FILE name for a script written in script_text, with its suffix - 'notes_backup.py' or "
                "'check_disk.sh'. Lowercase, underscores, no directories. null when an existing script is used or no "
                "script runs."
            ),
        },
        "script_text": {
            "type": ["string", "null"],
            "description": (
                "The complete text of a NEW script, only when none of the existing ones does the job. Python (.py) or "
                "bash (.sh/.bash) only, standard library only, and its standard output is what gets delivered. null "
                "when an existing script is used or no script runs."
            ),
        },
        "monitor_url": {
            "type": ["string", "null"],
            "description": "The http(s) URL that shape 'watch' checks before each run. null for every other shape.",
        },
        "deliver": {
            "type": "string",
            "enum": ["familiar", "local", "telegram", "topic"],
            "description": (
                "'familiar' - into Familiar's notifications, the machine's client. 'topic' - also readable in a "
                "thread, named below. 'telegram' - the machine's own home chat. 'local' - a file on the machine that "
                "nobody is ever told about."
            ),
        },
        "topic": {
            "type": ["string", "null"],
            "description": "The NAME of an existing topic, only when deliver is 'topic'. null otherwise.",
        },
        "why": {
            "type": "string",
            "description": (
                "One sentence, in the language the request was written in, saying what the job will do and what it "
                "costs to run. This is shown to the reader above the fields."
            ),
        },
    },
}

#: Our own words, and the machine's rules stated where they are decided. Kept as one constant so the contract with the
#: form above and the behaviour below cannot drift apart in two files.
DRAFT_SYSTEM = """\
You turn one person's request into ONE scheduled job for a Hermes machine, and answer with JSON only.

The machine refuses these combinations, so a draft that breaks them is worthless:
- A script job IS a script. Shape "script" and "scriptThenAgent" must name an existing script in `script`, or write a
  new one in `script_text`.
- A monitor exists to decide whether to wake an AGENT. Shape "watch" always carries a prompt, and a job with no agent
  ("script") can never have a monitor_url.
- Only http or https URLs are checked.
- A script has to be one of the names you were given: the machine runs scripts from one directory and nothing else, so
  a name you invented would be refused. If none of them does the job, write a complete new script in `script_text`.
- A new script runs as Python (.py) or as bash (.sh or .bash) - the SUFFIX decides, a shebang is ignored - and its
  standard output is what gets delivered. Standard library only. It has to work unattended, print something useful,
  and never wait for input.

Choose the shape that does the work for the least: a script that needs no model costs nothing on every run, so use one
when the work is deterministic. Use an agent when judgement is genuinely needed, and "watch" when the answer is
usually "nothing changed" - that shape only wakes the agent when something did.

Write `name` short and recognisable, and `schedule` in a form the machine parses (a cron expression, "every 30m", or
"every monday 9am").

LANGUAGE: everything you write is read by the person who asked, so every string - the name, the schedule, the prompt,
the script's own comments and its output - is in the SAME LANGUAGE as the request. An answer in another language is a
wrong answer, whatever it says.

When you write a new script, `script_name` is what the file will be called: a bare name with a .py, .sh or .bash
suffix, lowercase, and it must not be one of the names you were given.

Fill every field. Use null and the empty string where a field does not apply to the shape you chose - do not omit
them, and do not invent a fifth shape.
"""

DRAFT_INSTRUCTIONS = """\
Draft ONE scheduled job from the request below, as a single JSON object matching the requested schema.

You are given the machine's name, the scripts it already has, the topics a reader watches, and - when the request is a
correction - the draft so far. When a draft is given, change only what the request asks for and keep everything else
exactly as it is, including a script or a URL somebody chose by hand.
"""


def _rule(label: str, values: Sequence[str], empty: str) -> str:
    """One context line, saying when there is nothing to list rather than showing an empty list."""
    return f"{label}: {', '.join(values)}" if values else f"{label}: {empty}"


def _context_blocks(
    *, machine: str, scripts: Sequence[str], topics: Sequence[str], current: Mapping[str, Any] | None
) -> list[str]:
    """What the model cannot see for itself, as plain lines.

    A model with no tools cannot list a directory or read a database, so the machine's own facts are handed to it: the
    scripts that EXIST (a name it invents would be refused), the topics a reader has, and the draft so far when this is
    a correction. Without these it would guess, and a guessed script name fails at the machine.
    """
    blocks = [
        _rule("Machine", [machine] if machine else [], "an unnamed Hermes machine"),
        _rule("Scripts it already has", list(scripts), "none - a script job needs script_text"),
        _rule("Topics a reader watches", list(topics), "none"),
    ]

    if current:
        import json

        blocks.append("The draft so far, as JSON:\n" + json.dumps(dict(current), ensure_ascii=False, sort_keys=True))

    return blocks


async def draft_job(
    ask: str,
    *,
    machine: str = "",
    scripts: Sequence[str] = (),
    topics: Sequence[str] = (),
    current: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """One sentence (and, on a correction, the draft so far) -> a job's fields.

    Raises `DraftUnavailable` when this machine has no model handle, and `DraftInvalid` when the reply is not a usable
    draft. Both are the caller's to report: a form that quietly filled itself with nothing would be worse than one that
    said the draft failed.
    """
    if _llm is None:
        raise DraftUnavailable(
            "This machine's Familiar plugin has no model handle, so it cannot draft a job. Reload the plugin."
        )

    # Imported here rather than at module level: the plugin is loaded BY Hermes, and the check suite loads these files
    # flat. Neither ordering is worth breaking for one import.
    from agent.plugin_llm import PluginLlmTextInput

    inputs = [PluginLlmTextInput(text=block) for block in _context_blocks(
        machine=machine, scripts=scripts, topics=topics, current=current
    )]
    inputs.append(PluginLlmTextInput(text=f"The request:\n{ask}"))

    try:
        result = await _llm.acomplete_structured(
            instructions=DRAFT_INSTRUCTIONS,
            input=inputs,
            json_schema=DRAFT_SCHEMA,
            schema_name="ScheduledJobDraft",
            system_prompt=DRAFT_SYSTEM,
            task=DRAFT_TASK,
        )
    except Exception as error:  # noqa: BLE001 - the host's words are the reason, whatever it raised
        raise DraftInvalid(f"The draft could not be made: {error}") from error

    if getattr(result, "parsed", None) is None:
        # The host validates the JSON against the schema when it can, so this is a reply that was not JSON at all -
        # prose, or a truncated answer. Reported rather than repaired.
        raise DraftInvalid("The model did not answer with a usable draft. Try again, or say it differently.")

    return dict(result.parsed)
