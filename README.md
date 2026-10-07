# hermes-familiar

A Hermes plugin that makes a machine a Familiar client's
channel: it delivers output to the client's notifications, carries a person's message into the agent, and
answers what the client asks about this machine's own conversations. Runs on the machine Hermes runs on.
Standard library only, MIT licensed.

## Install

```bash
hermes plugins install hishambt/hermes-familiar --enable
```

That is the whole of it on a machine that has `git` and a way out to GitHub. Hermes scans a plugin from an
unreviewed source as it installs and prints a warning line saying so: this one scans safe, and the warning is the
scanner telling you it looked. For a machine without `git`, unpack the
archive from the [latest release](https://github.com/hishambt/hermes-familiar/releases/latest) into the
directory Hermes reads plugins from. The archive's own top level is `familiar-platform/`, so it lands exactly
where `hermes plugins enable` looks:

```bash
unzip -o familiar-platform.zip -d ~/.hermes/plugins/
hermes plugins enable familiar-platform
```

Either way the plugin is in place and pointed nowhere yet: the settings and the pairing code are under
[Setup](#setup).

## Why it exists

A Hermes cron job's output is **pushed, never read**. The instance's Jobs API carries status and no prose, so a
job that ran and a job whose words nobody can reach look identical from a client. The output has three homes:
the delivery target, a file under `cron/output/` on the instance, or an opt-in mirror into a chat's transcript.
For a client like Familiar the first one is the only useful answer.

A cron `deliver` target is valid when it is a built-in platform name or a platform a plugin registered.
Registering one with a **cron delivery env var** is what makes `deliver: familiar` a legal job target
(`cron/scheduler_delivery.py`), and `send()` is the whole contract the delivery path needs.

The other direction is the channel. A message from the client becomes a turn on this machine, the questions and
permissions a turn raises reach the client with the id their answer comes back under, and the client's reads of
this machine's conversations are answered here, out of Hermes' own state.

## How it works

- The plugin registers the platform `familiar`. Once a token is configured, Hermes' gateway enables it
  automatically and holds an adapter, exactly as it would for Telegram.
- A job with `deliver: familiar` resolves its target from `FAMILIAR_HOME_CHANNEL`, then calls the adapter's
  `send()`, which POSTs to Familiar.
- Cron deliveries ride the same path as Hermes' own `send_message` tool, so this is not only for scheduled
  work: any Hermes event can notify a client.
- The adapter's job id rides along on a cron delivery, because the scheduler puts it in the route metadata.
  A `send_message` call sends `jobId: null`, which is a different fact from "job unknown".

## The wire contract

`POST {url}/api/hermes/notifications`, with `Authorization: Bearer {token}` and `Content-Type: application/json`.

```json
{
  "instance": "default",
  "target": "default",
  "content": "Daily payment reminder: 3 invoices are past due.",
  "truncated": false,
  "jobId": "02c072f93038",
  "jobName": "Daily payment reminder",
  "sentAt": "2026-09-29T15:22:44+00:00"
}
```

- `instance` labels which Hermes spoke, so one Familiar serving several instances can say so.
- `target` is the delivery the job asked for, `default` being the instance's own inbox.
- `truncated` is true when the message was longer than the plugin's 20 000-character cap, which is the same cap
  Familiar enforces on its side: it refuses a longer delivery rather than trimming it, so anything over the cap
  has to be cut here or it does not arrive.
- `jobId` is the scheduled job's id when a cron delivery produced it, otherwise null, and `jobName` is that
  job's own name when this process can read the cron store. The name is what a reader recognizes, so a client
  can title the notification with it; the id is what anything acting on the job uses, and it is what stays
  when the name cannot be read.
- A 2xx answer should carry `{"id": "..."}`; the plugin stores it as the delivery's message id. An empty body
  is accepted, and the id is then absent rather than invented.
- Any non-2xx is a failed delivery. **401 and 403 are treated as permanent** (a refused token will not fix
  itself); 5xx, timeouts and connection failures are marked retryable and the scheduler's delivery ledger
  retries them. A retry can duplicate a delivery that in fact landed: dedupe on
  `(instance, jobId, content)` if that matters to you.

## What a read can be asked for

The client's reads of this machine's conversations are answered out of Hermes' own state, and the listing is the
one that takes arguments:

- `limit` / `offset` - a page. `limit` is clamped to 200, the same ceiling the API server uses.
- `source` - one source's sessions (`cli`, `telegram`, `familiar`, ...).
- `search` - free text, matched against a session's **title or id anywhere in its compression chain**. It is
  applied in SQL **before** the page, so page two of a search is page two of the matches. This is the reason to
  send it here rather than filter a page in the client, which can only ever answer about the page it holds.
- `total` comes back with the rows: how many the filters admit. It is exact, and how the machine gets it depends
  on the question. A listing with no search uses the store's own count, over the same WHERE its rows came
  through. A search is counted by **reading the matches** - nothing above the store counts a search, and the
  matches are what its own filter admits, so their length is the answer - up to 5000 of them. Only **past** that
  ceiling is `total` `null`: count more than that and the machine is reading rows nobody asked for, and a number
  nobody read is worse than none. Show what you have; do not invent a number.

### A conversation

`GET /familiar/conversation?ids=<session id>,<session id>`, asked the way every read above is: down the connection
this machine holds, so it works on a machine with no address. It answers which sessions one conversation is and
what they add up to. Ask about any of them - the one it started in, the middle, the one it is in now - and the
answer is the same conversation.

The listing above cannot answer this on its own: Hermes' list hides the sessions a conversation was compressed
into, so a reader of that list can neither see nor count them. The walk happens where the store is.

```json
{
  "conversations": {
    "20261006_101800_chainmid12": {
      "sessions": [
        { "id": "20261006_101700_chainroot1", "end_reason": "compression", "message_count": 5, "estimated_cost_usd": 0.25, "...": "..." },
        { "id": "20261006_101800_chainmid12", "end_reason": "compression", "message_count": 7, "estimated_cost_usd": 0.5, "...": "..." },
        { "id": "20261006_101900_chaintip12", "end_reason": "session_reset", "message_count": 1, "estimated_cost_usd": null, "...": "..." }
      ],
      "total": { "sessions": 3, "message_count": 13, "input_tokens": 350, "estimated_cost_usd": 0.75 }
    }
  }
}
```

- The sessions come **oldest first**, and each carries what the single-session read carries for it.
- A link is only a continuation when the **parent ended by `compression`**. A branch, a reset - which the
  instance's own account calls a separate conversation that merely keeps the pointer - and a subagent run are
  conversations of their own, and their numbers are not this conversation's.
- `total` adds the chain up. `estimated_cost_usd` is `null` when no session in the chain reported one: a total
  nobody gave is not zero.
- At most 50 ids per call, which is a page of rows in one request. An unknown id answers with no sessions rather
  than a guess.

## Setup

**A machine Familiar has a shell on needs none of this.** Familiar's own setup finds Hermes and runs the
install above on that machine, enables it, sets all three settings below and restarts the gateway, and the
machine reports itself back. What follows is for a machine Familiar has no shell on, or for a reader doing it
by hand.

The token is the one thing not set by hand: it is issued when the machine **pairs**. Install and enable the
plugin, point it at where Familiar is with `FAMILIAR_URL`, restart the gateway - and the machine shows a code.
Two places have it, and both are on that machine: the log, at the moment it is minted, and
`http://127.0.0.1:8644/familiar/pair`, which answers with the code while this machine is unpaired and
`paired: true` once it is not. If the code has not arrived yet it is asked for and waited on, so one command is
enough: nothing here needs running twice to get an answer. Claiming that code in Familiar creates the instance and hands the machine its
token. It keeps it under Hermes' own home - `~/.hermes/familiar-platform/state.json`, never inside this
plugin's directory, which an update replaces outright: a pairing thrown away by an update is a machine made to pair
again for no reason. Setting `FAMILIAR_TOKEN` by hand instead works, but a pairing replaces it.

**Deleting the instance is not the end of that machine.** It keeps the token it was given, so Familiar refuses
its next connection - and a machine that is refused drops that token and pairs again, showing a new code in the
same two places. A token the machine was configured with (`FAMILIAR_TOKEN`) is remembered as refused rather than
deleted, so a corrected value is still honoured the moment it changes.

**If nothing answers on 8644**, the plugin did not load or something else on that machine holds the port. The
gateway log says which (`ingress could not bind 127.0.0.1:8644`), and a machine that pairs and delivers while
its ingress is down still cannot take a message through it. Move it with `FAMILIAR_INGRESS_PORT` (and
`FAMILIAR_INGRESS_HOST`) if the port is taken - both are settings like any other, and the address to read the
code from moves with them.

Four cards in Familiar's Console, in this order:

1. **Plugins → Install a plugin**: `hishambt/hermes-familiar`
2. **Plugins → Enable a plugin**: `familiar-platform`
3. **Configuration → Set a setting**:
   - `platforms.familiar.extra.url=http://localhost:3100` when Familiar is not on the default address
   - `platforms.familiar.extra.instance=homelab` to label this instance in the notifications list
   - `FAMILIAR_TOKEN=<the token Familiar issued for this instance>` only when wiring by hand rather than pairing
     (the `_TOKEN` suffix is what routes it to `.env` rather than `config.yaml`)
4. **This machine → Restart the gateway**, which is the re-discovery a plugin needs.

From a terminal it is the same four steps:

```bash
hermes plugins install hishambt/hermes-familiar
hermes plugins enable familiar-platform
hermes config set FAMILIAR_URL http://localhost:3100   # if Familiar is not on the default address
hermes gateway restart
curl -s http://127.0.0.1:8644/familiar/pair            # the code to claim in Familiar
```

Verify with `hermes plugins doctor familiar-platform` and `hermes gateway status` (the platform appears
once a token is set). A job then gets `deliver: familiar` from Familiar's own job dialog.

### The self-check

`check.py` proves the whole contract without a Familiar to talk to: it registers the plugin in a real platform
registry, asserts that the scheduler accepts `deliver: familiar` and resolves it to `FAMILIAR_HOME_CHANNEL`,
then delivers to a stub receiver on loopback and reads what arrived (path, bearer header, payload, and how a
401, a 503 and an unreachable host each come back). Run it from the Hermes install, with its own interpreter:

```bash
cd "$LOCALAPPDATA/hermes/hermes-agent"                    # ~/.hermes/hermes-agent on Linux
./venv/Scripts/python.exe /path/to/hermes-familiar/check.py   # venv/bin/python on Linux
```

## Settings

| `platforms.familiar.extra` | Env var | Default | Meaning |
|---|---|---|---|
| `url` | `FAMILIAR_URL` | `http://127.0.0.1:3100` | Where Familiar's API is |
| `token` | `FAMILIAR_TOKEN` | none, required | Bearer token Familiar issued for this instance |
| `instance` | `FAMILIAR_INSTANCE` | the profile name | Label shown with every delivery |
| `home_channel` | `FAMILIAR_HOME_CHANNEL` | `default` | Target for a job that names no chat |

`config.yaml` wins over the environment, as it does for every other platform plugin.

## What it cannot do

- **Reach a remote instance.** The plugin runs where Hermes runs. Installing it on this machine changes
  nothing for a Hermes on another box; that box needs the plugin too.
- **Receive.** Only on this machine's own loopback. A message from Familiar arrives at the plugin's ingress,
  or down the connection this machine holds, which is outbound. Nothing listens on a public interface.
- **Deliver with no token.** Without `FAMILIAR_TOKEN` the platform stays disabled rather than failing
  deliveries quietly, and `send()` refuses with an error the caller can read.
- **Guarantee exactly-once.** See the retry note above.

## Security

- The token is read per delivery and never logged, never echoed in an error, and never written into the
  plugin's own metadata on the instance. Errors name the status code and Familiar's response body only.
- Everything inbound is authorized twice over: the ingress requires this instance's own token, and the gateway
  refuses any sender who is not on `FAMILIAR_ALLOWED_USERS` (set `FAMILIAR_ALLOW_ALL_USERS` to widen it).
- The default address is loopback. Pointing it at a hostname means the token travels over the network, so use
  HTTPS when it is not `127.0.0.1`.
