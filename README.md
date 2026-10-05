# hermes-familiar

A Hermes plugin that delivers output to a [Familiar](https://github.com/hishambt/familiar-frontend) client's
notifications. Runs on the machine Hermes runs on. Standard library only.

## Why it exists

A Hermes cron job's output is **pushed, never read**. The instance's Jobs API carries status and no prose, so a
job that ran and a job whose words nobody can reach look identical from a client. The output has three homes:
the delivery target, a file under `cron/output/` on the instance, or an opt-in mirror into a chat's transcript.
For a client like Familiar the first one is the only useful answer.

A cron `deliver` target is valid when it is a built-in platform name or a platform a plugin registered.
Registering one with a **cron delivery env var** is what makes `deliver: familiar` a legal job target
(`cron/scheduler_delivery.py`), and `send()` is the whole contract the delivery path needs. That is all this
plugin is: a platform, a sender, and no way in.

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

## Setup

The intended path is Familiar's own Console, which runs Hermes commands on the machine its backend is on. Four
cards, in this order:

1. **Plugins → Install a plugin**: `hishambt/hermes-familiar`
2. **Plugins → Enable a plugin**: `familiar-platform`
3. **Configuration → Set a setting**:
   - `FAMILIAR_TOKEN=<the token Familiar issued for this instance>` (the `_TOKEN` suffix is what routes it to
     `.env` rather than `config.yaml`)
   - `platforms.familiar.extra.url=http://localhost:3100` when Familiar is not on the default address
   - `platforms.familiar.extra.instance=homelab` to label this instance in the notifications list
4. **This machine → Restart the gateway**, which is the re-discovery a plugin needs.

From a terminal it is the same four steps:

```bash
hermes plugins install hishambt/hermes-familiar
hermes plugins enable familiar-platform
hermes config set FAMILIAR_TOKEN <the token Familiar issued for this instance>
hermes gateway restart
```

Verify with `hermes plugins doctor familiar-platform` and `hermes gateway status` (the platform appears
once a token is set). A job then gets `deliver: familiar` from Familiar's own job dialog.

### The self-check

`check.py` proves the whole contract without a Familiar to talk to: it registers the plugin in a real platform
registry, asserts that the scheduler accepts `deliver: familiar` and resolves it to `FAMILIAR_HOME_CHANNEL`,
then delivers to a stub receiver on loopback and reads what arrived (path, bearer header, payload, and how a
401, a 503 and an unreachable host each come back). Run it from the Hermes install, with its own interpreter:

```bash
cd "$LOCALAPPDATA/hermes/hermes-agent"
./venv/Scripts/python.exe "C:/Work/Personal/Familiar/hermes-familiar/check.py"
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
- **Receive.** There is no inbound path, by design: Familiar talks to an instance through the instance's own
  API, so nothing here accepts a message, carries a turn, or offers a way in.
- **Deliver with no token.** Without `FAMILIAR_TOKEN` the platform stays disabled rather than failing
  deliveries quietly, and `send()` refuses with an error the caller can read.
- **Guarantee exactly-once.** See the retry note above.

## Security

- The token is read per delivery and never logged, never echoed in an error, and never written into the
  plugin's own metadata on the instance. Errors name the status code and Familiar's response body only.
- Nothing inbound means nothing to authorize: there are no allowed users, no chat allowlist, and no commands
  reachable through this platform.
- The default address is loopback. Pointing it at a hostname means the token travels over the network, so use
  HTTPS when it is not `127.0.0.1`.
