"""Familiar platform adapter for Hermes: delivery only, over HTTP.

Familiar is a client for Hermes instances. It reads an instance's sessions and jobs over the instance's own
HTTP API, but a cron job's OUTPUT is pushed and never read: the Jobs API carries status alone. This adapter
is the push side, and it is the whole reason the plugin exists.

Registering a platform with a ``cron_deliver_env_var`` is what makes ``deliver: familiar`` a valid job
target (``cron/scheduler_delivery.py`` -> ``_is_known_delivery_platform``), and ``send()`` is the one method
the delivery path needs. So a job's output lands in Familiar instead of in a file on the instance that no
client can read.

Nothing arrives over it. Familiar talks to the instance on the instance's API, so this platform has no
inbound path and never carries a turn: ``interactive_resume`` and ``supports_async_delivery`` are both
False, and no ``platform_hint`` is set because no session ever runs here.

Settings, ``config.yaml platforms.familiar.extra.<key>`` first and the env var second (``extra`` wins):

    url           FAMILIAR_URL           default http://127.0.0.1:3100
    token         FAMILIAR_TOKEN         required: the bearer token Familiar issued for this instance
    instance      FAMILIAR_INSTANCE      optional label, defaults to the profile's name
    home_channel  FAMILIAR_HOME_CHANNEL  default "default"

Standard library only, on purpose. A missing third-party import would disable the channel silently, and a
delivery path is the last place that should be conditional. The POST runs in a worker thread, so it never
blocks the gateway's event loop.
"""

from __future__ import annotations

import asyncio
import json
import logging
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from gateway.config import Platform, PlatformConfig
from gateway.platforms.event import MessageEvent, MessageType
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms._shared import get_scoped_secret as _get_scoped_secret

logger = logging.getLogger(__name__)

DEFAULT_URL = "http://127.0.0.1:3100"
DEFAULT_TARGET = "default"
NOTIFY_PATH = "/api/hermes/notifications"
#: Where the agent's asks go: a clarify question, and a command approval. Same convention as the
#: notification path - one base URL, one path per kind of thing being delivered.
ASK_PATH = "/api/hermes/ask"
#: Where Familiar posts what a person said, and the answers to the asks above. One route for both:
#: the gateway routes a message that arrives while a run is parked to the clarify intercept.
INGRESS_PATH = "/familiar/ingress"
#: The port this adapter listens on inside the instance. The api_server holds 8642 and the dashboard
#: 8080, so the channel takes the next one.
DEFAULT_INGRESS_PORT = 8644

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


class _DeliveryError(Exception):
    """A delivery that did not land. ``retryable`` separates "try again" from "this will never work"."""

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.retryable = retryable


def _setting(extra: Dict[str, Any], key: str, env: str, default: str = "") -> str:
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


def _instance_name(extra: Dict[str, Any]) -> str:
    return _setting(extra, "instance", "FAMILIAR_INSTANCE") or _default_instance_name()


def _job_name(job_id: str) -> Optional[str]:
    """The job's own name, when this process can see the cron store.

    Best effort on purpose: a delivery must not fail because a name could not be read, and the id is already in
    the payload for a client that wants to resolve the name itself. A gateway delivers through this plugin, so
    the store is the same one the scheduler just read.
    """
    try:
        from cron.jobs import get_job

        return (get_job(job_id) or {}).get("name") or None
    except Exception:
        logger.debug("[familiar] could not read the name of job %s", job_id, exc_info=True)
        return None


def _payload(instance: str, target: str, content: str, metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
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
        "content": content[:MAX_MESSAGE_LENGTH],
        "truncated": truncated,
        "jobId": str(job_id) if job_id else None,
        "jobName": _job_name(str(job_id)) if job_id else None,
        "sentAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def _post(url: str, token: str, payload: Dict[str, Any], path: str = NOTIFY_PATH) -> Dict[str, Any]:
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
            "User-Agent": f"hermes-familiar-notify/1.0 (instance:{payload['instance']})",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as error:
        detail = ""
        try:
            detail = error.read().decode("utf-8", errors="replace")[:200]
        except Exception:
            pass
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
    choices: Optional[List[str]],
    request_id: str,
    session_key: str,
    callback_prefix: str,
) -> Dict[str, Any]:
    """What Familiar receives when the agent needs an answer.

    ``requestId`` is the thing that makes the answer land: the gateway resolves the reply through
    ``tools.clarify_gateway.resolve_gateway_clarify`` (or ``tools.approval.resolve_gateway_approval``), and
    neither can when the id is lost. ``callbackPrefix`` is the shared convention - ``cl`` for clarify,
    ``appr`` for an approval - so the client builds the same callback ids every adapter uses.
    """
    return {
        "instance": instance,
        "target": target,
        "kind": kind,
        "body": body,
        "choices": choices or [],
        "requestId": request_id,
        "sessionKey": session_key,
        "callbackPrefix": callback_prefix,
    }


def check_requirements() -> bool:
    """Always loadable: standard library only, so there is no dependency to be missing."""
    return True


def validate_config(config) -> bool:
    """True when a token is configured, in ``extra`` or the env."""
    return bool(_setting(getattr(config, "extra", {}) or {}, "token", "FAMILIAR_TOKEN"))


def is_connected(config) -> bool:
    """Configured is connected.

    The gateway enables a plugin platform when this says so (``gateway/config.py``), so it must answer the
    real question - "did the user configure this?" - or the gateway tries to deliver through a channel that
    was never set up and reports failures nobody asked for.
    """
    return validate_config(config)


class FamiliarAdapter(BasePlatformAdapter):
    """Familiar as a channel: what a person says here reaches the agent, and its asks reach them."""

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH

    # A person IS there: Familiar has an ingress now, so a prompt asking whether to resume an interrupted turn
    # lands in front of someone who can answer it. Both flags are read generically by the gateway.
    interactive_resume = True
    supports_async_delivery = False

    def __init__(self, config: PlatformConfig) -> None:
        super().__init__(config=config, platform=Platform("familiar"))
        extra = config.extra or {}
        self._url: str = _setting(extra, "url", "FAMILIAR_URL", DEFAULT_URL).rstrip("/")
        self._token: str = _setting(extra, "token", "FAMILIAR_TOKEN")
        self._instance: str = _instance_name(extra)
        self._ingress_token: str = _setting(extra, "ingress_token", "FAMILIAR_INGRESS_TOKEN", self._token)
        self._ingress_host: str = _setting(extra, "ingress_host", "FAMILIAR_INGRESS_HOST", "127.0.0.1")
        self._ingress_port: int = int(
            _setting(extra, "ingress_port", "FAMILIAR_INGRESS_PORT", str(DEFAULT_INGRESS_PORT)) or DEFAULT_INGRESS_PORT)
        self._ingress_runner: Optional[Any] = None
        # The lifted HomeChannel is the canonical store; extra and the env are the fallbacks.
        self._home: str = (
            str(getattr(config.home_channel, "chat_id", "") or "")
            or _setting(extra, "home_channel", "FAMILIAR_HOME_CHANNEL", DEFAULT_TARGET)
        )

    # -- Connection lifecycle -----------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Open the ingress: the one route Familiar posts a person's message to.

        This is the direction that makes Familiar a channel rather than a notifier. The other direction - the
        notifications and the asks - is outbound HTTP, and needs no listener at all.
        """
        if not self._token:
            logger.warning("[%s] FAMILIAR_TOKEN is not set, so deliveries would be refused", self.name)
            return False

        if not self._url:
            logger.warning("[familiar] no FAMILIAR_URL configured - outbound delivery will fail")

        from aiohttp import web

        app = web.Application()
        app.router.add_post(INGRESS_PATH, self._handle_ingress)
        app.router.add_get(INGRESS_PATH, self._handle_ingress_probe)

        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, self._ingress_host, self._ingress_port)
        try:
            await site.start()
        except OSError as error:
            logger.error("[familiar] ingress could not bind %s:%s - %s", self._ingress_host, self._ingress_port, error)
            return False

        self._ingress_runner = runner
        logger.info("[familiar] ingress listening on http://%s:%s%s", self._ingress_host, self._ingress_port, INGRESS_PATH)

        self._mark_connected()

        return True

    async def disconnect(self) -> None:
        if self._ingress_runner is not None:
            await self._ingress_runner.cleanup()
            self._ingress_runner = None

        self._mark_disconnected()

    # -- Outbound ------------------------------------------------------------

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """POST one delivery. ``chat_id`` is the job's target; the default is the instance's inbox."""
        if not self._token:
            return SendResult(success=False, error="FAMILIAR_TOKEN is not configured")

        payload = _payload(self._instance, chat_id or self._home, content, metadata)
        try:
            body = await asyncio.to_thread(_post, self._url, self._token, payload)
        except _DeliveryError as error:
            logger.warning("[%s] Delivery failed: %s", self.name, error)
            return SendResult(success=False, error=str(error), retryable=error.retryable)
        return SendResult(success=True, message_id=str(body["id"]) if body.get("id") else None)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": chat_id or self._home, "type": "dm"}


# -- Plugin registration -----------------------------------------------------


def _env_enablement() -> Optional[dict]:
    """Seed ``PlatformConfig.extra`` from env vars during gateway config load.

    ``None`` when no token is set: a platform nobody configured must not be enabled, or the gateway
    delivers into it and reports failures for a channel the user never asked for. The ``home_channel`` key
    is lifted into a ``HomeChannel`` on the ``PlatformConfig`` instead of being merged into ``extra``.
    """
    token = (_get_scoped_secret("FAMILIAR_TOKEN", "") or "").strip()
    if not token:
        return None
    seed: Dict[str, Any] = {
        "url": (_get_scoped_secret("FAMILIAR_URL", "") or DEFAULT_URL).strip().rstrip("/"),
        "token": token,
    }
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
    thread_id: Optional[str] = None,
    media_files: Optional[list] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
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


    async def _handle_ingress_probe(self, request: Any) -> Any:
        """A health probe: whether this instance can be reached at all."""
        from aiohttp import web

        return web.json_response({"ok": True, "platform": "familiar"})

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
        if not self._ingress_token or presented != f"Bearer {self._ingress_token}":
            logger.warning("[familiar] refused an ingress call: bad or missing token")
            return web.json_response({"error": "unauthorized"}, status=401)

        try:
            payload = await request.json()
        except Exception as error:  # noqa: BLE001 - a malformed body is the caller's problem, not the gateway's
            logger.warning("[familiar] ingress parse error: %s", error)
            return web.json_response({"error": "invalid payload"}, status=400)

        text = str(payload.get("text") or "").strip()
        chat_id = str(payload.get("channel") or payload.get("chatId") or self._home or DEFAULT_TARGET)
        if not text:
            return web.json_response({"error": "missing text"}, status=400)

        source = self.build_source(
            chat_id=chat_id,
            chat_name=str(payload.get("channelName") or chat_id),
            chat_type="dm",
            user_id=str(payload.get("userId") or "familiar"),
            user_name=str(payload.get("userName") or "Familiar"),
        )
        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            raw_message=payload,
            message_id=str(payload.get("messageId") or "") or None,
        )

        # Dispatched as a task, as every adapter does it: the turn can take minutes, and the caller - Familiar -
        # is waiting on a prompt HTTP response, not on the agent.
        task = asyncio.create_task(self.handle_message(event))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

        return web.json_response({"accepted": True, "messageId": event.message_id})

    async def send_clarify(
        self,
        chat_id: str,
        question: str,
        choices: Optional[List[str]] = None,
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

        payload = _ask_payload(
            self._instance, chat_id or self._home, "clarify", question, choices, clarify_id, session_key, "cl")
        try:
            await asyncio.to_thread(_post, self._url, self._token, payload, ASK_PATH)
        except _DeliveryError as error:
            logger.warning("[%s] Clarify failed: %s", self.name, error)
            return SendResult(success=False, error=str(error), retryable=error.retryable)

        return SendResult(success=True, message_id=clarify_id or None)

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

        payload = _ask_payload(
            self._instance,
            chat_id or self._home,
            "approval",
            description or command,
            kwargs.get("choices") or ["once", "deny"],
            str(kwargs.get("request_id") or ""),
            session_key,
            "appr",
        )
        try:
            await asyncio.to_thread(_post, self._url, self._token, payload, ASK_PATH)
        except _DeliveryError as error:
            logger.warning("[%s] Approval failed: %s", self.name, error)
            return SendResult(success=False, error=str(error), retryable=error.retryable)

        return SendResult(success=True, message_id=str(kwargs.get("request_id") or "") or None)


def register(ctx) -> None:
    """Plugin entry point, called once at startup."""
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
        standalone_sender_fn=_standalone_send,
        # 0 = the router does not chunk: one delivery becomes one notification, and the adapter truncates
        # rather than turning a long brief into a burst of them.
        max_message_length=0,
        emoji="🛰️",
    )
