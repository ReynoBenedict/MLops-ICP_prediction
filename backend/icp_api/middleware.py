"""ASGI middleware: request IDs, access logging + metrics, gateway verification, body limits, timeouts."""

from __future__ import annotations

import asyncio
import hmac
import logging
import re
import secrets
import time
from typing import Any

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from icp_api.errors import BodyTooLarge, error_body
from icp_api.logging_setup import request_id_var
from icp_api.metrics import HTTP_LATENCY, HTTP_REQUESTS
from icp_api.security import QUOTA_REFUND_STATUSES
from icp_api.settings import Settings
from icp_api.storage.repository import request_read_cache
from icp_api.storage.sql import request_deadline

logger = logging.getLogger("icp_api.access")
_RID = re.compile(r"^[A-Za-z0-9._:-]{8,128}$")
_EXEMPT_PREFIXES = ("/health", "/metrics")
# Interactive API docs load their own scripts/styles; every other response is JSON or a download.
_DOC_PATHS = ("/docs", "/redoc")
_API_CSP = b"default-src 'none'; frame-ancestors 'none'"
_HSTS = b"max-age=31536000; includeSubDomains"


async def _send_json(
    send: Send, status: int, body: dict[str, Any], headers: list[tuple[bytes, bytes]]
) -> None:
    import json

    raw = json.dumps(body).encode()
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(raw)).encode()),
                *headers,
            ],
        }
    )
    await send({"type": "http.response.body", "body": raw})


def gateway_secret_matches(presented: str, settings: Settings) -> bool:
    """Constant-time check against the current secret and any previous ones still accepted
    (``GATEWAY_ACCEPTED_SECRETS``, for zero-downtime rotation). Every candidate is compared."""
    if not presented:
        return False
    ok = False
    for candidate in (settings.gateway_shared_secret, *settings.gateway_accepted_secrets):
        if candidate and hmac.compare_digest(presented.encode(), candidate.encode()):
            ok = True
    return ok


def _route(scope: Scope) -> str:
    route = scope.get("route")
    return getattr(route, "path", None) or "unmatched"


class RequestContextMiddleware:
    """Assigns/propagates X-Request-ID, verifies the gateway secret, limits body size and
    request duration, emits one structured access log line and Prometheus metrics per request."""

    def __init__(self, app: ASGIApp, settings: Settings):
        self.app = app
        self.settings = settings

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = {k.decode().lower(): v.decode(errors="replace") for k, v in scope.get("headers", [])}
        incoming = headers.get("x-request-id", "")
        rid = incoming if _RID.match(incoming) else f"req_{secrets.token_hex(12)}"
        token = request_id_var.set(rid)
        started = time.perf_counter()
        status_holder = {"status": 500, "started": 0}
        rid_header = (b"x-request-id", rid.encode())
        path: str = scope.get("path", "")

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                status_holder["status"] = message["status"]
                status_holder["started"] = 1
                hdrs = list(message.get("headers", []))
                hdrs += [
                    rid_header,
                    (b"x-content-type-options", b"nosniff"),
                    (b"referrer-policy", b"no-referrer"),
                    (b"x-frame-options", b"DENY"),
                    (b"cross-origin-resource-policy", b"same-origin"),
                ]
                if not path.startswith(_DOC_PATHS):
                    hdrs.append((b"content-security-policy", _API_CSP))
                if self.settings.is_deployed:
                    hdrs.append((b"strict-transport-security", _HSTS))
                quota = scope.get("state", {}).get("quota")
                if quota:  # API-key requests: monthly quota state (security.enforce_api_quota)
                    limit, remaining, reset = quota
                    hdrs += [
                        (b"x-quota-limit", str(limit).encode()),
                        (b"x-quota-remaining", str(max(0, remaining)).encode()),
                        (b"x-quota-reset", str(reset).encode()),
                    ]
                if path.startswith("/api/") and not any(k.lower() == b"cache-control" for k, _ in hdrs):
                    hdrs.append((b"cache-control", b"no-store"))
                message["headers"] = hdrs
            await send(message)

        try:
            # Gateway verification: in deployment only the Cloudflare Worker may call /api/*.
            via_gateway = gateway_secret_matches(headers.get("x-gateway-secret", ""), self.settings)
            scope.setdefault("state", {})["via_gateway"] = via_gateway
            if (
                self.settings.require_gateway_secret
                and not via_gateway
                and not path.startswith(_EXEMPT_PREFIXES)
            ):
                status_holder["status"] = 403
                await _send_json(
                    send_wrapper,
                    403,
                    error_body("GATEWAY_REQUIRED", "Requests must come through the API gateway."),
                    [],
                )
                return

            length = headers.get("content-length")
            if length and length.isdigit() and int(length) > self.settings.max_request_bytes:
                status_holder["status"] = 413
                await _send_json(
                    send_wrapper,
                    413,
                    error_body(
                        "PAYLOAD_TOO_LARGE", f"Request body exceeds {self.settings.max_request_bytes} bytes."
                    ),
                    [],
                )
                return

            received = 0

            async def limited_receive() -> Message:
                nonlocal received
                message = await receive()
                if message["type"] == "http.request":
                    received += len(message.get("body", b""))
                    if received > self.settings.max_request_bytes:
                        raise BodyTooLarge()
                return message

            try:
                # rows that cannot change during the request are read once (storage.repository);
                # data-plane retries end with the request's own deadline (storage.sql.request_deadline)
                with request_read_cache(), request_deadline(self.settings.request_timeout_seconds):
                    await asyncio.wait_for(
                        self.app(scope, limited_receive, send_wrapper),
                        timeout=self.settings.request_timeout_seconds,
                    )
            except asyncio.TimeoutError:
                status_holder["status"] = 504
                logger.error("request timed out", extra={"path": path})
                await _send_json(
                    send_wrapper, 504, error_body("REQUEST_TIMEOUT", "The request took too long."), []
                )
            except BodyTooLarge:
                status_holder["status"] = 413
                await _send_json(
                    send_wrapper, 413, error_body("PAYLOAD_TOO_LARGE", "Request body too large."), []
                )
            except Exception:
                # Unhandled errors are answered here, inside the request context, so the 500 envelope
                # carries the request id (body and X-Request-ID) and the security headers. The
                # exception text goes to the server log only.
                logger.exception("unhandled error", extra={"path": path})
                if status_holder["started"]:
                    raise
                status_holder["status"] = 500
                await _send_json(
                    send_wrapper,
                    500,
                    error_body(
                        "INTERNAL_ERROR",
                        "An unexpected error occurred. Quote the request ID when reporting this problem.",
                    ),
                    [],
                )
        finally:
            refund = scope.get("state", {}).get("quota_refund")
            if refund is not None and status_holder["status"] in QUOTA_REFUND_STATUSES:
                try:
                    refund()  # a refused API-key request does not consume quota
                except Exception:
                    logger.exception("quota refund failed", extra={"path": path})
            elapsed = time.perf_counter() - started
            route = _route(scope)
            method = scope.get("method", "")
            HTTP_REQUESTS.labels(method=method, route=route, status=str(status_holder["status"])).inc()
            HTTP_LATENCY.labels(method=method, route=route).observe(elapsed)
            if not path.startswith(("/health/live", "/metrics")):
                logger.info(
                    "request",
                    extra={
                        "method": method,
                        "path": path,
                        "route": route,
                        "status": status_holder["status"],
                        "duration_ms": round(elapsed * 1000, 1),
                    },
                )
            request_id_var.reset(token)
