"""Authentication, authorization, entitlements and rate limiting (all decided server-side).

Principals
* ``public``   - anonymous visitor: public market snapshot, limited forecasts (rate limited).
* ``user``     - organisation member authenticated with a session token (``icp_sess_…``) or an
                 organisation API key (``icp_live_…``); carries org, role and plan. Roles:
                 viewer < analyst < admin < owner.
* ``operator`` - platform operator authenticated with an admin token from ``ADMIN_API_TOKENS``
                 (``name:token,...``); unlocks training, ingestion, promotion, jobs, audit and
                 tenant administration. Tokens are never logged or stored.

Entitlements come from the organisation's plan (``entitlements.py``) and are checked here, never
only in the UI. Rate limits here are per process (in memory); in deployment the Cloudflare Worker
applies the primary edge-wide limit and this layer is defence in depth.
"""

from __future__ import annotations

import hmac
import threading
import time
from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from fastapi import Request

from icp_api.entitlements import ANONYMOUS_HORIZON_MAX, Plan, has_role, plan
from icp_api.errors import ApiError
from icp_api.logging_setup import request_id_var


@dataclass(frozen=True)
class Principal:
    name: str  # actor label for audit: operator name, user id or "anonymous"
    role: str  # "public" | "viewer" | "analyst" | "admin" | "owner" | "operator"
    kind: str = "public"  # "public" | "user" | "operator"
    user_id: str | None = None
    org_id: str | None = None
    session_id: str | None = None
    api_key_id: str | None = None
    plan_code: str | None = None
    email: str | None = None
    display_name: str | None = None
    org_name: str | None = None
    must_change_password: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def is_operator(self) -> bool:
        return self.kind == "operator"

    @property
    def is_user(self) -> bool:
        return self.kind == "user"

    @property
    def plan(self) -> Plan:
        return plan(self.plan_code)

    @property
    def horizon_max(self) -> int:
        if self.is_operator:
            return 6
        if self.is_user:
            return self.plan.forecast_horizon_max
        return ANONYMOUS_HORIZON_MAX

    def as_user_dict(self) -> dict[str, Any]:
        return {
            "id": self.user_id,
            "role": self.role,
            "org_id": self.org_id,
            "email": self.email,
            "name": self.display_name,
        }


PUBLIC = Principal("anonymous", "public")


def client_key(request: Request) -> str:
    """Client identity for rate limiting. Proxy headers are trusted only from the verified gateway."""
    if getattr(request.state, "via_gateway", False):
        forwarded = request.headers.get("cf-connecting-ip") or request.headers.get("x-forwarded-for", "")
        if forwarded:
            return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


class RateLimiter:
    def __init__(self) -> None:
        self._hits: dict[tuple[str, str], deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def check(self, bucket: str, key: str, limit: int, window: float = 60.0) -> None:
        now = time.monotonic()
        with self._lock:
            if len(self._hits) > 10_000:  # bound memory: drop idle keys
                for k in [k for k, v in self._hits.items() if not v or v[-1] <= now - window]:
                    del self._hits[k]
            q = self._hits[(bucket, key)]
            while q and q[0] <= now - window:
                q.popleft()
            if len(q) >= limit:
                retry = max(1, int(window - (now - q[0])))
                raise ApiError(
                    429,
                    "RATE_LIMITED",
                    "Too many requests. Slow down and retry later.",
                    {"retry_after_seconds": retry},
                    headers={"Retry-After": str(retry)},
                )
            q.append(now)


def _bearer(request: Request) -> str | None:
    header = request.headers.get("authorization", "")
    if not header:
        return None
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise ApiError(401, "AUTH_INVALID", "Malformed Authorization header.")
    return token


def authenticate(request: Request) -> Principal:
    """Resolve the caller. Cached on ``request.state`` so several dependencies share one lookup."""
    cached = getattr(request.state, "principal", None)
    if cached is not None:
        return cached
    token = _bearer(request)
    if token is None:
        request.state.principal = PUBLIC
        return PUBLIC
    c = request.app.state.container
    principal: Principal | None = None
    if token.startswith("icp_sess_"):
        row = c.accounts.resolve_session(token)
        if row:
            principal = Principal(
                f"user:{row['user_id']}",
                row["role"],
                "user",
                user_id=row["user_id"],
                org_id=row["org_id"],
                session_id=row["id"],
                plan_code=row["plan"],
                email=row["email"],
                display_name=row["user_name"],
                org_name=row["org_name"],
                must_change_password=bool(row.get("must_change_password")),
            )
    elif token.startswith("icp_live_"):
        row = c.accounts.resolve_api_key(token)
        if row:
            principal = Principal(
                f"apikey:{row['id']}",
                "analyst",
                "user",
                org_id=row["org_id"],
                api_key_id=row["id"],
                plan_code=row["plan"],
                org_name=row["org_name"],
            )
            c.repo.record_usage(
                row["org_id"], None, row["id"], "api.request", request_id=request_id_var.get()
            )
    else:
        tokens: dict[str, str] = c.settings.admin_tokens
        for name, expected in tokens.items():
            if hmac.compare_digest(token.encode(), expected.encode()):
                principal = Principal(name, "operator", "operator", display_name=name)
    if principal is None:
        c.repo.audit(
            "unknown", "auth.failed", "denied", target=request.url.path, request_id=request_id_var.get()
        )
        raise ApiError(401, "AUTH_INVALID", "Invalid credentials.")
    request.state.principal = principal
    return principal


def require_admin(request: Request) -> Principal:
    """Platform-operator only (training, ingestion, promotion, jobs, audit, tenant administration)."""
    if not request.app.state.container.settings.admin_tokens:
        raise ApiError(
            503, "ADMIN_DISABLED", "Administrative operations are disabled: no admin tokens configured."
        )
    principal = authenticate(request)
    if not principal.is_operator:
        raise ApiError(
            401 if principal.kind == "public" else 403,
            "AUTH_REQUIRED",
            "This operation requires a platform operator token.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    limiter: RateLimiter = request.app.state.rate_limiter
    settings = request.app.state.container.settings
    if settings.rate_limit_enabled:
        limiter.check("admin", principal.name, settings.rate_limit_admin_per_minute)
    return principal


def require_user(
    minimum_role: str = "viewer", allow_operator: bool = True, allow_api_key: bool = True
) -> Callable[[Request], Principal]:
    """Signed-in organisation member with at least ``minimum_role`` (operators pass unless disallowed).

    ``allow_api_key=False`` restricts the route to interactive sessions (organisation administration).
    """

    def dependency(request: Request) -> Principal:
        principal = authenticate(request)
        if principal.is_operator and allow_operator:
            return principal
        if not principal.is_user:
            raise ApiError(
                401,
                "AUTH_REQUIRED",
                "Silakan masuk untuk menggunakan fitur ini.",
                headers={"WWW-Authenticate": "Bearer"},
            )
        if principal.api_key_id and (not allow_api_key or minimum_role in ("admin", "owner")):
            raise ApiError(403, "FORBIDDEN", "Kunci API tidak dapat mengakses administrasi organisasi.")
        if not has_role(principal.role, minimum_role):
            raise ApiError(
                403, "FORBIDDEN", f"Tindakan ini memerlukan peran {minimum_role} atau lebih tinggi."
            )
        return principal

    return dependency


def require_feature(feature: str) -> Callable[[Request], Principal]:
    """Signed-in member whose plan includes ``feature`` (a boolean field of ``Plan``)."""

    def dependency(request: Request) -> Principal:
        principal = require_user("viewer")(request)
        if principal.is_operator:
            return principal
        if not getattr(principal.plan, feature, False):
            raise ApiError(
                403,
                "FEATURE_NOT_IN_PLAN",
                f"Fitur ini tidak termasuk dalam paket {principal.plan.label_id}.",
                {"feature": feature, "plan": principal.plan.code},
            )
        return principal

    return dependency


def rate_limit(bucket: str, setting_name: str) -> Callable[[Request], None]:
    def dependency(request: Request) -> None:
        settings = request.app.state.container.settings
        if settings.rate_limit_enabled:
            request.app.state.rate_limiter.check(
                bucket, client_key(request), int(getattr(settings, setting_name))
            )

    return dependency
