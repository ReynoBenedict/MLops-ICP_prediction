"""Authentication, authorization, entitlements and rate limiting (all decided server-side).

Principals
* ``public``   - anonymous visitor: plans, public market snapshot (no forecast values), sign-in.
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
import ipaddress
import threading
import time
from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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

# Requests a member holding an unchanged temporary password may still make (everything else is
# refused with 403 PASSWORD_CHANGE_REQUIRED). /plans and /public/market-snapshot never authenticate.
PASSWORD_CHANGE_ALLOWED = frozenset(
    {
        ("GET", "/api/v1/auth/whoami"),
        ("GET", "/api/v1/auth/me"),
        ("POST", "/api/v1/auth/password"),
        ("POST", "/api/v1/auth/logout"),
        ("GET", "/api/v1/auth/sessions"),
        ("POST", "/api/v1/auth/sessions/revoke-others"),
    }
)


def _auth_required(message: str = "Silakan masuk untuk menggunakan fitur ini.") -> ApiError:
    return ApiError(401, "AUTH_REQUIRED", message, headers={"WWW-Authenticate": "Bearer"})


def check_horizon(horizon: int, horizon_max: int, plan_code: str | None = None) -> None:
    """Plan horizon limit, shared by every forecast-producing path (same error as POST /forecasts)."""
    if horizon > horizon_max:
        details: dict[str, Any] = {"limit": horizon_max}
        if plan_code:
            details["plan"] = plan_code
        raise ApiError(
            403, "PLAN_LIMIT", f"Horizon maksimal untuk akses ini adalah {horizon_max} bulan.", details
        )


CLIENT_IP_HEADER = "x-icp-client-ip"


def _valid_ip(value: str) -> str | None:
    try:
        return str(ipaddress.ip_address(value.strip()))
    except ValueError:
        return None


def client_key(request: Request) -> str:
    """Client identity for rate limiting. Proxy headers are trusted only from the verified gateway.

    The Worker sends the visitor address in ``X-ICP-Client-IP`` (set from the edge's own
    ``CF-Connecting-IP`` after discarding any client copy). That dedicated header is preferred
    because Cloudflare documents that ``CF-Connecting-IP`` on a Worker subrequest to another
    Cloudflare-proxied host (Quick Tunnel, Render) is replaced by the Worker's egress address, which
    would merge every visitor into one bucket. ``CF-Connecting-IP`` / ``X-Forwarded-For`` remain a
    fallback for older gateways. Values that are not IP addresses are ignored."""
    if getattr(request.state, "via_gateway", False):
        for name in (CLIENT_IP_HEADER, "cf-connecting-ip", "x-forwarded-for"):
            raw = request.headers.get(name, "")
            ip = _valid_ip(raw.split(",")[0]) if raw else None
            if ip:
                return ip
    return request.client.host if request.client else "unknown"


class AuthFailureAudit:
    """Collapses ``auth.failed`` audit rows to one per (client, minute).

    Every request with an unknown bearer token used to write one audit row - and one metadata-store
    write - so rotating junk tokens could amplify writes without limit. The first failure of a client
    in a minute is recorded (with that fact noted in the details); further failures in the same
    minute are counted in memory only."""

    def __init__(self) -> None:
        self._seen: dict[tuple[str, int], int] = {}
        self._lock = threading.Lock()

    def should_write(self, client: str, now: float | None = None) -> bool:
        minute = int((now if now is not None else time.time()) // 60)
        with self._lock:
            if len(self._seen) > 10_000:
                for k in [k for k in self._seen if k[1] < minute]:
                    del self._seen[k]
            key = (client, minute)
            self._seen[key] = self._seen.get(key, 0) + 1
            return self._seen[key] == 1


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


class FailureThrottle:
    """Failed sign-ins, counted per (account, client) and per account in a sliding window.

    ``account`` is a hash of the normalised email, ``client`` the client address (``client_key``).
    * pair limit (``LOGIN_FAILURES_MAX``, small): one client guessing one account is stopped quickly,
      and only that client - the real owner signing in from elsewhere is unaffected.
    * account limit (``LOGIN_ACCOUNT_FAILURES_MAX``, much higher): distributed guessing from many
      addresses is still capped, but locking a known account out now takes that many failures from
      at least ``account_limit / pair_limit`` addresses instead of a handful from anywhere.
    A successful sign-in clears only that client's pair counter. Unknown emails are throttled exactly like known ones (no
    account enumeration through 429s). Counters live in process memory (single API instance)."""

    def __init__(self) -> None:
        self._account: dict[str, deque[float]] = defaultdict(deque)
        self._pair: dict[str, dict[str, deque[float]]] = defaultdict(lambda: defaultdict(deque))
        self._lock = threading.Lock()

    @staticmethod
    def _prune(q: deque[float], window: float, now: float) -> deque[float]:
        while q and q[0] <= now - window:
            q.popleft()
        return q

    def _gc(self, window: float, now: float) -> None:
        if len(self._account) <= 10_000:
            return
        for k in [k for k, v in self._account.items() if not v or v[-1] <= now - window]:
            del self._account[k]
            self._pair.pop(k, None)

    def check(self, account: str, client: str, pair_limit: int, account_limit: int, window: float) -> None:
        now = time.monotonic()
        with self._lock:
            self._gc(window, now)
            pair = self._prune(self._pair[account][client], window, now)
            acct = self._prune(self._account[account], window, now)
            for q, limit, scope in ((pair, pair_limit, "client"), (acct, account_limit, "account")):
                if len(q) >= limit:
                    retry = max(1, int(window - (now - q[0])))
                    raise ApiError(
                        429,
                        "RATE_LIMITED",
                        "Terlalu banyak percobaan masuk yang gagal. Coba lagi nanti.",
                        {"retry_after_seconds": retry, "scope": scope},
                        headers={"Retry-After": str(retry)},
                    )

    def record_failure(self, account: str, client: str) -> None:
        now = time.monotonic()
        with self._lock:
            self._account[account].append(now)
            self._pair[account][client].append(now)

    def reset(self, account: str, client: str) -> None:
        """After a successful sign-in, forget only that client's failures for the account. Other
        clients' counters and the account-wide window keep running (they expire with the window),
        so an owner signing in elsewhere never refills an attacker's guesses."""
        with self._lock:
            pairs = self._pair.get(account)
            if pairs is not None:
                pairs.pop(client, None)


def quota_period(now: datetime | None = None) -> tuple[str, str, int]:
    """Current UTC calendar month: (start timestamp, reset timestamp, seconds until reset)."""
    now = now or datetime.now(timezone.utc)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    reset = (start + timedelta(days=32)).replace(day=1)
    return (
        start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        reset.strftime("%Y-%m-%dT%H:%M:%SZ"),
        max(1, int((reset - now).total_seconds())),
    )


# Statuses after which a reserved API-key request is handed back: the request was refused (not
# authorised for the route, outside the plan, or rate limited), so it must not consume quota.
QUOTA_REFUND_STATUSES = frozenset({401, 403, 429})


def enforce_api_quota(request: Request, org_id: str, plan_code: str | None, api_key_id: str) -> None:
    """Monthly API-key quota from the plan (``api_requests_per_month``), counted from usage_events.

    Only API-key requests count and are limited; interactive sessions and operators never are. The
    check and the increment are one conditional INSERT (atomic in SQLite/D1), so concurrent requests
    cannot overshoot the limit. A request refused by the quota is not recorded; a request later
    refused by authorisation or a rate limit (``QUOTA_REFUND_STATUSES``) is handed back by the
    middleware through ``request.state.quota_refund``. Quota state is exposed to the middleware
    through ``request.state.quota`` (X-Quota-* response headers)."""
    p = plan(plan_code)
    start, reset, seconds = quota_period()
    repo = request.app.state.container.repo
    usage_id = repo.reserve_usage(
        org_id, api_key_id, "api.request", start, p.api_requests_per_month, request_id_var.get()
    )
    used = repo.count_usage(org_id, "api.request", start)
    if usage_id is None:
        raise ApiError(
            429,
            "QUOTA_EXCEEDED",
            f"Kuota API bulanan paket {p.label_id} ({p.api_requests_per_month} permintaan) sudah habis.",
            {
                "limit": p.api_requests_per_month,
                "used": used,
                "period": start[:7],
                "reset_at": reset,
                "plan": p.code,
            },
            headers={"Retry-After": str(seconds)},
        )
    request.state.quota = (p.api_requests_per_month, p.api_requests_per_month - used, reset)
    request.state.quota_refund = lambda: repo.delete_usage(usage_id)


def _bearer(request: Request) -> str | None:
    header = request.headers.get("authorization", "")
    if not header:
        return None
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise ApiError(
            401, "AUTH_INVALID", "Malformed Authorization header.", headers={"WWW-Authenticate": "Bearer"}
        )
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
            enforce_api_quota(request, row["org_id"], row["plan"], row["id"])
    else:
        tokens: dict[str, str] = c.settings.admin_tokens
        for name, expected in tokens.items():
            if hmac.compare_digest(token.encode(), expected.encode()):
                principal = Principal(name, "operator", "operator", display_name=name)
    if principal is None:
        who = client_key(request)
        if c.settings.rate_limit_enabled:
            # Invalid credentials are limited per client on every route (also those without a
            # route-level rate limit), before anything is written.
            request.app.state.rate_limiter.check("auth_failed", who, c.settings.rate_limit_default_per_minute)
        aggregator: AuthFailureAudit | None = getattr(request.app.state, "auth_failure_audit", None)
        if aggregator is None or aggregator.should_write(who):
            c.repo.audit(
                "unknown",
                "auth.failed",
                "denied",
                target=request.url.path,
                request_id=request_id_var.get(),
                details={"aggregation": "first failure of this client in the minute"},
            )
        raise ApiError(401, "AUTH_INVALID", "Invalid credentials.", headers={"WWW-Authenticate": "Bearer"})
    if principal.must_change_password and (request.method, request.url.path) not in PASSWORD_CHANGE_ALLOWED:
        raise ApiError(
            403,
            "PASSWORD_CHANGE_REQUIRED",
            "Ganti kata sandi sementara Anda sebelum menggunakan fitur lain.",
        )
    request.state.principal = principal
    return principal


def require_admin(request: Request) -> Principal:
    """Platform-operator only (training, ingestion, promotion, jobs, audit, tenant administration)."""
    if not request.app.state.container.settings.admin_tokens:
        raise ApiError(
            503, "ADMIN_DISABLED", "Administrative operations are disabled: no admin tokens configured."
        )
    principal = authenticate(request)
    if principal.kind == "public":
        raise _auth_required("This operation requires a platform operator token.")
    if not principal.is_operator:
        raise ApiError(403, "FORBIDDEN", "Tindakan ini hanya untuk operator platform.")
    limiter: RateLimiter = request.app.state.rate_limiter
    settings = request.app.state.container.settings
    if settings.rate_limit_enabled:
        # Reads (console pages, status polling) and writes (training, promotion, tenant changes) have
        # separate budgets: browsing the operator console must not exhaust the write budget, and the
        # write limit stays as strict as before.
        if request.method in ("GET", "HEAD"):
            limiter.check("admin_read", principal.name, settings.rate_limit_admin_read_per_minute)
        else:
            limiter.check("admin", principal.name, settings.rate_limit_admin_per_minute)
    return principal


def require_user(
    minimum_role: str = "viewer", allow_operator: bool = True, allow_api_key: bool = True
) -> Callable[[Request], Principal]:
    """Signed-in organisation member with at least ``minimum_role`` (operators pass unless disallowed).

    ``allow_operator=False`` marks customer-organisation endpoints: operators get 403 FORBIDDEN
    there (tenant administration lives under ``/admin/organizations``).
    ``allow_api_key=False`` restricts the route to interactive sessions (organisation administration).
    """

    def dependency(request: Request) -> Principal:
        principal = authenticate(request)
        if principal.is_operator:
            if allow_operator:
                return principal
            raise ApiError(
                403, "FORBIDDEN", "Endpoint ini khusus anggota organisasi, bukan untuk akun operator."
            )
        if not principal.is_user:
            raise _auth_required()
        if principal.api_key_id and (not allow_api_key or minimum_role in ("admin", "owner")):
            raise ApiError(403, "FORBIDDEN", "Kunci API tidak dapat mengakses administrasi organisasi.")
        if not has_role(principal.role, minimum_role):
            raise ApiError(
                403, "FORBIDDEN", f"Tindakan ini memerlukan peran {minimum_role} atau lebih tinggi."
            )
        return principal

    return dependency


def check_feature(principal: Principal, feature: str) -> None:
    """403 FEATURE_NOT_IN_PLAN unless the member's plan includes ``feature`` (operators pass)."""
    if principal.is_user and not getattr(principal.plan, feature, False):
        raise ApiError(
            403,
            "FEATURE_NOT_IN_PLAN",
            f"Fitur ini tidak termasuk dalam paket {principal.plan.label_id}.",
            {"feature": feature, "plan": principal.plan.code},
        )


def require_feature(
    feature: str, allow_operator: bool = True, minimum_role: str = "viewer"
) -> Callable[[Request], Principal]:
    """Signed-in member with at least ``minimum_role`` whose plan includes ``feature`` (a boolean
    field of ``Plan``). Used on every read *and* write path of a plan feature, so a downgraded
    organisation cannot keep using it through update/run endpoints."""

    def dependency(request: Request) -> Principal:
        principal = require_user(minimum_role, allow_operator=allow_operator)(request)
        check_feature(principal, feature)
        return principal

    return dependency


def member_org_id(principal: Principal) -> str:
    """The calling member's organisation. Raises instead of asserting (asserts vanish under -O)."""
    if not principal.is_user or not principal.org_id:
        raise ApiError(403, "FORBIDDEN", "Endpoint ini khusus anggota organisasi.")
    return principal.org_id


def member_user_id(principal: Principal) -> str:
    """The calling member's user id (sessions only; API keys have none)."""
    if not principal.is_user or not principal.user_id:
        raise ApiError(403, "FORBIDDEN", "Tindakan ini memerlukan sesi pengguna, bukan kunci API.")
    return principal.user_id


def rate_limit(bucket: str, setting_name: str) -> Callable[[Request], None]:
    def dependency(request: Request) -> None:
        settings = request.app.state.container.settings
        if settings.rate_limit_enabled:
            request.app.state.rate_limiter.check(
                bucket, client_key(request), int(getattr(settings, setting_name))
            )

    return dependency
