"""Liveness/readiness and system status. Reports measured dependency state, never placeholders."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future
from typing import Any, Literal, cast

from fastapi import APIRouter, Depends, Request, Response

from icp_api import __version__
from icp_api.container import Container
from icp_api.errors import ApiError
from icp_api.logging_setup import request_id_var
from icp_api.routers.common import container
from icp_api.schemas import DependencyStatus, LiveResponse, ReadinessResponse, SystemStatus, WhoAmI
from icp_api.security import authenticate, require_admin
from icp_api.storage.migrations import current_version, expected_version

router = APIRouter(tags=["health"])
CHECK_TIMEOUT = 5.0
# While the start-up warm-up is loading the model, a model check waits only this long before it reports
# "still loading" (degraded): the answer is already known, and a probe should not spend CHECK_TIMEOUT on it.
WARM_UP_WAIT = 0.5
# Every dependency check runs in its own daemon thread, never in the request thread pool, so a check that
# never returns cannot hold up process exit. A check still running from an earlier probe (a slow or stuck
# dependency, or the model warm-up) is shared by later probes of the same container instead of being
# started again, so frequent probes cannot pile up threads behind a stuck dependency.
_inflight: dict[tuple[int, str], Future[Any]] = {}
_inflight_lock = threading.Lock()


def _run(fn: Callable[[], Any]) -> Future[Any]:
    fut: Future[Any] = Future()

    def target() -> None:
        if not fut.set_running_or_notify_cancel():
            return
        try:
            fut.set_result(fn())
        except BaseException as exc:  # noqa: BLE001 - delivered to the awaiting probe
            fut.set_exception(exc)

    threading.Thread(target=target, name="health-check", daemon=True).start()
    return fut


def _start(key: tuple[int, str], fn: Callable[[], Any]) -> Future[Any]:
    with _inflight_lock:
        fut = _inflight.get(key)
        if fut is None or fut.done():
            fut = _run(fn)
            _inflight[key] = fut
        return fut


def _retrieve(fut: asyncio.Future[Any]) -> None:
    if not fut.cancelled():
        fut.exception()


async def _timed(
    owner: object,
    name: str,
    critical: bool,
    fn: Callable[[], Any],
    starting: Callable[[], bool] | None = None,
    keep: dict[str, Any] | None = None,
) -> DependencyStatus:
    """Run one check of ``owner`` (the container) with a timeout; a check still running from an earlier
    probe is awaited instead of started again. ``starting`` tells whether the start-up warm-up is still
    loading: the check then waits only ``WARM_UP_WAIT`` and a timeout is "degraded" (still loading) rather
    than "down" (a dependency that does not answer). ``keep`` receives the raw result or exception."""
    started = time.perf_counter()
    loading = starting is not None and starting()
    try:
        waiter = asyncio.wrap_future(_start((id(owner), name), fn))
        waiter.add_done_callback(_retrieve)  # a check finishing after the timeout logs no "never retrieved"
        result = await asyncio.wait_for(asyncio.shield(waiter), WARM_UP_WAIT if loading else CHECK_TIMEOUT)
        if keep is not None:
            keep["result"] = result
        detail = None if result is None else str(result)
        status = "ok"
    except _Degraded as exc:
        status, detail = "degraded", str(exc)
    except asyncio.TimeoutError:
        if loading or (starting is not None and starting()):
            status, detail = "degraded", "still loading after start-up (model warm-up in progress)"
        else:
            status, detail = "down", f"timed out after {CHECK_TIMEOUT:.0f}s"
    except ApiError as exc:
        status, detail = "down", exc.message
    except _SchemaBehind as exc:
        if keep is not None:
            keep["error"] = exc
        status, detail = "down", str(exc)
    except Exception as exc:  # noqa: BLE001 - report, never raise, from a health check
        if keep is not None:
            keep["error"] = exc
        status, detail = "down", type(exc).__name__
    return DependencyStatus(
        name=name,
        status=cast(Literal["ok", "degraded", "down", "not_configured"], status),
        critical=critical,
        detail=detail,
        latency_ms=round((time.perf_counter() - started) * 1000, 1),
    )


class _Degraded(Exception):
    pass


class _Versions(str):
    """Detail of the metadata check that also carries the schema versions it measured, so a probe that
    shares a check started by an earlier probe judges the schema from that same measurement."""

    have: int
    want: int

    def __new__(cls, detail: str, have: int, want: int) -> _Versions:
        obj = super().__new__(cls, detail)
        obj.have, obj.want = have, want
        return obj


class _SchemaBehind(RuntimeError):
    def __init__(self, have: int, want: int):
        super().__init__(f"schema version {have}, expected {want}: apply migrations")
        self.have, self.want = have, want


def _schema_status(have: int | None, want: int | None) -> DependencyStatus:
    """Non-critical: a database ahead of this code (e.g. after a code rollback) is served, since
    migrations are additive, but flagged so an operator verifies compatibility (audit v7 DATA-04).
    Judged from the versions the metadata check measured (no query of its own)."""
    status: Literal["ok", "degraded"]
    if have is None or want is None:
        status, detail = "degraded", "schema version unknown (metadata store check failed)"
    elif have > want:
        status = "degraded"
        detail = (
            f"database schema v{have} is newer than this code (expects v{want}); "
            "roll the code forward or verify the newer migrations are additive"
        )
    else:
        status, detail = "ok", f"v{have} (expected v{want})"
    return DependencyStatus(
        name="schema_version", status=status, critical=False, detail=detail, latency_ms=0.0
    )


async def run_checks(c: Container, jobs: Any = None) -> tuple[list[DependencyStatus], int | None]:
    def metadata() -> _Versions:
        c.db.ping()
        have, want = current_version(c.db), expected_version()
        if have < want:
            raise _SchemaBehind(have, want)
        return _Versions(f"{c.db.backend}, schema v{have}", have, want)

    def objects() -> str:
        c.objects.ping()
        return c.objects.backend

    def registry() -> str:
        c.models.registry.ping()
        if c.models.bundle_mode:
            return "exported bundle present (no MLflow online)"
        return "reachable"

    def model() -> str:
        st = c.models.status()
        if not st["available"]:
            raise _Degraded(st["message"])
        return f"production v{st['version']}"

    def datasets() -> str:
        missing = [d for d in ("icp", "wti") if c.repo.get_active_dataset(d) is None]
        if missing:
            raise _Degraded(f"no active version for {', '.join(missing)}")
        return "icp and wti active"

    measured: dict[str, Any] = {}

    async def metadata_then_schema() -> list[DependencyStatus]:
        first = await _timed(c, "metadata_store", True, metadata, keep=measured)
        versions = measured.get("result") or measured.get("error")
        return [first, _schema_status(getattr(versions, "have", None), getattr(versions, "want", None))]

    def warming() -> bool:
        return c.models.warming

    # Independent checks run concurrently: a probe takes as long as the slowest check (bounded by
    # CHECK_TIMEOUT), not the sum of all of them.
    meta, obj, reg, mdl, ds = await asyncio.gather(
        metadata_then_schema(),
        _timed(c, "object_store", True, objects),
        _timed(c, "model_registry" if not c.models.bundle_mode else "model_bundle", False, registry, warming),
        _timed(c, "production_model", False, model, warming),
        _timed(c, "datasets", False, datasets),
    )
    checks = [*meta, obj, reg, mdl, ds]
    checks.append(
        DependencyStatus(
            name="job_runner",
            status="ok" if jobs else "not_configured",
            critical=False,
            detail="in-process runner" if jobs else "jobs disabled in this process",
        )
    )
    versions = measured.get("result") or measured.get("error")
    return checks, getattr(versions, "have", None)


def _overall(checks: list[DependencyStatus]) -> Literal["ready", "degraded", "not_ready"]:
    if any(ch.critical and ch.status != "ok" for ch in checks):
        return "not_ready"
    if any(ch.status in ("down", "degraded") for ch in checks):
        return "degraded"
    return "ready"


@router.get("/", include_in_schema=False)
def root() -> dict[str, str]:
    # Fast response for platform health probes that check "/" (no service name or version).
    return {"status": "ok"}


@router.get("/health/live", response_model=LiveResponse)
def live() -> LiveResponse:
    return LiveResponse()


@router.get(
    "/health/ready",
    response_model=ReadinessResponse,
    responses={503: {"model": ReadinessResponse, "description": "A critical dependency is unavailable"}},
)
async def ready(request: Request, response: Response) -> ReadinessResponse:
    c = container(request)
    checks, _ = await run_checks(c, getattr(request.app.state, "jobs", None))
    status = _overall(checks)
    if status == "not_ready":
        response.status_code = 503
    # Probes and the public only learn the overall status; environment, version, check names and
    # dependency details are for platform operators (admin bearer token).
    try:
        is_operator = authenticate(request).is_operator
    except ApiError:
        is_operator = False
    if not is_operator:
        return ReadinessResponse(status=status)
    return ReadinessResponse(
        status=status, environment=c.settings.app_env, version=__version__, checks=checks
    )


@router.get("/api/v1/system/status", response_model=SystemStatus, dependencies=[Depends(require_admin)])
async def system_status(request: Request) -> SystemStatus:
    c = container(request)
    checks, schema_version = await run_checks(c, getattr(request.app.state, "jobs", None))
    return SystemStatus(
        status=_overall(checks),
        environment=c.settings.app_env,
        version=__version__,
        checks=checks,
        metadata_backend=c.db.backend,
        object_store_backend=c.objects.backend,
        schema_version=schema_version,
        expected_schema_version=expected_version(),
        model_alias=c.settings.model_alias,
        model_source=cast(Literal["mlflow", "bundle"], c.settings.model_source),
        admin_enabled=bool(c.settings.admin_tokens),
        request_id=request_id_var.get(),
    )


@router.get("/api/v1/auth/whoami", response_model=WhoAmI)
def whoami(principal: Any = Depends(authenticate)) -> WhoAmI:
    return WhoAmI(
        authenticated=principal.kind != "public",
        name=(principal.display_name or principal.name) if principal.kind != "public" else None,
        role=principal.role,
        kind=principal.kind,
        org_id=principal.org_id,
        org_name=principal.org_name,
        plan=principal.plan_code,
        must_change_password=principal.must_change_password,
        horizon_max=principal.horizon_max,
    )
