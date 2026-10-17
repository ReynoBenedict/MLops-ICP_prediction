"""Liveness/readiness and system status. Reports measured dependency state, never placeholders."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any, Literal, cast

from fastapi import APIRouter, Depends, Request, Response
from fastapi.concurrency import run_in_threadpool

from icp_api import __version__
from icp_api.container import Container
from icp_api.errors import ApiError
from icp_api.routers.common import container
from icp_api.schemas import DependencyStatus, LiveResponse, ReadinessResponse, SystemStatus, WhoAmI
from icp_api.security import authenticate
from icp_api.storage.migrations import current_version, expected_version

router = APIRouter(tags=["health"])
CHECK_TIMEOUT = 5.0


async def _timed(name: str, critical: bool, fn: Callable[[], str | None]) -> DependencyStatus:
    started = time.perf_counter()
    try:
        detail = await asyncio.wait_for(run_in_threadpool(fn), CHECK_TIMEOUT)
        status = "ok"
    except _Degraded as exc:
        status, detail = "degraded", str(exc)
    except asyncio.TimeoutError:
        status, detail = "down", f"timed out after {CHECK_TIMEOUT:.0f}s"
    except ApiError as exc:
        status, detail = "down", exc.message
    except Exception as exc:  # noqa: BLE001 - report, never raise, from a health check
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


async def run_checks(c: Container, jobs: Any = None) -> tuple[list[DependencyStatus], int | None]:
    schema: dict[str, Any] = {"version": None}

    def metadata() -> str:
        c.db.ping()
        have, want = current_version(c.db), expected_version()
        schema["version"] = have
        if have < want:
            raise RuntimeError(f"schema version {have}, expected {want}: apply migrations")
        return f"{c.db.backend}, schema v{have}"

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

    checks = [
        await _timed("metadata_store", True, metadata),
        await _timed("object_store", True, objects),
        await _timed("model_registry" if not c.models.bundle_mode else "model_bundle", False, registry),
        await _timed("production_model", False, model),
        await _timed("datasets", False, datasets),
    ]
    checks.append(
        DependencyStatus(
            name="job_runner",
            status="ok" if jobs else "not_configured",
            critical=False,
            detail="in-process runner" if jobs else "jobs disabled in this process",
        )
    )
    return checks, schema["version"]


def _overall(checks: list[DependencyStatus]) -> Literal["ready", "degraded", "not_ready"]:
    if any(ch.critical and ch.status != "ok" for ch in checks):
        return "not_ready"
    if any(ch.status in ("down", "degraded") for ch in checks):
        return "degraded"
    return "ready"


@router.get("/", include_in_schema=False)
def root() -> dict[str, str]:
    # Fast response for platform health probes that check "/".
    return {"service": "icp-api", "status": "ok", "health": "/health/ready"}


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
    return ReadinessResponse(
        status=status, environment=c.settings.app_env, version=__version__, checks=checks
    )


@router.get("/api/v1/system/status", response_model=SystemStatus)
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
