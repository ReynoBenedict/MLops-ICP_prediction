"""Forecast execution, history and downloadable results."""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import Response

from icp_api.entitlements import has_role
from icp_api.errors import ApiError
from icp_api.logging_setup import request_id_var
from icp_api.routers.common import ERRORS, CursorQuery, LimitQuery, container
from icp_api.schemas import ForecastRequest, ForecastRun, ForecastRunList
from icp_api.security import Principal, rate_limit, require_user
from icp_api.storage.repository import InvalidCursor

router = APIRouter(prefix="/api/v1/forecasts", tags=["forecasts"], responses=ERRORS)
SIGNED_IN = require_user("viewer")


def check_scenario_access(principal: Principal) -> None:
    """Own scenario assumptions need analyst+ and a plan with scenarios (same rule as /scenarios)."""
    if not principal.is_user:
        return  # operators
    if not has_role(principal.role, "analyst"):
        raise ApiError(
            403, "FORBIDDEN", "Proyeksi dengan skenario memerlukan peran analyst atau lebih tinggi."
        )
    if not principal.plan.scenarios:
        raise ApiError(
            403,
            "FEATURE_NOT_IN_PLAN",
            f"Skenario tidak termasuk dalam paket {principal.plan.label_id}.",
            {"feature": "scenarios", "plan": principal.plan.code},
        )


def to_model(row: dict[str, Any]) -> ForecastRun:
    return ForecastRun.model_validate(
        {**row, "result": legacy_result(row), "export_available": bool(row.get("export_object_key"))}
    )


def legacy_result(row: dict[str, Any]) -> dict[str, Any] | None:
    """Records written before the 2026-10 redesign keep their data but are presented in the current shape."""
    res = row.get("result")
    if not res or "forecast_origin" in res:
        return res
    points = []
    for p in res.get("points", []):
        src = {"override": "scenario"}.get(p.get("wti_source"), p.get("wti_source", "unavailable"))
        iv = {"0.95": [p["lower"], p["upper"]]} if p.get("lower") is not None else {}
        points.append(
            {
                "period": p["period"],
                "step": p["step"],
                "kind": "legacy_record",
                "prediction": p["prediction"],
                "intervals": iv,
                "interval_basis": None,
                "conditional_on_scenario": src == "scenario",
                "benchmarks": {
                    "wti": {"value": p.get("wti_price"), "source": src, "used_by_model": True},
                    "brent": {"value": None, "source": "unavailable", "used_by_model": False},
                    "dubai": {"value": None, "source": "unavailable", "used_by_model": False},
                },
                "recursive": bool(p.get("recursive")),
                "prediction_idr": None,
                "fx_rate": None,
                "fx_source": None,
            }
        )
    return {
        "forecast_origin": row.get("last_observed_period") or "",
        "issue_date": (row.get("created_at") or "")[:10],
        "horizon": row["horizon"],
        "method": "legacy",
        "points": points,
        "warnings": [
            "Legacy record from the pre-2026-10 forecasting model (WTI-only, approximate intervals)."
        ],
        "interval_levels": [0.95] if any(p["intervals"] for p in points) else [],
        "interval_note": res.get("interval", ""),
        "model": None,
    }


@router.post(
    "",
    response_model=ForecastRun,
    status_code=201,
    dependencies=[Depends(rate_limit("forecast", "rate_limit_forecast_per_minute"))],
)
def create_forecast(
    request: Request, body: ForecastRequest, principal: Principal = Depends(SIGNED_IN)
) -> ForecastRun:
    scenario = body.scenario_inputs()
    if scenario:
        check_scenario_access(principal)
    c = container(request)
    row = c.forecasts.run(
        body.horizon,
        scenario,
        principal.name,
        request_id_var.get(),
        org_id=principal.org_id,
        horizon_max=principal.horizon_max,
    )
    if principal.org_id:
        c.repo.record_usage(
            principal.org_id,
            principal.user_id,
            principal.api_key_id,
            "forecast.run",
            request_id=request_id_var.get(),
        )
    return to_model(row)


@router.get(
    "",
    response_model=ForecastRunList,
    dependencies=[Depends(rate_limit("default", "rate_limit_default_per_minute"))],
)
def list_forecasts(
    request: Request,
    limit: int = LimitQuery,
    cursor: str | None = CursorQuery,
    status: Literal["succeeded", "failed"] | None = Query(None),
    principal: Principal = Depends(SIGNED_IN),
) -> ForecastRunList:
    c = container(request)
    try:
        if principal.is_operator:
            page = c.forecasts.list(limit, cursor, status)
        else:  # the organisation's own runs plus the published system forecasts
            assert principal.org_id is not None
            page = c.repo.list_org_forecasts(principal.org_id, limit, cursor, status, include_system=True)
    except InvalidCursor as exc:
        raise ApiError(400, "BAD_REQUEST", str(exc)) from exc
    return ForecastRunList(items=[to_model(r) for r in page.items], next_cursor=page.next_cursor)


@router.get(
    "/latest",
    response_model=ForecastRun,
    dependencies=[Depends(rate_limit("default", "rate_limit_default_per_minute"))],
)
def latest_forecast(request: Request, _: Principal = Depends(SIGNED_IN)) -> ForecastRun:
    row = container(request).repo.latest_system_forecast()
    if row is None:
        raise ApiError(404, "NOT_FOUND", "Belum ada proyeksi sistem yang diterbitkan.")
    return to_model(row)


@router.get(
    "/{forecast_id}",
    response_model=ForecastRun,
    dependencies=[Depends(rate_limit("default", "rate_limit_default_per_minute"))],
)
def get_forecast(
    request: Request, forecast_id: str, principal: Principal = Depends(SIGNED_IN)
) -> ForecastRun:
    row = container(request).forecasts.get_visible(forecast_id, principal.org_id, principal.is_operator)
    return to_model(row)


@router.get(
    "/{forecast_id}/export.csv",
    responses={200: {"content": {"text/csv": {}}}},
    dependencies=[Depends(rate_limit("default", "rate_limit_default_per_minute"))],
)
def export_forecast(
    request: Request, forecast_id: str, principal: Principal = Depends(SIGNED_IN)
) -> Response:
    data = container(request).forecasts.export(forecast_id, principal.org_id, principal.is_operator)
    return Response(
        data,
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="icp_forecast_{forecast_id}.csv"',
            # Tenant data: never kept by shared caches (the gateway no longer edge-caches exports).
            "Cache-Control": "private, no-store",
        },
    )
