"""Business product surface: public market snapshot, insights, scenarios, alerts and reports."""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import Response

from icp_api.errors import ApiError
from icp_api.logging_setup import request_id_var
from icp_api.routers.common import ERRORS, CursorQuery, LimitQuery, container
from icp_api.routers.forecasts import to_model
from icp_api.schemas import (
    AlertEventList,
    AlertKind,
    AlertRule,
    AlertRuleCreateRequest,
    AlertRuleUpdateRequest,
    ForecastRun,
    InsightsResponse,
    MarketSnapshot,
    ReportCreateRequest,
    ReportList,
    ReportRun,
    Scenario,
    ScenarioCompareRequest,
    ScenarioCompareResponse,
    ScenarioCreateRequest,
    ScenarioList,
    ScenarioUpdateRequest,
)
from icp_api.security import Principal, authenticate, rate_limit, require_feature, require_user
from icp_api.storage.repository import InvalidCursor, utcnow

router = APIRouter(prefix="/api/v1", tags=["product"], responses=ERRORS)
DEFAULT_RL = Depends(rate_limit("default", "rate_limit_default_per_minute"))


def _actor(p: Principal) -> dict[str, Any]:
    return {"id": p.user_id, "role": p.role, "name": p.name}


def _org(p: Principal, request: Request) -> str:
    if p.is_operator:
        org = request.query_params.get("org_id")
        if not org:
            raise ApiError(422, "VALIDATION_ERROR", "Operators must specify org_id.")
        return org
    assert p.org_id is not None
    return p.org_id


# -- public snapshot --------------------------------------------------------------------------------
@router.get("/public/market-snapshot", response_model=MarketSnapshot, dependencies=[DEFAULT_RL])
def market_snapshot(request: Request) -> dict[str, Any]:
    c = container(request)
    s = c.datasets.summary()
    icp = s["datasets"].get("icp") or {}
    brent = s["datasets"].get("brent") or {}
    fx = s["datasets"].get("usd_idr") or {}
    latest = c.repo.latest_system_forecast()
    fc = None
    if latest and latest.get("result"):
        pts = latest["result"]["points"]
        fc = {
            "id": latest["id"],
            "issue_date": latest["issue_date"],
            "forecast_origin": latest["forecast_origin"],
            "method": latest["method"],
            "points": [
                {
                    "period": p["period"],
                    "kind": p["kind"],
                    "prediction": p["prediction"],
                    "interval_80": p["intervals"].get("0.8"),
                }
                for p in pts[:3]
            ],
        }
    return {
        "icp_period": icp.get("period_end"),
        "icp_usd": icp.get("latest_value"),
        "previous_period": str(__import__("pandas").Period(icp["period_end"], freq="M") - 1)
        if icp.get("previous_value") is not None
        else None,
        "previous_usd": icp.get("previous_value"),
        "mom_change_pct": icp.get("mom_change_pct"),
        "brent_usd": brent.get("latest_value"),
        "brent_period": brent.get("period_end"),
        "usd_idr": fx.get("latest_value"),
        "icp_idr": (s.get("icp_idr") or {}).get("icp_idr"),
        "freshness": icp.get("freshness") or {"status": "unknown"},
        "source": "Kementerian ESDM (ICP resmi); EIA/World Bank (acuan); Bank Indonesia JISDOR (kurs)",
        "updated_at": icp.get("updated_at"),
        "latest_forecast": fc,
    }


# -- insights ---------------------------------------------------------------------------------------
@router.get("/insights", response_model=InsightsResponse, dependencies=[DEFAULT_RL])
def insights(request: Request, principal: Principal = Depends(authenticate)) -> dict[str, Any]:
    out = container(request).insights.generate()
    if principal.kind == "public":
        out["insights"] = out["insights"][:3]
        out["disclaimer"] += " Masuk untuk melihat seluruh insight."
    return out


# -- scenarios --------------------------------------------------------------------------------------
@router.get("/scenarios", response_model=ScenarioList, dependencies=[DEFAULT_RL])
def list_scenarios(
    request: Request,
    limit: int = LimitQuery,
    cursor: str | None = CursorQuery,
    include_archived: bool = Query(False),
    principal: Principal = Depends(require_feature("scenarios")),
) -> dict[str, Any]:
    try:
        page = container(request).scenarios.list_scenarios(
            _org(principal, request), limit, cursor, include_archived
        )
    except InvalidCursor as exc:
        raise ApiError(400, "BAD_REQUEST", str(exc)) from exc
    return {"items": page.items, "next_cursor": page.next_cursor}


@router.post("/scenarios", response_model=Scenario, status_code=201, dependencies=[DEFAULT_RL])
def create_scenario(
    request: Request, body: ScenarioCreateRequest, principal: Principal = Depends(require_user("analyst"))
) -> dict[str, Any]:
    if principal.is_user and not principal.plan.scenarios:
        raise ApiError(
            403, "FEATURE_NOT_IN_PLAN", "Skenario tidak termasuk dalam paket Anda.", {"feature": "scenarios"}
        )
    return container(request).scenarios.create(
        _org(principal, request),
        _actor(principal),
        body.name,
        body.description,
        body.horizon,
        body.inputs.model_dump(),
        request_id_var.get(),
    )


@router.post(
    "/scenarios/compare",
    response_model=ScenarioCompareResponse,
    dependencies=[Depends(rate_limit("forecast", "rate_limit_forecast_per_minute"))],
)
def compare_scenarios(
    request: Request, body: ScenarioCompareRequest, principal: Principal = Depends(require_user("analyst"))
) -> dict[str, Any]:
    return container(request).scenarios.compare(
        _org(principal, request), principal.name, body.scenario_ids, request_id_var.get(), body.horizon
    )


@router.get("/scenarios/{scenario_id}", response_model=Scenario, dependencies=[DEFAULT_RL])
def get_scenario(
    request: Request, scenario_id: str, principal: Principal = Depends(require_feature("scenarios"))
) -> dict[str, Any]:
    return container(request).scenarios.get(_org(principal, request), scenario_id)


@router.post("/scenarios/{scenario_id}", response_model=Scenario, dependencies=[DEFAULT_RL])
def update_scenario(
    request: Request,
    scenario_id: str,
    body: ScenarioUpdateRequest,
    principal: Principal = Depends(require_user("analyst")),
) -> dict[str, Any]:
    return container(request).scenarios.update(
        _org(principal, request),
        _actor(principal),
        scenario_id,
        request_id_var.get(),
        name=body.name,
        description=body.description,
        horizon=body.horizon,
        inputs=body.inputs.model_dump() if body.inputs is not None else None,
    )


@router.post("/scenarios/{scenario_id}/archive", status_code=204, dependencies=[DEFAULT_RL])
def archive_scenario(
    request: Request, scenario_id: str, principal: Principal = Depends(require_user("analyst"))
) -> None:
    container(request).scenarios.archive(
        _org(principal, request), _actor(principal), scenario_id, request_id_var.get()
    )


@router.post(
    "/scenarios/{scenario_id}/run",
    response_model=ForecastRun,
    status_code=201,
    dependencies=[Depends(rate_limit("forecast", "rate_limit_forecast_per_minute"))],
)
def run_scenario(
    request: Request, scenario_id: str, principal: Principal = Depends(require_user("analyst"))
) -> ForecastRun:
    return to_model(
        container(request).scenarios.run(
            _org(principal, request), principal.name, scenario_id, request_id_var.get()
        )
    )


# -- alerts -----------------------------------------------------------------------------------------
@router.get("/alerts/kinds", response_model=list[AlertKind], dependencies=[DEFAULT_RL])
def alert_kinds(request: Request) -> list[dict[str, Any]]:
    return container(request).alerts.kinds()


@router.get("/alerts/rules", response_model=list[AlertRule], dependencies=[DEFAULT_RL])
def alert_rules(
    request: Request, principal: Principal = Depends(require_feature("alerts"))
) -> list[dict[str, Any]]:
    return container(request).alerts.list_rules(_org(principal, request))


@router.post("/alerts/rules", response_model=AlertRule, status_code=201, dependencies=[DEFAULT_RL])
def create_alert_rule(
    request: Request, body: AlertRuleCreateRequest, principal: Principal = Depends(require_user("analyst"))
) -> dict[str, Any]:
    if principal.is_user and not principal.plan.alerts:
        raise ApiError(
            403, "FEATURE_NOT_IN_PLAN", "Peringatan tidak termasuk dalam paket Anda.", {"feature": "alerts"}
        )
    rule = container(request).alerts.create_rule(
        _org(principal, request), _actor(principal), body.kind, body.params, request_id_var.get()
    )
    return {**rule, "enabled": bool(rule["enabled"])}


@router.post("/alerts/rules/{rule_id}", response_model=AlertRule, dependencies=[DEFAULT_RL])
def update_alert_rule(
    request: Request,
    rule_id: str,
    body: AlertRuleUpdateRequest,
    principal: Principal = Depends(require_user("analyst")),
) -> dict[str, Any]:
    rule = container(request).alerts.update_rule(
        _org(principal, request), _actor(principal), rule_id, body.params, body.enabled, request_id_var.get()
    )
    return {**rule, "enabled": bool(rule["enabled"])}


@router.post("/alerts/rules/{rule_id}/delete", status_code=204, dependencies=[DEFAULT_RL])
def delete_alert_rule(
    request: Request, rule_id: str, principal: Principal = Depends(require_user("analyst"))
) -> None:
    container(request).alerts.delete_rule(
        _org(principal, request), _actor(principal), rule_id, request_id_var.get()
    )


@router.get("/alerts/events", response_model=AlertEventList, dependencies=[DEFAULT_RL])
def alert_events(
    request: Request,
    limit: int = LimitQuery,
    cursor: str | None = CursorQuery,
    unread_only: bool = Query(False),
    principal: Principal = Depends(require_feature("alerts")),
) -> dict[str, Any]:
    try:
        return container(request).alerts.events(_org(principal, request), limit, cursor, unread_only)
    except InvalidCursor as exc:
        raise ApiError(400, "BAD_REQUEST", str(exc)) from exc


@router.post("/alerts/events/read", dependencies=[DEFAULT_RL])
def mark_alerts_read(
    request: Request,
    event_id: str | None = Query(None),
    principal: Principal = Depends(require_user("viewer")),
) -> dict[str, Any]:
    return {"marked": container(request).alerts.mark_read(_org(principal, request), event_id), "at": utcnow()}


@router.post("/alerts/evaluate", dependencies=[DEFAULT_RL])
def evaluate_alerts(
    request: Request, principal: Principal = Depends(require_user("analyst"))
) -> dict[str, Any]:
    return {"created": container(request).alerts.evaluate(org_id=_org(principal, request))}


# -- reports ----------------------------------------------------------------------------------------
@router.get("/reports", response_model=ReportList, dependencies=[DEFAULT_RL])
def list_reports(
    request: Request,
    limit: int = LimitQuery,
    cursor: str | None = CursorQuery,
    principal: Principal = Depends(require_user("viewer")),
) -> dict[str, Any]:
    try:
        return container(request).reports.list_reports(
            None if principal.is_operator else principal.org_id, limit, cursor
        )
    except InvalidCursor as exc:
        raise ApiError(400, "BAD_REQUEST", str(exc)) from exc


@router.post(
    "/reports",
    response_model=ReportRun,
    status_code=201,
    dependencies=[Depends(rate_limit("forecast", "rate_limit_forecast_per_minute"))],
)
def create_report(
    request: Request, body: ReportCreateRequest, principal: Principal = Depends(require_user("viewer"))
) -> dict[str, Any]:
    formats = list(body.formats)
    if principal.is_user:
        p = principal.plan
        if "pdf" in formats and not p.reports_pdf:
            raise ApiError(
                403,
                "FEATURE_NOT_IN_PLAN",
                "Laporan PDF tidak termasuk dalam paket Anda.",
                {"feature": "reports_pdf"},
            )
        if "xlsx" in formats and not p.reports_excel:
            formats.remove("xlsx")  # silently drop rather than fail: CSV remains available on every plan
    return container(request).reports.generate(
        body.kind,
        body.params,
        principal.name,
        None if principal.is_operator else principal.org_id,
        request_id_var.get(),
        tuple(formats),
    )


@router.get("/reports/{report_id}", response_model=ReportRun, dependencies=[DEFAULT_RL])
def get_report(
    request: Request, report_id: str, principal: Principal = Depends(require_user("viewer"))
) -> dict[str, Any]:
    return container(request).reports.get(report_id, principal.org_id, principal.is_operator)


@router.get(
    "/reports/{report_id}/download",
    responses={200: {"content": {"application/pdf": {}, "text/csv": {}}}},
    dependencies=[DEFAULT_RL],
)
def download_report(
    request: Request,
    report_id: str,
    format: Literal["pdf", "xlsx", "csv"] = Query("pdf"),
    principal: Principal = Depends(require_user("viewer")),
) -> Response:
    data, media, filename = container(request).reports.download(
        report_id, format, principal.org_id, principal.is_operator
    )
    return Response(
        data,
        media_type=media,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "private, max-age=86400",
        },
    )
