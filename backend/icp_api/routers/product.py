"""Business product surface: public market snapshot, insights, scenarios, alerts and reports."""

from __future__ import annotations

import contextlib
import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import Response
from pydantic import ValidationError

from icp_api.entitlements import has_role
from icp_api.errors import ApiError
from icp_api.logging_setup import request_id_var
from icp_api.routers.common import ERRORS, CursorQuery, LimitQuery, container
from icp_api.routers.forecasts import (
    IDEMPOTENCY_KEY_PATTERN,
    IDEMPOTENCY_WINDOW,
    check_scenario_access,
    present_one,
)
from icp_api.schemas import (
    AlertEventList,
    AlertKind,
    AlertRule,
    AlertRuleCreateRequest,
    AlertRuleUpdateRequest,
    ForecastRequest,
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
from icp_api.security import (
    Principal,
    check_feature,
    check_horizon,
    member_org_id,
    rate_limit,
    require_feature,
    require_user,
)
from icp_api.services.public_snapshot import market_snapshot as public_market_snapshot
from icp_api.storage.repository import InvalidCursor, utcnow

router = APIRouter(prefix="/api/v1", tags=["product"], responses=ERRORS)
DEFAULT_RL = Depends(rate_limit("default", "rate_limit_default_per_minute"))


def _actor(p: Principal) -> dict[str, Any]:
    return {"id": p.user_id, "role": p.role, "name": p.name}


def _org(p: Principal, request: Request) -> str:
    """The member's own organisation. Scenario and alert endpoints refuse operators (403 FORBIDDEN);
    tenant administration happens under /admin/organizations instead."""
    return member_org_id(p)


SCENARIO_WRITER = Depends(require_feature("scenarios", allow_operator=False, minimum_role="analyst"))
ALERT_WRITER = Depends(require_feature("alerts", allow_operator=False, minimum_role="analyst"))
ALERT_READER = Depends(require_feature("alerts", allow_operator=False))


# -- public snapshot --------------------------------------------------------------------------------
@router.get("/public/market-snapshot", response_model=MarketSnapshot, dependencies=[DEFAULT_RL])
def market_snapshot(request: Request) -> dict[str, Any]:
    # The same answer for every caller, memoised in-process (services/public_snapshot.py, SEC-C-01).
    c = container(request)
    return public_market_snapshot(c, c.snapshot_memo)


# -- insights ---------------------------------------------------------------------------------------
@router.get("/insights", response_model=InsightsResponse, dependencies=[DEFAULT_RL])
def insights(
    request: Request,
    lang: Literal["id", "en"] = Query("id", description="Language of every text field"),
    principal: Principal = Depends(require_feature("insights")),
) -> dict[str, Any]:
    audience = "operator" if principal.is_operator else "customer"
    return container(request).insights.generate(lang, audience)


# -- scenarios --------------------------------------------------------------------------------------
@router.get("/scenarios", response_model=ScenarioList, dependencies=[DEFAULT_RL])
def list_scenarios(
    request: Request,
    limit: int = LimitQuery,
    cursor: str | None = CursorQuery,
    include_archived: bool = Query(False),
    principal: Principal = Depends(require_feature("scenarios", allow_operator=False)),
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
    request: Request,
    body: ScenarioCreateRequest,
    principal: Principal = SCENARIO_WRITER,
) -> dict[str, Any]:
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
    request: Request,
    body: ScenarioCompareRequest,
    principal: Principal = SCENARIO_WRITER,
) -> dict[str, Any]:
    return container(request).scenarios.compare(
        _org(principal, request),
        principal.name,
        body.scenario_ids,
        request_id_var.get(),
        body.horizon,
        horizon_max=principal.horizon_max,
    )


@router.get("/scenarios/{scenario_id}", response_model=Scenario, dependencies=[DEFAULT_RL])
def get_scenario(
    request: Request,
    scenario_id: str,
    principal: Principal = Depends(require_feature("scenarios", allow_operator=False)),
) -> dict[str, Any]:
    return container(request).scenarios.get(_org(principal, request), scenario_id)


@router.post("/scenarios/{scenario_id}", response_model=Scenario, dependencies=[DEFAULT_RL])
def update_scenario(
    request: Request,
    scenario_id: str,
    body: ScenarioUpdateRequest,
    principal: Principal = SCENARIO_WRITER,
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
    request: Request,
    scenario_id: str,
    principal: Principal = SCENARIO_WRITER,
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
    request: Request,
    scenario_id: str,
    principal: Principal = SCENARIO_WRITER,
) -> ForecastRun:
    row = container(request).scenarios.run(
        _org(principal, request),
        principal.name,
        scenario_id,
        request_id_var.get(),
        horizon_max=principal.horizon_max,
    )
    return present_one(request, principal, row)


# -- alerts -----------------------------------------------------------------------------------------
@router.get("/alerts/kinds", response_model=list[AlertKind], dependencies=[DEFAULT_RL])
def alert_kinds(request: Request, _: Principal = Depends(require_user("viewer"))) -> list[dict[str, Any]]:
    return container(request).alerts.kinds()


@router.get("/alerts/rules", response_model=list[AlertRule], dependencies=[DEFAULT_RL])
def alert_rules(request: Request, principal: Principal = ALERT_READER) -> list[dict[str, Any]]:
    return container(request).alerts.list_rules(_org(principal, request))


@router.post("/alerts/rules", response_model=AlertRule, status_code=201, dependencies=[DEFAULT_RL])
def create_alert_rule(
    request: Request,
    body: AlertRuleCreateRequest,
    principal: Principal = ALERT_WRITER,
) -> dict[str, Any]:
    rule = container(request).alerts.create_rule(
        _org(principal, request), _actor(principal), body.kind, body.params, request_id_var.get()
    )
    return {**rule, "enabled": bool(rule["enabled"])}


@router.post("/alerts/rules/{rule_id}", response_model=AlertRule, dependencies=[DEFAULT_RL])
def update_alert_rule(
    request: Request,
    rule_id: str,
    body: AlertRuleUpdateRequest,
    principal: Principal = ALERT_WRITER,
) -> dict[str, Any]:
    rule = container(request).alerts.update_rule(
        _org(principal, request), _actor(principal), rule_id, body.params, body.enabled, request_id_var.get()
    )
    return {**rule, "enabled": bool(rule["enabled"])}


@router.post("/alerts/rules/{rule_id}/delete", status_code=204, dependencies=[DEFAULT_RL])
def delete_alert_rule(
    request: Request,
    rule_id: str,
    principal: Principal = ALERT_WRITER,
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
    principal: Principal = ALERT_READER,
) -> dict[str, Any]:
    try:
        return container(request).alerts.events(_org(principal, request), limit, cursor, unread_only)
    except InvalidCursor as exc:
        raise ApiError(400, "BAD_REQUEST", str(exc)) from exc


@router.post("/alerts/events/read", dependencies=[DEFAULT_RL])
def mark_alerts_read(
    request: Request,
    event_id: str | None = Query(None, max_length=64),
    principal: Principal = ALERT_READER,
) -> dict[str, Any]:
    return {"marked": container(request).alerts.mark_read(_org(principal, request), event_id), "at": utcnow()}


@router.post("/alerts/evaluate", dependencies=[DEFAULT_RL])
def evaluate_alerts(request: Request, principal: Principal = ALERT_WRITER) -> dict[str, Any]:
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
    request: Request,
    response: Response,
    body: ReportCreateRequest,
    principal: Principal = Depends(require_user("viewer")),
    idempotency_key: str | None = Header(
        None,
        alias="Idempotency-Key",
        pattern=IDEMPOTENCY_KEY_PATTERN,
        description="Optional. Repeating a request with the same key (same caller, same body, within 24 h) returns "
        "the original successful report instead of generating and counting a new one. While the first request "
        "with that key is still being generated (for example after its 504), a repeat gets 409 "
        "IDEMPOTENCY_IN_PROGRESS with Retry-After instead of a second report.",
    ),
) -> dict[str, Any]:
    formats = list(body.formats)
    params = _validated_report_params(body.kind, body.params, principal)
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
            # silently drop rather than fail: CSV remains available on every plan, and an Excel-only
            # request gets CSV instead of a "succeeded" report without any file
            formats = [f for f in formats if f != "xlsx"] or ["csv"]
    c = container(request)
    org_id = None if principal.is_operator else principal.org_id
    idempotency: dict[str, str] | None = None
    # A retry while the first request with the same key is still generating: 409 IDEMPOTENCY_IN_PROGRESS.
    reservation = (
        c.reports.reserve_idempotency(idempotency_key, principal.name, org_id)
        if idempotency_key
        else contextlib.nullcontext()
    )
    with reservation:
        if idempotency_key:
            fingerprint = hashlib.sha256(
                json.dumps(body.model_dump(mode="json"), sort_keys=True).encode("utf-8")
            ).hexdigest()
            since = (datetime.now(timezone.utc) - IDEMPOTENCY_WINDOW).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            previous = c.reports.find_idempotent(idempotency_key, principal.name, org_id, since)
            if previous is not None:
                if (previous.get("params") or {}).get("idempotency_fingerprint") != fingerprint:
                    raise ApiError(
                        409,
                        "IDEMPOTENCY_KEY_REUSED",
                        "Idempotency-Key ini sudah dipakai untuk permintaan laporan yang berbeda.",
                    )
                response.headers["Idempotent-Replayed"] = "true"
                return previous
            idempotency = {"idempotency_key": idempotency_key, "idempotency_fingerprint": fingerprint}
        return c.reports.generate(
            body.kind,
            params,
            principal.name,
            None if principal.is_operator else principal.org_id,
            request_id_var.get(),
            tuple(formats),
            horizon_max=principal.horizon_max,
            history_floor=c.datasets.history_floor(principal.plan.history_years)
            if principal.is_user and principal.plan.history_years
            else None,
            include_insights=principal.is_operator or principal.plan.insights,
            idempotency=idempotency,
        )


def _validation_error(exc: ValidationError) -> ApiError:
    issues = [
        {
            "loc": ["params", *(str(p) for p in e.get("loc", ()))],
            "msg": str(e.get("msg", "")),
            "type": str(e.get("type", "")),
        }
        for e in exc.errors()
    ]
    return ApiError(422, "VALIDATION_ERROR", "Request validation failed.", {"issues": issues})


def _validated_report_params(kind: str, params: dict[str, Any], principal: Principal) -> dict[str, Any]:
    """Validate forecast-producing report parameters exactly like the equivalent direct action.

    * ``forecast`` with ``forecast_id`` reports an existing (visible) run; otherwise ``horizon`` and
      ``scenario`` are checked like ``POST /forecasts`` (scenario: analyst + plan feature).
    * ``scenario_comparison`` is checked like ``POST /scenarios/compare`` (analyst; plan horizon is
      enforced when the comparison runs).
    """
    if kind == "forecast":
        fid = params.get("forecast_id")
        if fid is not None:
            if not isinstance(fid, str) or not fid or len(fid) > 64 or set(params) - {"forecast_id"}:
                raise ApiError(
                    422, "VALIDATION_ERROR", "params: forecast_id must be the only field and a valid id."
                )
            return {"forecast_id": fid}
        try:
            req = ForecastRequest.model_validate(
                {"horizon": params.get("horizon", 3), "scenario": params.get("scenario") or {}}
                | ({k: v for k, v in params.items() if k not in ("horizon", "scenario")})
            )
        except ValidationError as exc:
            raise _validation_error(exc) from exc
        scenario = req.scenario_inputs()
        if scenario:
            check_scenario_access(principal)
        check_horizon(req.horizon, principal.horizon_max, principal.plan_code)
        return {"horizon": req.horizon, "scenario": scenario}
    if kind == "scenario_comparison":
        if principal.is_user and not has_role(principal.role, "analyst"):
            raise ApiError(
                403, "FORBIDDEN", "Perbandingan skenario memerlukan peran analyst atau lebih tinggi."
            )
        check_feature(principal, "scenarios")
        try:
            cmp = ScenarioCompareRequest.model_validate(params)
        except ValidationError as exc:
            raise _validation_error(exc) from exc
        if cmp.horizon is not None:
            check_horizon(cmp.horizon, principal.horizon_max, principal.plan_code)
        return cmp.model_dump(exclude_none=True)
    # market_monthly / data_provenance take no parameters: nothing unvalidated is stored with the run.
    if params:
        raise ApiError(
            422,
            "VALIDATION_ERROR",
            f"params: report kind {kind} takes no parameters.",
            {
                "issues": [
                    {"loc": ["params", k], "msg": "unexpected field", "type": "extra_forbidden"}
                    for k in sorted(params)
                ][:20]
            },
        )
    return {}


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
            "Cache-Control": "private, no-store",
        },
    )
