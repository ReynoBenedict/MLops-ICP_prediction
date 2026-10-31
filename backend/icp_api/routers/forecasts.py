"""Forecast execution, history and downloadable results."""

from __future__ import annotations

import csv
import hashlib
import io
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import Response

from icp_api.entitlements import has_role
from icp_api.errors import ApiError
from icp_api.logging_setup import request_id_var
from icp_api.routers.common import ERRORS, CursorQuery, LimitQuery, container
from icp_api.schemas import ForecastRequest, ForecastRun, ForecastRunList
from icp_api.security import Principal, rate_limit, require_user
from icp_api.services.explain import explain_run
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


def present(request: Request, principal: Principal, rows: list[dict[str, Any]]) -> list[ForecastRun]:
    """Records as shown to this caller: read-time evaluation/actuals/notes, and for customers the
    plan horizon clip (stored records are never rewritten)."""
    c = container(request)
    actuals = c.verification.by_period("icp") if rows else {}
    horizon_max = principal.horizon_max if principal.is_user else None
    out = []
    for row in rows:
        evals = c.models.evaluation_facts(row.get("model_version")) if row.get("model_version") else None
        result, clip = explain_run(row, legacy_result(row), evals, actuals, horizon_max)
        out.append(
            ForecastRun.model_validate(
                {
                    **row,
                    "result": result,
                    "plan_clip": clip,
                    "export_available": bool(row.get("export_object_key")),
                }
            )
        )
    return out


def tenant_org(principal: Principal) -> str:
    """The caller's organisation id; an explicit 403 (never an ``assert``, which ``python -O`` strips)."""
    if not principal.org_id:
        raise ApiError(403, "FORBIDDEN", "Akses ini memerlukan keanggotaan organisasi.")
    return principal.org_id


def neutralise_csv(data: bytes) -> bytes:
    """Apply the report exports' spreadsheet-formula neutralisation (``cell_safe``) to every cell of a
    forecast CSV at download time, so stored exports written before this rule are covered too."""
    from icp_api.services.reports import cell_safe

    rows = list(csv.reader(io.StringIO(data.decode("utf-8-sig"))))
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\r\n")
    for i, row in enumerate(rows):
        w.writerow(row if i == 0 else [cell_safe(v) for v in row])
    return buf.getvalue().encode()


def clip_export(data: bytes, horizon_max: int, origin: str | None = None) -> bytes:
    """Drop export rows beyond the caller's plan horizon (same rule as the JSON view).

    Fails closed (audit v7 DATA-05): without a ``step`` column the step is derived from ``period``
    and the forecast origin; a row whose step cannot be determined is dropped, never shown."""
    text = data.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        return data
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=reader.fieldnames, lineterminator="\r\n")
    w.writeheader()
    for r in reader:
        step = _export_step(r, origin)
        if step is not None and step <= horizon_max:
            w.writerow(r)
    return buf.getvalue().encode()


def _export_step(row: dict[str, Any], origin: str | None) -> int | None:
    raw = (row.get("step") or "").strip()
    if raw:
        try:
            return int(raw)
        except ValueError:
            return None
    period = (row.get("period") or "").strip()
    if not period or not origin:
        return None
    try:
        p = datetime.strptime(period[:7], "%Y-%m")
        o = datetime.strptime(origin[:7], "%Y-%m")
    except ValueError:
        return None
    return (p.year - o.year) * 12 + (p.month - o.month)


READ_TIME_COLUMNS = [
    "kind_now",
    "official_icp_usd",
    "official_basis",
    "brent_used_by_method",
    "dubai_used_by_method",
    "wti_used_by_method",
    "eval_mode",
    "eval_n",
    "eval_mae_usd",
    "baseline_naive_mae_usd",
    "eval_origin_start",
    "eval_origin_end",
    "coverage_80",
    "coverage_n",
    "coverage_warning",
]


def _cell(v: Any) -> Any:
    if isinstance(v, bool):
        return "true" if v else "false"
    return "" if v is None else v


def augment_export(data: bytes, run: ForecastRun) -> bytes:
    """Append read-time columns to the immutable stored CSV: whether ESDM has since published the
    month (official value supersedes the stored 'awaiting publication' kind), which benchmark inputs
    the method reads, and the backtest evaluation of the row's step and input situation. Rows not in
    the presented result (e.g. beyond the caller's plan horizon) are dropped."""
    text = data.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames or "period" not in reader.fieldnames or run.result is None:
        return data
    points = {p.period: p for p in run.result.points}
    fields = [*reader.fieldnames, *[c for c in READ_TIME_COLUMNS if c not in reader.fieldnames]]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fields, lineterminator="\r\n")
    w.writeheader()
    for r in reader:
        p = points.get(r["period"])
        if p is None:
            continue
        ev = p.evaluation
        extra = {
            "kind_now": p.kind_now or p.kind,
            "official_icp_usd": p.actual.value if p.actual else None,
            "official_basis": p.actual.basis if p.actual else None,
            "eval_mode": ev.mode if ev else None,
            "eval_n": ev.n if ev else None,
            "eval_mae_usd": round(ev.mae, 4) if ev and ev.mae is not None else None,
            "baseline_naive_mae_usd": round(ev.baseline.mae, 4)
            if ev and ev.baseline and ev.baseline.mae is not None
            else None,
            "eval_origin_start": ev.origin_start if ev else None,
            "eval_origin_end": ev.origin_end if ev else None,
            "coverage_80": (ev.coverage or {}).get("0.8") if ev else None,
            "coverage_n": ev.coverage_n if ev else None,
            "coverage_warning": ev.coverage_warning.id if ev and ev.coverage_warning else None,
        }
        for b in ("brent", "dubai", "wti"):
            bi = p.benchmarks.get(b)
            extra[f"{b}_used_by_method"] = bi.used_by_model if bi else None
        w.writerow({**r, **{k: _cell(v) for k, v in extra.items()}})
    return buf.getvalue().encode()


def present_one(request: Request, principal: Principal, row: dict[str, Any]) -> ForecastRun:
    return present(request, principal, [row])[0]


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


IDEMPOTENCY_KEY_PATTERN = r"^[A-Za-z0-9._:-]{8,128}$"
IDEMPOTENCY_WINDOW = timedelta(hours=24)


@router.post(
    "",
    response_model=ForecastRun,
    status_code=201,
    dependencies=[Depends(rate_limit("forecast", "rate_limit_forecast_per_minute"))],
)
def create_forecast(
    request: Request,
    response: Response,
    body: ForecastRequest,
    principal: Principal = Depends(SIGNED_IN),
    idempotency_key: str | None = Header(
        None,
        alias="Idempotency-Key",
        pattern=IDEMPOTENCY_KEY_PATTERN,
        description="Optional. Repeating a request with the same key (same caller, same body, within 24 h) returns "
        "the original successful run instead of creating and counting a new one.",
    ),
) -> ForecastRun:
    scenario = body.scenario_inputs()
    if scenario:
        check_scenario_access(principal)
    c = container(request)
    idempotency: dict[str, str] | None = None
    if idempotency_key:
        fingerprint = hashlib.sha256(
            json.dumps(body.model_dump(mode="json"), sort_keys=True).encode("utf-8")
        ).hexdigest()
        since = (datetime.now(timezone.utc) - IDEMPOTENCY_WINDOW).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        previous = c.forecasts.find_idempotent(idempotency_key, principal.name, principal.org_id, since)
        if previous is not None:
            if (previous.get("params") or {}).get("idempotency_fingerprint") != fingerprint:
                raise ApiError(
                    409,
                    "IDEMPOTENCY_KEY_REUSED",
                    "Idempotency-Key ini sudah dipakai untuk permintaan proyeksi yang berbeda.",
                )
            response.headers["Idempotent-Replayed"] = "true"
            return present_one(request, principal, previous)
        idempotency = {"idempotency_key": idempotency_key, "idempotency_fingerprint": fingerprint}
    row = c.forecasts.run(
        body.horizon,
        scenario,
        principal.name,
        request_id_var.get(),
        org_id=principal.org_id,
        horizon_max=principal.horizon_max,
        idempotency=idempotency,
    )
    if principal.org_id:
        c.repo.record_usage(
            principal.org_id,
            principal.user_id,
            principal.api_key_id,
            "forecast.run",
            request_id=request_id_var.get(),
        )
    return present_one(request, principal, row)


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
            page = c.repo.list_org_forecasts(
                tenant_org(principal), limit, cursor, status, include_system=True
            )
    except InvalidCursor as exc:
        raise ApiError(400, "BAD_REQUEST", str(exc)) from exc
    return ForecastRunList(items=present(request, principal, page.items), next_cursor=page.next_cursor)


@router.get(
    "/latest",
    response_model=ForecastRun,
    dependencies=[Depends(rate_limit("default", "rate_limit_default_per_minute"))],
)
def latest_forecast(request: Request, principal: Principal = Depends(SIGNED_IN)) -> ForecastRun:
    row = container(request).repo.latest_system_forecast()
    if row is None:
        raise ApiError(404, "NOT_FOUND", "Belum ada proyeksi sistem yang diterbitkan.")
    return present_one(request, principal, row)


@router.get(
    "/{forecast_id}",
    response_model=ForecastRun,
    dependencies=[Depends(rate_limit("default", "rate_limit_default_per_minute"))],
)
def get_forecast(
    request: Request, forecast_id: str, principal: Principal = Depends(SIGNED_IN)
) -> ForecastRun:
    row = container(request).forecasts.get_visible(forecast_id, principal.org_id, principal.is_operator)
    return present_one(request, principal, row)


@router.get(
    "/{forecast_id}/export.csv",
    responses={200: {"content": {"text/csv": {}}}},
    dependencies=[Depends(rate_limit("default", "rate_limit_default_per_minute"))],
)
def export_forecast(
    request: Request, forecast_id: str, principal: Principal = Depends(SIGNED_IN)
) -> Response:
    c = container(request)
    data = c.forecasts.export(forecast_id, principal.org_id, principal.is_operator)
    row = c.forecasts.get_visible(forecast_id, principal.org_id, principal.is_operator)
    if principal.is_user:
        origin = row.get("forecast_origin") or row.get("last_observed_period")
        data = clip_export(data, principal.horizon_max, origin)
    data = neutralise_csv(augment_export(data, present_one(request, principal, row)))
    return Response(
        data,
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="icp_forecast_{forecast_id}.csv"',
            # Tenant data: never kept by shared caches (the gateway no longer edge-caches exports).
            "Cache-Control": "private, no-store",
        },
    )
