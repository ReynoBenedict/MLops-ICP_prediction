"""Job task implementations (executed inside the worker subprocess)."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

from icp_api.container import Container
from icp_api.errors import ApiError
from icp_ml.config import load_config
from icp_ml.sources.base import HttpClient, SourceError, SourceUnavailable

logger = logging.getLogger(__name__)


class TransientJobError(RuntimeError):
    """A failure that may succeed when retried (network outage, upstream 5xx)."""


class JobInputError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _client(c: Container) -> HttpClient:
    return HttpClient(cache_dir=c.settings.source_cache_dir, min_interval_s=1.0)


def run_train(c: Container, job: dict[str, Any]) -> dict[str, Any]:
    if c.models.bundle_mode:
        raise ApiError(
            409,
            "OPERATION_UNAVAILABLE",
            "Training is unavailable in bundle mode (MODEL_SOURCE=bundle); train in the operator environment.",
        )
    # MLflow is imported here, not at module level, so job processes in bundle mode stay light.
    from icp_ml.training import DatasetRef, TrainingDataError, train_and_register

    params = job.get("params") or {}
    data = c.datasets.panel()
    tracking = c.settings.mlflow_tracking_uri
    if tracking.startswith("sqlite:///"):
        Path(tracking.removeprefix("sqlite:///")).parent.mkdir(parents=True, exist_ok=True)
    try:
        result = train_and_register(
            data.panel,
            load_config(c.settings.training_config_path),
            tracking_uri=tracking,
            artifact_root=c.settings.mlflow_artifact_root,
            datasets=[DatasetRef(name, v["id"], v["sha256"]) for name, v in data.versions.items()],
            job_id=job["id"],
        )
    except TrainingDataError as exc:
        raise JobInputError("DATA_INSUFFICIENT", str(exc)) from exc
    summary: dict[str, Any] = {
        "registered_version": result.registered_version,
        "parent_run_id": result.parent_run_id,
        "rows": result.n_rows,
        "origins": result.n_origins,
        "fit_period": result.fit_period,
        "backtest_period": result.backtest_period,
        "selection": result.selection,
        "naive": result.naive,
        "candidates": [
            {
                "name": cnd.name,
                "family": cnd.family,
                "run_id": cnd.run_id,
                "score": cnd.score,
                "rule_based": cnd.rule_based,
                **cnd.metrics,
            }
            for cnd in result.candidates
        ],
        "best": result.best.name if result.best else None,
        "datasets": data.version_ids,
    }
    if params.get("auto_promote") and result.registered_version:
        promo = c.models.auto_promote_if_valid(result.registered_version, actor=f"job:{job['id']}")
        summary["promotion"] = {"promoted": promo["promoted"], "passed": promo["validation"]["passed"]}
        c.repo.audit(
            f"job:{job['id']}",
            "model.promote.auto",
            "success" if promo["promoted"] else "failure",
            target=result.registered_version,
            request_id=job.get("request_id"),
            details=summary["promotion"],
        )
    return summary


def _ingest_results(
    c: Container, job: dict[str, Any], fetches: list[Any], allow_revisions: bool
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for f in fetches:
        res = c.datasets.ingest_fetch(
            f, actor=f"job:{job['id']}", job_id=job["id"], allow_revisions=allow_revisions
        )
        out[f.dataset] = {
            "committed": res["committed"],
            "version_id": (res.get("version") or {}).get("id"),
            "added": res["added"],
            "changed": res["changed"],
            "partial_months_excluded": res["partial_months_excluded"],
            "warnings": res["warnings"],
        }
    return out


def run_ingest_benchmarks(c: Container, job: dict[str, Any]) -> dict[str, Any]:
    """WTI + Brent (EIA / oil-prices mirror) and Dubai (World Bank Pink Sheet)."""
    from icp_ml.sources import oil_benchmarks, worldbank_cmo

    params = job.get("params") or {}
    client = _client(c)
    fetches = []
    try:
        for ds in ("wti", "brent"):
            fetches.append(
                oil_benchmarks.fetch_benchmark(
                    client, ds, start="1986-01-01", api_key=c.settings.eia_api_key or None
                )
            )
        cmo = worldbank_cmo.fetch_cmo_monthly(client)
        fetches.append(cmo["dubai"])
    except SourceUnavailable as exc:
        raise TransientJobError(str(exc)) from exc
    except SourceError as exc:
        raise JobInputError("SOURCE_ERROR", str(exc)) from exc
    return _ingest_results(c, job, fetches, bool(params.get("allow_revisions", True)))


def run_ingest_fx(c: Container, job: dict[str, Any]) -> dict[str, Any]:
    """USD/IDR monthly average and month-end from Bank Indonesia JISDOR."""
    from icp_ml.sources import bi_jisdor

    params = job.get("params") or {}
    try:
        fx = bi_jisdor.fetch_usd_idr(_client(c))
    except SourceUnavailable as exc:
        raise TransientJobError(str(exc)) from exc
    except SourceError as exc:
        raise JobInputError("SOURCE_ERROR", str(exc)) from exc
    return _ingest_results(
        c, job, [fx["usd_idr"], fx["usd_idr_eom"]], bool(params.get("allow_revisions", False))
    )


def run_ingest_icp(c: Container, job: dict[str, Any]) -> dict[str, Any]:
    """Official ICP from ESDM decrees + announcements, reconciled, verified months only."""
    from icp_api.services.icp_official import OfficialIcpIngestion

    params = job.get("params") or {}
    try:
        return OfficialIcpIngestion(c).run(
            actor=f"job:{job['id']}",
            job_id=job["id"],
            crawl_news_pages=int(params.get("news_pages", 6)),
            full_backfill=bool(params.get("full_backfill", False)),
        )
    except SourceUnavailable as exc:
        raise TransientJobError(str(exc)) from exc
    except SourceError as exc:
        raise JobInputError("SOURCE_ERROR", str(exc)) from exc


def _with_hook(fn: Any) -> Any:
    def wrapped(c: Container, job: dict[str, Any]) -> dict[str, Any]:
        return after_data_change(c, job, fn(c, job))

    return wrapped


def run_refresh_forecast(c: Container, job: dict[str, Any]) -> dict[str, Any]:
    """Publish the system forecast (shown on the public overview / landing page) from the current production model."""
    params = job.get("params") or {}
    fc = c.forecasts.run(
        int(params.get("horizon", 6)), {}, "system", job.get("request_id"), workflow="system"
    )
    return {
        "forecast_id": fc["id"],
        "forecast_origin": fc["forecast_origin"],
        "issue_date": fc["issue_date"],
        "method": fc["method"],
        "points": [
            {"period": p["period"], "kind": p["kind"], "prediction": p["prediction"]}
            for p in fc["result"]["points"]
        ],
    }


def run_evaluate_alerts(c: Container, job: dict[str, Any]) -> dict[str, Any]:
    params = job.get("params") or {}
    return {"created": c.alerts.evaluate(org_id=params.get("org_id"))}


def after_data_change(c: Container, job: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    """Post-hook for ingestion/training jobs: republish the system forecast and evaluate alert rules.

    Failures here never fail the parent job (the data/model change already happened); they are
    reported in the job result instead.
    """
    try:
        summary["system_forecast"] = run_refresh_forecast(
            c, {"params": {}, "request_id": job.get("request_id")}
        )
    except Exception as exc:  # noqa: BLE001 - reported, not raised
        summary["system_forecast"] = {"error": f"{type(exc).__name__}: {exc}"[:300]}
    try:
        summary["alerts"] = run_evaluate_alerts(c, {"params": {}})
    except Exception as exc:  # noqa: BLE001
        summary["alerts"] = {"error": f"{type(exc).__name__}: {exc}"[:300]}
    return summary


def run_selftest(c: Container, job: dict[str, Any]) -> dict[str, Any]:
    """Local/test-only job used to verify runner behaviour (timeouts, retries, cancellation)."""
    params = job.get("params") or {}
    time.sleep(float(params.get("sleep_seconds", 0)))
    mode = params.get("mode", "ok")
    if mode == "transient":
        raise TransientJobError("simulated transient failure")
    if mode == "fail":
        raise JobInputError("SELFTEST_FAILED", "simulated failure")
    return {"ok": True}


TASKS = {
    "train": _with_hook(run_train),
    "ingest_benchmarks": _with_hook(run_ingest_benchmarks),
    "ingest_fx": _with_hook(run_ingest_fx),
    "ingest_icp": _with_hook(run_ingest_icp),
    "refresh_forecast": run_refresh_forecast,
    "evaluate_alerts": run_evaluate_alerts,
    "selftest": run_selftest,
}
