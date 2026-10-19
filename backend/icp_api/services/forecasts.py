"""Forecast execution with full traceability.

Every request produces a ``forecast_runs`` record - including failures (model unavailable,
insufficient data, invalid scenario) - and successful runs get an immutable CSV export. A record
carries the *forecast origin* (last official ICP month), the *issue date*, the method/model
version, the dataset versions of every input, the scenario, and per-month semantics (estimate
awaiting publication vs future forecast, benchmark sources, intervals). Old records are never
rewritten when newer data or models arrive.
"""

from __future__ import annotations

import csv
import io
import logging
import time
from datetime import date
from typing import Any

from icp_api.errors import ApiError
from icp_api.metrics import FORECAST_RUNS, PREDICTION_VALUE
from icp_api.security import check_horizon
from icp_api.services.datasets import DatasetService
from icp_api.services.models import ModelService
from icp_api.storage.objects import ObjectStore
from icp_api.storage.repository import MetadataRepository, Page, new_id, utcnow
from icp_api.storage.sql import StorageError
from icp_ml.features import MissingFeatureData
from icp_ml.forecasting import ForecastInputError, make_forecast

logger = logging.getLogger(__name__)

EXPORT_COLUMNS = [
    "forecast_id",
    "issue_date",
    "forecast_origin",
    "model_name",
    "model_version",
    "method",
    "period",
    "step",
    "kind",
    "prediction_usd",
    "p50_low",
    "p50_high",
    "p80_low",
    "p80_high",
    "p95_low",
    "p95_high",
    "interval_basis",
    "conditional_on_scenario",
    "brent_usd",
    "brent_source",
    "dubai_usd",
    "dubai_source",
    "wti_usd",
    "wti_source",
    "usd_idr",
    "usd_idr_source",
    "prediction_idr",
]


class ForecastService:
    def __init__(
        self, repo: MetadataRepository, datasets: DatasetService, models: ModelService, objects: ObjectStore
    ):
        self.repo = repo
        self.datasets = datasets
        self.models = models
        self.objects = objects

    def run(
        self,
        horizon: int,
        scenario: dict[str, dict[str, float]],
        actor: str,
        request_id: str | None,
        workflow: str = "api",
        today: date | None = None,
        org_id: str | None = None,
        scenario_id: str | None = None,
        horizon_max: int = 6,
    ) -> dict[str, Any]:
        started = time.perf_counter()
        fid = new_id("fc")
        created_at = utcnow()
        scenario = {k: v for k, v in (scenario or {}).items() if v}
        record: dict[str, Any] = {
            "id": fid,
            "created_at": created_at,
            "request_id": request_id,
            "requested_by": actor,
            "horizon": horizon,
            "params": {"horizon": horizon, "scenario": scenario},
            "scenario": scenario or None,
            "model_name": self.models.model_name,
            "workflow": workflow,
            "issue_date": (today or date.today()).isoformat(),
            "org_id": org_id,
            "scenario_id": scenario_id,
        }
        try:
            check_horizon(horizon, horizon_max)
            data = self.datasets.panel()
            record.update(
                icp_dataset_version_id=data.versions["icp"]["id"],
                wti_dataset_version_id=(data.versions.get("wti") or {}).get("id"),
                dataset_versions=data.version_ids,
            )
            origin = data.panel["icp_price"].dropna().index.max()
            record.update(last_observed_period=str(origin), forecast_origin=str(origin))
            loaded = self.models.production()
            record.update(
                model_version=loaded.version, model_run_id=loaded.info.run_id, method=loaded.spec.name
            )
            fc = make_forecast(
                loaded.model, loaded.spec, data.panel, horizon, scenario, loaded.intervals, today
            )
        except ApiError as exc:
            self._record_failure(record, started, exc.code, exc.message)
            raise
        except MissingFeatureData as exc:
            msg = f"Insufficient recent data to forecast: missing {', '.join(exc.missing)}."
            self._record_failure(record, started, "DATA_INSUFFICIENT", msg)
            raise ApiError(422, "DATA_INSUFFICIENT", msg, {"missing": exc.missing}) from exc
        except ForecastInputError as exc:
            self._record_failure(record, started, "INVALID_FORECAST_INPUT", str(exc))
            raise ApiError(422, "INVALID_FORECAST_INPUT", str(exc)) from exc
        except Exception as exc:
            logger.exception("forecast failed")
            self._record_failure(record, started, "PREDICTION_FAILED", "Model inference failed.")
            raise ApiError(500, "PREDICTION_FAILED", "Model inference failed.") from exc

        result = fc.to_dict()
        result["model"] = {
            "version": loaded.version,
            "method": loaded.spec.name,
            "method_family": loaded.spec.family,
            "rule_based": loaded.spec.is_rule_based,
            "description": loaded.spec.description,
        }
        key: str | None = f"exports/forecasts/{created_at[:4]}/{created_at[5:7]}/{fid}.csv"
        try:
            self.objects.put(str(key), self._to_csv(fid, record, result), "text/csv", overwrite=False)
        except StorageError as exc:
            # The forecast itself succeeded; only the CSV export is lost (e.g. a free-tier object-store
            # write quota was exhausted). Keep the record and mark the export unavailable.
            logger.warning("forecast export not stored: %s", exc, extra={"forecast_id": fid})
            key = None
        record.update(
            status="succeeded",
            result=result,
            export_object_key=key,
            duration_ms=int((time.perf_counter() - started) * 1000),
        )
        self.repo.insert_forecast(record)
        FORECAST_RUNS.labels(status="succeeded").inc()
        for p in fc.points:
            PREDICTION_VALUE.observe(p.prediction)
        return self.get(fid)

    def _record_failure(self, record: dict[str, Any], started: float, code: str, message: str) -> None:
        record.update(
            status="failed",
            error_code=code,
            error_message=message[:500],
            duration_ms=int((time.perf_counter() - started) * 1000),
        )
        FORECAST_RUNS.labels(status="failed").inc()
        try:
            self.repo.insert_forecast(record)
        except Exception:
            logger.exception("could not record failed forecast run")

    def _to_csv(self, fid: str, record: dict[str, Any], result: dict[str, Any]) -> bytes:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=EXPORT_COLUMNS)
        writer.writeheader()
        for p in result["points"]:
            iv = p.get("intervals") or {}
            b = p["benchmarks"]
            row = {
                "forecast_id": fid,
                "issue_date": result["issue_date"],
                "forecast_origin": result["forecast_origin"],
                "model_name": self.models.model_name,
                "model_version": record.get("model_version"),
                "method": result["method"],
                "period": p["period"],
                "step": p["step"],
                "kind": p["kind"],
                "prediction_usd": p["prediction"],
                "interval_basis": p.get("interval_basis") or "",
                "conditional_on_scenario": p["conditional_on_scenario"],
                "brent_usd": b["brent"]["value"],
                "brent_source": b["brent"]["source"],
                "dubai_usd": b["dubai"]["value"],
                "dubai_source": b["dubai"]["source"],
                "wti_usd": b["wti"]["value"],
                "wti_source": b["wti"]["source"],
                "usd_idr": p.get("fx_rate"),
                "usd_idr_source": p.get("fx_source") or "",
                "prediction_idr": p.get("prediction_idr"),
            }
            for lv, key in (("0.5", "p50"), ("0.8", "p80"), ("0.95", "p95")):
                lo, hi = iv.get(lv, (None, None))
                row[f"{key}_low"], row[f"{key}_high"] = lo, hi
            writer.writerow(row)
        return buf.getvalue().encode()

    def get(self, forecast_id: str) -> dict[str, Any]:
        row = self.repo.get_forecast(forecast_id)
        if row is None:
            raise ApiError(404, "NOT_FOUND", "Forecast not found.")
        return row

    def get_visible(self, forecast_id: str, org_id: str | None, operator: bool = False) -> dict[str, Any]:
        """Tenant view of a forecast: the organisation's own runs or a published system forecast.

        Other tenants' runs and legacy anonymous rows (no organisation, not a system run) are
        reported as 404 so an id never reveals that a record exists. Operators see every run.
        """
        row = self.get(forecast_id)
        if operator or row.get("workflow") == "system":
            return row
        if org_id is None or row.get("org_id") != org_id:
            raise ApiError(404, "NOT_FOUND", "Forecast not found.")
        return row

    def list(self, limit: int, cursor: str | None, status: str | None) -> Page:
        return self.repo.list_forecasts(limit, cursor, status)

    def export(self, forecast_id: str, org_id: str | None, operator: bool = False) -> bytes:
        row = self.get_visible(forecast_id, org_id, operator)
        if row["status"] != "succeeded" or not row["export_object_key"]:
            raise ApiError(409, "EXPORT_UNAVAILABLE", "Only successful forecasts have an export.")
        return self.objects.get(row["export_object_key"])
