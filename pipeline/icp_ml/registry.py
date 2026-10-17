"""MLflow Model Registry access: versions, aliases, candidate validation, experiment history.

The production model is identified by a registry *alias* (default ``production``); MLflow
stages are deprecated and are not used. Promotion history (who, when, why, previous version)
is recorded by the backend in the application metadata store, which enables rollback.
"""

from __future__ import annotations

import io
import json
import math
from typing import Any

import mlflow
import numpy as np
import pandas as pd
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient

from icp_ml.config import PromotionPolicy
from icp_ml.features import FEATURE_COLUMNS
from icp_ml.registry_types import ModelVersionInfo, RegistryError, ValidationCheck, ValidationReport

__all__ = [
    "ModelRegistry",
    "ModelVersionInfo",
    "RegistryError",
    "ValidationCheck",
    "ValidationReport",
]


def _signature_columns(model: Any) -> list[str] | None:
    try:
        sig = model.metadata.signature
        return list(sig.inputs.input_names()) if sig is not None else None
    except (AttributeError, MlflowException):
        return None


def _fmt(v: float | None) -> str:
    return "n/a" if v is None or (isinstance(v, float) and math.isnan(v)) else f"{v:.4f}"


class ModelRegistry:
    def __init__(self, tracking_uri: str, model_name: str, alias: str = "production"):
        self.tracking_uri = tracking_uri
        self.model_name = model_name
        self.alias = alias
        self.client = MlflowClient(tracking_uri=tracking_uri, registry_uri=tracking_uri)
        # pyfunc loading and artifact helpers resolve `models:/` and `runs:/` URIs through the
        # process-global MLflow configuration; a process serves exactly one tracking store.
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_registry_uri(tracking_uri)

    # -- reads -------------------------------------------------------------------------------
    def ping(self) -> None:
        """Cheap connectivity check against the tracking/registry store."""
        self.client.search_experiments(max_results=1)

    def _info(self, mv: Any) -> ModelVersionInfo:
        metrics: dict[str, float] = {}
        params: dict[str, str] = {}
        tags = dict(mv.tags or {})
        if mv.run_id:
            try:
                run = self.client.get_run(mv.run_id)
                metrics = dict(run.data.metrics)
                params = dict(run.data.params)
                # Run tags carry the method description for versions registered before tags were copied.
                for k, v in run.data.tags.items():
                    tags.setdefault(k, v)
            except MlflowException:
                pass
        return ModelVersionInfo(
            name=mv.name,
            version=str(mv.version),
            run_id=mv.run_id or tags.get("icp.run_id"),
            created_at_ms=int(mv.creation_timestamp or 0),
            aliases=list(getattr(mv, "aliases", []) or []),
            tags=tags,
            metrics=metrics,
            params=params,
            status=str(mv.status),
        )

    def list_versions(self) -> list[ModelVersionInfo]:
        try:
            versions = self.client.search_model_versions(f"name='{self.model_name}'")
        except MlflowException as exc:
            if "RESOURCE_DOES_NOT_EXIST" in str(exc):
                return []
            raise
        alias_map: dict[str, list[str]] = {}
        try:
            rm = self.client.get_registered_model(self.model_name)
            for alias, ver in (rm.aliases or {}).items():
                alias_map.setdefault(str(ver), []).append(alias)
        except MlflowException:
            pass
        infos = [self._info(v) for v in versions]
        for i in infos:
            i.aliases = sorted(set(i.aliases) | set(alias_map.get(i.version, [])))
        return sorted(infos, key=lambda i: int(i.version), reverse=True)

    def get_version(self, version: str) -> ModelVersionInfo:
        try:
            info = self._info(self.client.get_model_version(self.model_name, version))
        except MlflowException as exc:
            raise RegistryError(f"Model version {version} not found") from exc
        info.aliases = [a for a, v in self.aliases().items() if v == info.version]
        return info

    def aliases(self) -> dict[str, str]:
        try:
            rm = self.client.get_registered_model(self.model_name)
        except MlflowException:
            return {}
        return {a: str(v) for a, v in (rm.aliases or {}).items()}

    def production_version(self) -> str | None:
        try:
            return str(self.client.get_model_version_by_alias(self.model_name, self.alias).version)
        except MlflowException:
            return None

    def load_model(self, version: str) -> Any:
        return mlflow.pyfunc.load_model(f"models:/{self.model_name}/{version}")

    def load_model_from_run(self, run_id: str) -> Any:
        """Load a candidate's model by run (also unregistered candidates)."""
        return mlflow.pyfunc.load_model(f"runs:/{run_id}/model")

    def _text_artifact(self, run_id: str, path: str) -> str | None:
        try:
            return mlflow.artifacts.load_text(f"runs:/{run_id}/{path}")
        except (MlflowException, OSError):
            return None

    def backtest_predictions(self, run_id: str) -> pd.DataFrame | None:
        text = self._text_artifact(run_id, "evaluation/backtest_predictions.csv")
        return pd.read_csv(io.StringIO(text)) if text else None

    def holdout_predictions(self, run_id: str) -> pd.DataFrame | None:
        """One-step, observed-benchmark backtest pairs (period, actual, predicted) for charts."""
        df = self.backtest_predictions(run_id)
        if df is None:
            return None
        one = df[(df["mode"] == "observed") & (df["horizon"] == 1)]
        return pd.DataFrame(
            {"period": one["target"].astype(str), "actual": one["y_true"], "predicted": one["y_pred"]}
        ).reset_index(drop=True)

    def backtest_metrics(self, run_id: str) -> pd.DataFrame | None:
        text = self._text_artifact(run_id, "evaluation/backtest_metrics.csv")
        return pd.read_csv(io.StringIO(text)) if text else None

    def intervals(self, run_id: str) -> dict[str, Any] | None:
        text = self._text_artifact(run_id, "evaluation/intervals.json")
        return json.loads(text) if text else None

    def interval_coverage(self, run_id: str) -> dict[str, Any] | None:
        text = self._text_artifact(run_id, "evaluation/interval_coverage.json")
        return json.loads(text) if text else None

    def model_card(self, run_id: str) -> str | None:
        return self._text_artifact(run_id, "model_card.md")

    def search_runs(
        self, experiment_name: str, max_results: int = 50, page_token: str | None = None
    ) -> tuple[list[dict[str, Any]], str | None]:
        exp = self.client.get_experiment_by_name(experiment_name)
        if exp is None:
            return [], None
        page = self.client.search_runs(
            [exp.experiment_id],
            filter_string="tags.`icp.run_kind` = 'candidate'",
            max_results=max_results,
            order_by=["attributes.start_time DESC"],
            page_token=page_token,
        )
        runs = [
            {
                "run_id": r.info.run_id,
                "run_name": r.info.run_name,
                "status": r.info.status,
                "start_time_ms": r.info.start_time,
                "end_time_ms": r.info.end_time,
                "parent_run_id": r.data.tags.get("mlflow.parentRunId"),
                "training_job_id": r.data.tags.get("icp.training_job_id") or None,
                "candidate": r.data.tags.get("icp.candidate"),
                "method_family": r.data.tags.get("icp.method_family"),
                "rule_based": r.data.tags.get("icp.rule_based") == "true",
                "metrics": dict(r.data.metrics),
                "params": dict(r.data.params),
                "fit_period": r.data.tags.get("icp.fit_period"),
                "backtest_origins": r.data.tags.get("icp.backtest_origins"),
            }
            for r in page
        ]
        return runs, page.token or None

    # -- writes ------------------------------------------------------------------------------
    def set_production(self, version: str) -> None:
        self.client.set_registered_model_alias(self.model_name, self.alias, version)
        self.client.set_model_version_tag(self.model_name, version, "icp.ever_promoted", "true")

    # -- validation --------------------------------------------------------------------------
    def validate_candidate(
        self, version: str, smoke_rows: pd.DataFrame, policy: PromotionPolicy
    ) -> ValidationReport:
        report = ValidationReport(version=version, passed=False)
        checks = report.checks
        try:
            info = self.get_version(version)
        except RegistryError as exc:
            checks.append(ValidationCheck("version_exists", False, str(exc)))
            return report
        checks.append(ValidationCheck("version_exists", True, f"version {version} status={info.status}"))
        if info.status != "READY":
            checks.append(ValidationCheck("version_ready", False, f"status is {info.status}"))
            return report

        spec = info.method_spec
        checks.append(
            ValidationCheck(
                "has_method_spec",
                spec is not None,
                f"method {spec.name} ({spec.family})"
                if spec
                else "version predates the forecasting redesign (no method spec)",
            )
        )
        score = info.metrics.get("score")
        report.candidate_score = score
        has_metrics = score is not None and math.isfinite(score)
        checks.append(
            ValidationCheck(
                "has_backtest_metrics",
                has_metrics,
                f"selection score={_fmt(score)}, h1 mae={_fmt(info.metrics.get('mae'))}"
                if has_metrics
                else "no backtest score logged",
            )
        )

        try:
            model = self.load_model(version)
            cols = _signature_columns(model)
            ok = cols == FEATURE_COLUMNS
            checks.append(
                ValidationCheck(
                    "signature_matches_features",
                    ok,
                    "signature matches feature schema" if ok else f"signature {cols}",
                )
            )
            preds = np.asarray(model.predict(smoke_rows[FEATURE_COLUMNS]), dtype=float).reshape(-1)
            finite = bool(len(preds) and np.all(np.isfinite(preds)) and np.all(preds > 0))
            checks.append(
                ValidationCheck(
                    "smoke_prediction",
                    finite,
                    f"{len(preds)} predictions, range {preds.min():.2f}..{preds.max():.2f}"
                    if len(preds)
                    else "no predictions",
                )
            )
        except Exception as exc:  # noqa: BLE001 - any load/predict failure blocks promotion
            checks.append(ValidationCheck("model_loads", False, f"{type(exc).__name__}: {exc}"[:300]))

        if policy.require_beats_naive_baseline and has_metrics:
            naive = info.metrics.get("baseline_naive_last_mae")
            mae = info.metrics.get("mae")
            if naive is None or mae is None:
                checks.append(ValidationCheck("beats_naive_baseline", False, "baseline metric missing"))
            else:
                checks.append(
                    ValidationCheck(
                        "beats_naive_baseline",
                        mae < naive,
                        f"1-month MAE {mae:.4f} vs naive {naive:.4f} (observed benchmarks)",
                    )
                )
            hl, nhl = (
                info.metrics.get("hold_last_h1_mae"),
                info.metrics.get("baseline_naive_hold_last_h1_mae"),
            )
            if hl is not None and nhl is not None:
                limit = nhl * (1 + policy.max_hold_last_vs_naive_pct / 100)
                checks.append(
                    ValidationCheck(
                        "hold_last_not_worse_than_naive",
                        hl <= limit,
                        f"no-scenario 1-month MAE {hl:.4f} vs naive {nhl:.4f} (limit {limit:.4f})",
                    )
                )

        prod = self.production_version()
        report.production_version = prod
        if prod and prod != version and has_metrics:
            prod_score = self.get_version(prod).metrics.get("score")
            report.production_score = prod_score
            if prod_score is not None and math.isfinite(prod_score):
                limit = prod_score * (1 + policy.max_regression_pct / 100)
                checks.append(
                    ValidationCheck(
                        "not_worse_than_production",
                        score <= limit,  # type: ignore[operator]
                        f"score {score:.4f} vs production v{prod} {prod_score:.4f} (limit {limit:.4f}; "
                        "different data windows are not perfectly comparable)",
                    )
                )
        elif prod == version:
            checks.append(ValidationCheck("not_already_production", False, "version is already production"))

        report.passed = all(c.passed for c in checks)
        return report
