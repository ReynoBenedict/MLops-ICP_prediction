"""Model lifecycle: production model resolution/caching, comparisons, validated promotion, rollback.

Serving never falls back to a dummy model: if the registry is unreachable, no version holds the
production alias, or the production version predates the forecasting redesign (no method spec /
interval table), inference endpoints return 503 ``MODEL_UNAVAILABLE``.

Two model sources share this service (``MODEL_SOURCE``):

* ``mlflow`` - the MLflow registry alias; training, validation, promotion and rollback work.
* ``bundle`` - an exported bundle in the object store (``icp_ml.bundle``); no MLflow is imported or
  contacted, and write operations answer ``409 OPERATION_UNAVAILABLE``. Predictions are identical
  because the same skops payload is loaded under the same trusted-type allow-list.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Protocol

import pandas as pd

from icp_api.errors import ApiError
from icp_api.services.datasets import DatasetService
from icp_api.storage.repository import MetadataRepository, Page, new_id, utcnow
from icp_ml.config import PromotionPolicy, load_config
from icp_ml.drift import drift_report
from icp_ml.features import FEATURE_COLUMNS, build_feature_frame
from icp_ml.methods import MethodSpec
from icp_ml.registry_types import ModelVersionInfo, RegistryError
from icp_ml.schema import DATASETS

if TYPE_CHECKING:
    from icp_api.storage.objects import ObjectStore

logger = logging.getLogger(__name__)
_BT = re.compile(r"^bt_(observed|hold_last)_h(\d+)_(mae|rmse|bias|smape|mase|dir_acc|n)$")


class RegistryLike(Protocol):
    """What ModelService needs from a registry (MLflow-backed or exported bundles)."""

    def ping(self) -> None: ...

    def production_version(self) -> str | None: ...

    def get_version(self, version: str) -> ModelVersionInfo: ...

    def list_versions(self) -> list[ModelVersionInfo]: ...

    def load_model(self, version: str) -> Any: ...

    def holdout_predictions(self, run_id: str) -> pd.DataFrame | None: ...

    def backtest_metrics(self, run_id: str) -> pd.DataFrame | None: ...

    def intervals(self, run_id: str) -> dict[str, Any] | None: ...

    def interval_coverage(self, run_id: str) -> dict[str, Any] | None: ...

    def model_card(self, run_id: str) -> str | None: ...

    def search_runs(
        self, experiment_name: str, max_results: int = 50, page_token: str | None = None
    ) -> tuple[list[dict[str, Any]], str | None]: ...

    def set_production(self, version: str) -> None: ...

    def validate_candidate(self, version: str, smoke_rows: pd.DataFrame, policy: PromotionPolicy) -> Any: ...


BUNDLE_MODE_MESSAGE = (
    "Operasi ini tidak tersedia pada mode bundle: validasi dan promosi model dilakukan di lingkungan "
    "operator (MLflow), lalu versi produksi diekspor sebagai bundle dengan `icp-api export-model`."
)


@dataclass
class LoadedModel:
    version: str
    model: Any
    info: ModelVersionInfo
    spec: MethodSpec
    intervals: dict[str, Any] | None
    loaded_at: float


def horizon_metrics(metrics: dict[str, float]) -> list[dict[str, Any]]:
    rows: dict[tuple[str, int], dict[str, Any]] = {}
    for k, v in metrics.items():
        m = _BT.match(k)
        if m:
            rows.setdefault((m.group(1), int(m.group(2))), {"mode": m.group(1), "horizon": int(m.group(2))})[
                m.group(3)
            ] = v
    out = []
    for (_, _), r in sorted(rows.items()):
        r["n"] = int(r.get("n", 0))
        out.append(r)
    return out


def version_summary(info: ModelVersionInfo) -> dict[str, Any]:
    m = info.metrics
    spec = info.method_spec
    return {
        "version": info.version,
        "run_id": info.run_id,
        "status": info.status,
        "aliases": info.aliases,
        "created_at_ms": info.created_at_ms,
        "candidate": info.tags.get("icp.candidate"),
        "method": spec.name if spec else None,
        "method_family": spec.family
        if spec
        else info.tags.get("icp.model_type") or info.params.get("model_type"),
        "rule_based": spec.is_rule_based if spec else None,
        "description": spec.description if spec else None,
        "benchmarks_used": spec.benchmarks_used if spec else [],
        "requires": spec.requires if spec else [],
        "legacy": spec is None,
        "metrics": {
            "score": m.get("score"),
            "mae": m.get("mae"),
            "rmse": m.get("rmse"),
            "smape": m.get("smape"),
            "baseline_naive_last_mae": m.get("baseline_naive_last_mae"),
            "baseline_naive_last_rmse": m.get("baseline_naive_last_rmse"),
            "hold_last_h1_mae": m.get("hold_last_h1_mae"),
            "baseline_naive_hold_last_h1_mae": m.get("baseline_naive_hold_last_h1_mae"),
            "by_horizon": horizon_metrics(m),
        },
        "fit_period": info.tags.get("icp.fit_period") or info.tags.get("icp.train_period"),
        "backtest_origins": info.tags.get("icp.backtest_origins"),
        "last_official_period": info.tags.get("icp.last_official_period"),
        "selection": info.tags.get("icp.selection"),
        "training_job_id": info.tags.get("icp.training_job_id") or None,
        "git_commit": info.tags.get("icp.git_commit"),
        "config_sha256": info.tags.get("icp.config_sha256"),
        "datasets": {
            ds: {
                "version_id": info.tags.get(f"icp.dataset.{ds}.version_id"),
                "sha256": info.tags.get(f"icp.dataset.{ds}.sha256"),
            }
            for ds in DATASETS
            if info.tags.get(f"icp.dataset.{ds}.version_id")
        },
    }


class ModelService:
    def __init__(
        self,
        repo: MetadataRepository,
        datasets: DatasetService,
        tracking_uri: str,
        model_name: str,
        alias: str,
        refresh_seconds: int,
        experiment_name: str,
        policy: PromotionPolicy,
        model_source: str = "mlflow",
        objects: ObjectStore | None = None,
    ):
        self.repo = repo
        self.datasets = datasets
        self.tracking_uri = tracking_uri
        self.model_name = model_name
        self.alias = alias
        self.refresh_seconds = refresh_seconds
        self.experiment_name = experiment_name
        self.policy = policy
        if model_source not in ("mlflow", "bundle"):
            raise ValueError("model_source must be mlflow or bundle")
        if model_source == "bundle" and objects is None:
            raise ValueError("bundle mode requires an object store")
        self.model_source = model_source
        self.objects = objects
        self._registry: RegistryLike | None = None
        self._loaded: LoadedModel | None = None
        self._checked_at = 0.0
        self._lock = threading.Lock()

    @classmethod
    def from_config(
        cls,
        repo: MetadataRepository,
        datasets: DatasetService,
        tracking_uri: str,
        model_name: str,
        alias: str,
        refresh_seconds: int,
        config_path: Any,
        model_source: str = "mlflow",
        objects: ObjectStore | None = None,
    ) -> ModelService:
        cfg = load_config(config_path)
        return cls(
            repo,
            datasets,
            tracking_uri,
            model_name,
            alias,
            refresh_seconds,
            cfg.experiment_name,
            cfg.promotion,
            model_source=model_source,
            objects=objects,
        )

    @property
    def bundle_mode(self) -> bool:
        return self.model_source == "bundle"

    @property
    def registry(self) -> RegistryLike:
        if self._registry is None:
            if self.bundle_mode:
                from icp_ml.bundle import BundleRegistry

                assert self.objects is not None
                self._registry = BundleRegistry(self.objects, self.model_name, self.alias)
            else:
                # Imported lazily: MLflow is heavy and is never needed in bundle mode.
                from icp_ml.registry import ModelRegistry

                self._registry = ModelRegistry(self.tracking_uri, self.model_name, self.alias)
        return self._registry

    def _require_registry_writes(self) -> None:
        if self.bundle_mode:
            raise ApiError(409, "OPERATION_UNAVAILABLE", BUNDLE_MODE_MESSAGE)

    def _unavailable(self, message: str, exc: Exception | None = None) -> ApiError:
        if exc is not None:
            logger.warning("model unavailable: %s (%s)", message, type(exc).__name__)
        return ApiError(503, "MODEL_UNAVAILABLE", message)

    # -- production model ---------------------------------------------------------------------
    def production(self, force_refresh: bool = False) -> LoadedModel:
        with self._lock:
            now = time.monotonic()
            if self._loaded and not force_refresh and now - self._checked_at < self.refresh_seconds:
                return self._loaded
            try:
                version = self.registry.production_version()
            except Exception as exc:
                if self._loaded:
                    logger.warning("registry unreachable; keeping cached model v%s", self._loaded.version)
                    return self._loaded
                raise self._unavailable("The model registry is unreachable.", exc) from exc
            self._checked_at = now
            if version is None:
                self._loaded = None
                raise self._unavailable(
                    f"No model version holds the '{self.alias}' alias. Train and promote a model first."
                )
            if self._loaded and self._loaded.version == version:
                return self._loaded
            try:
                info = self.registry.get_version(version)
            except Exception as exc:
                raise self._unavailable(
                    f"Model version {version} could not be read from the registry.", exc
                ) from exc
            spec = info.method_spec
            if spec is None:
                self._loaded = None
                raise self._unavailable(
                    f"Production model version {version} predates the forecasting redesign (no method specification). "
                    "Train and promote a new version."
                )
            try:
                model = self.registry.load_model(version)
                intervals = self.registry.intervals(info.run_id) if info.run_id else None
            except Exception as exc:
                raise self._unavailable(f"Model version {version} could not be loaded.", exc) from exc
            self._loaded = LoadedModel(version, model, info, spec, intervals, now)
            logger.info("loaded production model", extra={"model_version": version, "method": spec.name})
            return self._loaded

    def invalidate(self) -> None:
        with self._lock:
            self._checked_at = 0.0

    def status(self) -> dict[str, Any]:
        try:
            loaded = self.production()
            return {
                "available": True,
                "version": loaded.version,
                "alias": self.alias,
                "method": loaded.spec.name,
                "source": self.model_source,
            }
        except ApiError as exc:
            return {
                "available": False,
                "error_code": exc.code,
                "message": exc.message,
                "source": self.model_source,
            }

    # -- reads --------------------------------------------------------------------------------
    def _call(self, fn: Any, *args: Any) -> Any:
        try:
            return fn(*args)
        except RegistryError as exc:
            raise ApiError(404, "NOT_FOUND", str(exc)) from exc
        except ApiError:
            raise
        except Exception as exc:
            raise self._unavailable("The model registry is unreachable.", exc) from exc

    def list_versions(self) -> list[dict[str, Any]]:
        return [version_summary(v) for v in self._call(self.registry.list_versions)]

    def version_detail(self, version: str) -> dict[str, Any]:
        info = self._call(self.registry.get_version, version)
        out = version_summary(info)
        run_id = info.run_id
        holdout = self.registry.holdout_predictions(run_id) if run_id else None
        out["holdout_predictions"] = (
            [
                {"period": str(r.period), "actual": float(r.actual), "predicted": float(r.predicted)}
                for r in holdout.itertuples()
            ]
            if holdout is not None
            else []
        )
        out["params"] = info.params
        out["intervals"] = self.registry.intervals(run_id) if run_id else None
        out["interval_coverage"] = self.registry.interval_coverage(run_id) if run_id else None
        out["model_card"] = self.registry.model_card(run_id) if run_id else None
        bt = self.registry.backtest_metrics(run_id) if run_id else None
        out["backtest_metrics"] = (
            [{k: (None if pd.isna(v) else v) for k, v in r.items()} for r in bt.to_dict(orient="records")]
            if bt is not None
            else []
        )
        return out

    def experiment_runs(self, limit: int, page_token: str | None) -> tuple[list[dict[str, Any]], str | None]:
        try:
            return self._call(self.registry.search_runs, self.experiment_name, limit, page_token)
        except ApiError:
            raise
        except Exception as exc:
            raise ApiError(400, "BAD_REQUEST", "Invalid page token.") from exc

    def promotions(self, limit: int, cursor: str | None) -> Page:
        return self.repo.list_promotions(self.model_name, limit, cursor)

    # -- validation, promotion, rollback ------------------------------------------------------
    def _smoke_rows(self) -> pd.DataFrame:
        frame = build_feature_frame(self.datasets.panel().panel, ["icp_lag_1", "wti_price"])
        if frame.empty:
            raise ApiError(409, "DATA_INSUFFICIENT", "No complete feature rows available for validation.")
        return frame.tail(12)[FEATURE_COLUMNS]

    def validate(self, version: str) -> dict[str, Any]:
        self._require_registry_writes()
        rows = self._smoke_rows()
        report = self._call(self.registry.validate_candidate, version, rows, self.policy)
        return report.to_dict()

    def promote(
        self, version: str, reason: str, force: bool, actor: str, request_id: str | None
    ) -> dict[str, Any]:
        self._require_registry_writes()
        report = self.validate(version)
        current = self._call(self.registry.production_version)
        if current == version:
            raise ApiError(
                409, "ALREADY_PRODUCTION", f"Version {version} already holds the production alias."
            )
        hard = (
            "version_exists",
            "version_ready",
            "has_method_spec",
            "model_loads",
            "signature_matches_features",
            "smoke_prediction",
        )
        hard_failures = [c for c in report["checks"] if not c["passed"] and c["name"] in hard]
        if hard_failures or (not report["passed"] and not force):
            raise ApiError(
                409,
                "VALIDATION_FAILED",
                "Candidate failed promotion validation."
                + ("" if hard_failures else " Use force=true with a reason to override metric gates."),
                {"validation": report},
            )
        self._call(self.registry.set_production, version)
        self.repo.insert_promotion(
            {
                "id": new_id("prm"),
                "created_at": utcnow(),
                "model_name": self.model_name,
                "action": "promote",
                "from_version": current,
                "to_version": version,
                "reason": reason,
                "forced": force and not report["passed"],
                "validation": report,
                "actor": actor,
                "request_id": request_id,
            }
        )
        self.invalidate()
        return {
            "from_version": current,
            "to_version": version,
            "validation": report,
            "forced": force and not report["passed"],
        }

    def rollback(self, reason: str, actor: str, request_id: str | None) -> dict[str, Any]:
        self._require_registry_writes()
        current = self._call(self.registry.production_version)
        last = self.repo.latest_promotion(self.model_name)
        if last is None or last["to_version"] != current or not last["from_version"]:
            raise ApiError(
                409,
                "ROLLBACK_UNAVAILABLE",
                "No recorded previous production version to roll back to (the current alias was not "
                "set by a recorded promotion, or it was the first promotion).",
            )
        target = last["from_version"]
        try:
            self.registry.load_model(target)
        except Exception as exc:
            raise ApiError(
                409, "ROLLBACK_TARGET_INVALID", f"Version {target} can no longer be loaded."
            ) from exc
        self._call(self.registry.set_production, target)
        self.repo.insert_promotion(
            {
                "id": new_id("prm"),
                "created_at": utcnow(),
                "model_name": self.model_name,
                "action": "rollback",
                "from_version": current,
                "to_version": target,
                "reason": reason,
                "forced": False,
                "validation": None,
                "actor": actor,
                "request_id": request_id,
            }
        )
        self.invalidate()
        return {"from_version": current, "to_version": target}

    def auto_promote_if_valid(self, version: str, actor: str) -> dict[str, Any]:
        """Used by training jobs with auto_promote=true: promote only when every gate passes."""
        report = self.validate(version)
        if not report["passed"]:
            return {"promoted": False, "validation": report}
        result = self.promote(
            version, "automatic promotion after training (all gates passed)", False, actor, None
        )
        return {"promoted": True, **result}

    # -- monitoring ---------------------------------------------------------------------------
    def drift(self, recent_months: int = 12) -> dict[str, Any]:
        loaded = self.production()
        period = loaded.info.tags.get("icp.fit_period") or ""
        fit_end = period.split("..")[-1] if ".." in period else None
        if not fit_end:
            raise ApiError(409, "DRIFT_UNAVAILABLE", "The production model has no recorded fitting period.")
        frame = build_feature_frame(self.datasets.panel().panel, loaded.spec.requires)
        try:
            report = drift_report(frame, pd.Period(fit_end, freq="M"), recent_months=recent_months)
        except ValueError as exc:
            raise ApiError(409, "DRIFT_UNAVAILABLE", str(exc)) from exc
        return {"model_version": loaded.version, "method": loaded.spec.name, **asdict(report)}

    def describe_production(self) -> dict[str, Any]:
        """Compact description for forecast responses and the business UI."""
        loaded = self.production()
        return {
            "version": loaded.version,
            "method": loaded.spec.name,
            "method_family": loaded.spec.family,
            "rule_based": loaded.spec.is_rule_based,
            "description": loaded.spec.description,
            "benchmarks_used": loaded.spec.benchmarks_used,
            "metrics": json.loads(json.dumps(version_summary(loaded.info)["metrics"])),
        }
