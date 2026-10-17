"""Exported model bundles: serve a promoted model without an online MLflow server.

Training, evaluation and promotion happen where MLflow runs (the operator's machine or a CI job).
``export_bundle`` copies one registered version - the skops model plus every evaluation artifact
the API exposes - from MLflow into the object store under a stable key layout; ``BundleRegistry``
reads that layout back and offers the subset of the MLflow registry interface the backend uses
for inference and read-only views. Predictions are identical: the same skops payload MLflow
stored is loaded with the same trusted-type allow-list (``icp_ml.serialization``).

Key layout (object store, write-once except the two pointers)::

    models/production.json                       {"version", "exported_at", "source", "model_name"}
    models/bundles/index.json                    {"versions": ["5", "4", ...]}
    models/bundles/<version>/meta.json           ModelVersionInfo fields + skops_trusted_types
    models/bundles/<version>/model.skops
    models/bundles/<version>/intervals.json
    models/bundles/<version>/interval_coverage.json
    models/bundles/<version>/model_card.md
    models/bundles/<version>/backtest_metrics.csv
    models/bundles/<version>/backtest_predictions.csv

The registry has no write path except the export. Promotion, rollback and validation stay in the
operator environment and are refused by the backend in bundle mode.
"""

from __future__ import annotations

import io
import json
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Protocol

import pandas as pd

from icp_ml.registry_types import ModelVersionInfo, RegistryError
from icp_ml.serialization import check_trusted_types, load_skops

if TYPE_CHECKING:
    from icp_ml.registry import ModelRegistry

logger = logging.getLogger(__name__)

POINTER_KEY = "models/production.json"
INDEX_KEY = "models/bundles/index.json"
BUNDLE_PREFIX = "models/bundles"
MODEL_FILE = "model.skops"
META_FILE = "meta.json"
ARTIFACT_FILES = {
    "intervals.json": "evaluation/intervals.json",
    "interval_coverage.json": "evaluation/interval_coverage.json",
    "backtest_metrics.csv": "evaluation/backtest_metrics.csv",
    "backtest_predictions.csv": "evaluation/backtest_predictions.csv",
    "model_card.md": "model_card.md",
}


class BundleStore(Protocol):
    """The slice of the backend's ObjectStore protocol a bundle needs (local, S3 or gateway/KV)."""

    def put(self, key: str, data: bytes, content_type: str, overwrite: bool = False) -> None: ...

    def get(self, key: str) -> bytes: ...

    def exists(self, key: str) -> bool: ...


class BundleUnavailable(RegistryError):
    """No bundle pointer / bundle missing from the object store."""


class BundleOperationUnavailable(RegistryError):
    """Write operations (validate/promote/rollback) are not possible against exported bundles."""


def bundle_key(version: str, name: str) -> str:
    if not str(version).isdigit():
        raise ValueError(f"model version must be numeric, got {version!r}")
    return f"{BUNDLE_PREFIX}/{version}/{name}"


def _content_type(name: str) -> str:
    if name.endswith(".json"):
        return "application/json"
    if name.endswith(".csv"):
        return "text/csv"
    if name.endswith(".md"):
        return "text/markdown"
    return "application/octet-stream"


def _put(store: BundleStore, key: str, data: bytes, overwrite: bool) -> None:
    store.put(key, data, _content_type(key), overwrite=overwrite)


def _meta_from_info(info: ModelVersionInfo, trusted: list[str], exported_at: str) -> dict[str, Any]:
    meta = asdict(info)
    meta["status"] = "READY"
    meta["skops_trusted_types"] = trusted
    meta["exported_at"] = exported_at
    meta["bundle_format"] = 1
    return meta


def export_bundle(
    registry: ModelRegistry, store: BundleStore, version: str | None = None, overwrite: bool = False
) -> dict[str, Any]:
    """Copy one registered model version from MLflow into the object store and point production at it.

    Runs where MLflow is reachable (``icp-api export-model``). ``version`` defaults to the version
    holding the registry's production alias. Existing bundle files are left untouched unless
    ``overwrite`` is set; the ``production.json`` pointer and the index are always updated.
    """
    import mlflow.artifacts

    version = version or registry.production_version()
    if not version:
        raise RegistryError("No model version holds the production alias and no --version was given.")
    info = registry.get_version(version)
    if info.status != "READY":
        raise RegistryError(f"Model version {version} is not READY (status {info.status}).")
    if info.method_spec is None:
        raise RegistryError(f"Model version {version} predates the forecasting redesign (no method spec).")
    if not info.run_id:
        raise RegistryError(f"Model version {version} has no source run.")

    # The exact skops payload MLflow stored for this version (same bytes the pyfunc loader reads).
    local = mlflow.artifacts.download_artifacts(f"models:/{registry.model_name}/{version}")
    model_path = f"{local.rstrip('/')}/{MODEL_FILE}"
    try:
        with open(model_path, "rb") as fh:
            payload = fh.read()
    except OSError as exc:
        raise RegistryError(
            f"Model version {version} has no {MODEL_FILE} artifact (not skops-serialized?)"
        ) from exc
    import skops.io as sio

    trusted = check_trusted_types(sio.get_untrusted_types(data=payload))
    load_skops(payload, trusted)  # must load under the allow-list before anything is written

    exported_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    written: list[str] = []
    skipped: list[str] = []

    def write(name: str, data: bytes) -> None:
        key = bundle_key(version, name)
        if store.exists(key) and not overwrite:
            skipped.append(key)
            return
        _put(store, key, data, overwrite=True)
        written.append(key)

    write(MODEL_FILE, payload)
    for name, artifact in ARTIFACT_FILES.items():
        text = registry._text_artifact(info.run_id, artifact)
        if text is None:
            logger.warning("version %s has no artifact %s; bundle will omit it", version, artifact)
            continue
        write(name, text.encode("utf-8"))
    write(
        META_FILE, json.dumps(_meta_from_info(info, trusted, exported_at), indent=2, sort_keys=True).encode()
    )

    versions = BundleRegistry(store, registry.model_name, registry.alias).exported_versions()
    if version not in versions:
        versions = sorted({*versions, version}, key=int, reverse=True)
    _put(store, INDEX_KEY, json.dumps({"versions": versions}).encode(), overwrite=True)
    pointer = {
        "version": version,
        "exported_at": exported_at,
        "source": f"mlflow:{info.run_id}",
        "model_name": registry.model_name,
        "alias": registry.alias,
        "method": info.method_spec.name,
    }
    _put(store, POINTER_KEY, json.dumps(pointer, indent=2, sort_keys=True).encode(), overwrite=True)
    return {"version": version, "written": written, "skipped": skipped, "pointer": pointer}


class BundleRegistry:
    """Read-only registry over exported bundles (``MODEL_SOURCE=bundle``)."""

    source = "bundle"

    def __init__(self, store: BundleStore, model_name: str, alias: str = "production"):
        self.store = store
        self.model_name = model_name
        self.alias = alias
        self._meta: dict[str, dict[str, Any]] = {}

    # -- reads -------------------------------------------------------------------------------
    def _json(self, key: str) -> Any | None:
        if not self.store.exists(key):
            return None
        return json.loads(self.store.get(key).decode("utf-8"))

    def pointer(self) -> dict[str, Any] | None:
        data = self._json(POINTER_KEY)
        return dict(data) if isinstance(data, dict) else None

    def ping(self) -> None:
        if self.pointer() is None:
            raise BundleUnavailable(
                f"No exported model bundle found ({POINTER_KEY}); run `icp-api export-model` from the operator environment."
            )

    def exported_versions(self) -> list[str]:
        data = self._json(INDEX_KEY)
        versions = [str(v) for v in (data or {}).get("versions", [])] if isinstance(data, dict) else []
        return sorted({v for v in versions if v.isdigit()}, key=int, reverse=True)

    def production_version(self) -> str | None:
        ptr = self.pointer()
        version = str(ptr.get("version")) if ptr and ptr.get("version") is not None else None
        return version if version and version.isdigit() else None

    def aliases(self) -> dict[str, str]:
        version = self.production_version()
        return {self.alias: version} if version else {}

    def _meta_for(self, version: str) -> dict[str, Any]:
        if version not in self._meta:
            data = self._json(bundle_key(version, META_FILE))
            if not isinstance(data, dict):
                raise RegistryError(f"Model version {version} not found")
            self._meta[version] = data
        return self._meta[version]

    def get_version(self, version: str) -> ModelVersionInfo:
        if not str(version).isdigit():
            raise RegistryError(f"Model version {version} not found")
        m = self._meta_for(str(version))
        return ModelVersionInfo(
            name=str(m.get("name") or self.model_name),
            version=str(m.get("version") or version),
            run_id=m.get("run_id"),
            created_at_ms=int(m.get("created_at_ms") or 0),
            aliases=[a for a, v in self.aliases().items() if v == str(version)],
            tags={str(k): str(v) for k, v in (m.get("tags") or {}).items()},
            metrics={str(k): float(v) for k, v in (m.get("metrics") or {}).items()},
            params={str(k): str(v) for k, v in (m.get("params") or {}).items()},
            status=str(m.get("status") or "READY"),
        )

    def list_versions(self) -> list[ModelVersionInfo]:
        out = []
        for v in self.exported_versions():
            try:
                out.append(self.get_version(v))
            except RegistryError:
                logger.warning("bundle index lists version %s but its meta.json is missing", v)
        return out

    def load_model(self, version: str) -> Any:
        meta = self._meta_for(str(version))
        key = bundle_key(str(version), MODEL_FILE)
        if not self.store.exists(key):
            raise RegistryError(f"Model version {version} has no bundle payload ({key})")
        return load_skops(self.store.get(key), list(meta.get("skops_trusted_types") or []))

    def _version_for_run(self, run_id: str) -> str | None:
        for v, m in self._meta.items():
            if m.get("run_id") == run_id:
                return v
        for v in self.exported_versions():
            try:
                if self._meta_for(v).get("run_id") == run_id:
                    return v
            except RegistryError:
                continue
        return None

    def _text_artifact(self, run_id: str, name: str) -> str | None:
        version = self._version_for_run(run_id)
        if version is None:
            return None
        key = bundle_key(version, name)
        return self.store.get(key).decode("utf-8") if self.store.exists(key) else None

    def backtest_predictions(self, run_id: str) -> pd.DataFrame | None:
        text = self._text_artifact(run_id, "backtest_predictions.csv")
        return pd.read_csv(io.StringIO(text)) if text else None

    def holdout_predictions(self, run_id: str) -> pd.DataFrame | None:
        df = self.backtest_predictions(run_id)
        if df is None:
            return None
        one = df[(df["mode"] == "observed") & (df["horizon"] == 1)]
        return pd.DataFrame(
            {"period": one["target"].astype(str), "actual": one["y_true"], "predicted": one["y_pred"]}
        ).reset_index(drop=True)

    def backtest_metrics(self, run_id: str) -> pd.DataFrame | None:
        text = self._text_artifact(run_id, "backtest_metrics.csv")
        return pd.read_csv(io.StringIO(text)) if text else None

    def intervals(self, run_id: str) -> dict[str, Any] | None:
        text = self._text_artifact(run_id, "intervals.json")
        return json.loads(text) if text else None

    def interval_coverage(self, run_id: str) -> dict[str, Any] | None:
        text = self._text_artifact(run_id, "interval_coverage.json")
        return json.loads(text) if text else None

    def model_card(self, run_id: str) -> str | None:
        return self._text_artifact(run_id, "model_card.md")

    def search_runs(
        self, experiment_name: str, max_results: int = 50, page_token: str | None = None
    ) -> tuple[list[dict[str, Any]], str | None]:
        return [], None  # experiment history lives in the operator's MLflow store

    # -- writes (refused) --------------------------------------------------------------------
    def _refuse(self, operation: str) -> BundleOperationUnavailable:
        return BundleOperationUnavailable(
            f"{operation} is unavailable in bundle mode: validation and promotion happen in the operator "
            "environment (MLflow) and the promoted version is exported as a bundle with `icp-api export-model`."
        )

    def set_production(self, version: str) -> None:
        raise self._refuse("Promotion")

    def validate_candidate(self, version: str, smoke_rows: pd.DataFrame, policy: Any) -> Any:
        raise self._refuse("Validation")
