"""Exported bundles serve exactly what MLflow serves: same payload, same allow-list, same predictions."""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from icp_ml.bundle import (
    INDEX_KEY,
    POINTER_KEY,
    BundleOperationUnavailable,
    BundleRegistry,
    bundle_key,
    export_bundle,
)
from icp_ml.config import parse_config
from icp_ml.features import FEATURE_COLUMNS, build_feature_frame, build_panel
from icp_ml.forecasting import make_forecast
from icp_ml.registry import ModelRegistry, RegistryError
from icp_ml.serialization import UntrustedModelType, dump_skops, load_skops
from icp_ml.training import DatasetRef, train_and_register

CONFIG = {
    "experiment_name": "bundle-exp",
    "model_name": "Bundle_Model",
    "min_feature_rows": 20,
    "methods": [
        "naive_last",
        "spread_brent_6m",
        # A tree model exercises the skops trusted-type path end to end (export + load).
        {
            "name": "small_gbm",
            "family": "gradient_boosting",
            "params": {"n_estimators": 10, "max_depth": 2},
            "features": ["brent_price", "icp_lag_1"],
        },
    ],
    "backtest": {"horizons": [1, 2, 3], "min_train_rows": 24},
    "selection": {"mode": "observed", "horizons": [1, 2], "metric": "mae"},
}


class DictStore:
    """In-memory object store with the write-once semantics of the backend stores."""

    backend = "memory"

    def __init__(self) -> None:
        self.data: dict[str, tuple[bytes, str]] = {}

    def put(self, key: str, data: bytes, content_type: str, overwrite: bool = False) -> None:
        if key in self.data and not overwrite:
            raise FileExistsError(key)
        self.data[key] = (bytes(data), content_type)

    def get(self, key: str) -> bytes:
        return self.data[key][0]

    def exists(self, key: str) -> bool:
        return key in self.data


def _series(start: str, values, name: str) -> pd.Series:
    return pd.Series(
        np.asarray(values, dtype=float),
        index=pd.period_range(start, periods=len(values), freq="M"),
        name=name,
    )


@pytest.fixture(scope="module")
def panel():
    """Same generator as conftest.synthetic_inputs (module scope needs its own copy)."""
    rng = np.random.default_rng(0)
    n = 72
    brent = 70 + np.cumsum(rng.normal(0, 3, n + 8))
    wti = brent - 4 + rng.normal(0, 0.6, n + 8)
    dubai = brent - 2 + rng.normal(0, 0.5, n + 8)
    icp = brent[6 : 6 + n] + 1.5 + rng.normal(0, 0.5, n)
    fx = 14000 + np.cumsum(rng.normal(20, 60, n + 8))
    return build_panel(
        _series("2019-01", icp, "icp_price"),
        {
            "wti": _series("2018-07", wti, "wti_price"),
            "brent": _series("2018-07", brent, "brent_price"),
            "dubai": _series("2018-07", dubai, "dubai_price"),
        },
        _series("2018-07", fx, "usd_idr"),
    )


@pytest.fixture(scope="module")
def trained(panel, tmp_path_factory):
    """Train once per module into a private MLflow store (MLflow's tracking URI is process-global)."""
    tmp_path = tmp_path_factory.mktemp("bundle")
    tracking = f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}"
    cfg = parse_config(CONFIG)
    result = train_and_register(
        panel,
        cfg,
        tracking,
        str(tmp_path / "artifacts"),
        datasets=[DatasetRef("icp", "v1", "a" * 64)],
        job_id="job_bundle",
    )
    reg = ModelRegistry(tracking, "Bundle_Model")
    reg.set_production(result.registered_version)
    return reg, result, cfg


def test_export_then_serve_gives_identical_forecasts(trained, panel):
    reg, result, _ = trained
    store = DictStore()
    out = export_bundle(reg, store)
    version = result.registered_version
    assert out["version"] == version and out["skipped"] == []
    expected_keys = {
        bundle_key(version, n)
        for n in (
            "model.skops",
            "meta.json",
            "intervals.json",
            "interval_coverage.json",
            "model_card.md",
            "backtest_metrics.csv",
            "backtest_predictions.csv",
        )
    } | {POINTER_KEY, INDEX_KEY}
    assert set(store.data) == expected_keys
    pointer = json.loads(store.get(POINTER_KEY))
    assert pointer["version"] == version and pointer["source"].startswith("mlflow:")
    meta = json.loads(store.get(bundle_key(version, "meta.json")))
    assert meta["status"] == "READY" and "icp.method_spec" in meta["tags"]

    bundles = BundleRegistry(store, "Bundle_Model")
    bundles.ping()
    assert bundles.production_version() == version
    assert bundles.aliases() == {"production": version}
    assert [v.version for v in bundles.list_versions()] == [version]

    mv, bv = reg.get_version(version), bundles.get_version(version)
    assert bv.run_id == mv.run_id and bv.tags == mv.tags and bv.params == mv.params
    assert bv.metrics == pytest.approx(mv.metrics)
    assert bv.method_spec is not None and bv.method_spec.name == mv.method_spec.name
    assert bv.aliases == ["production"]

    # Same artifacts through both registries.
    run_id = mv.run_id
    assert bundles.intervals(run_id) == reg.intervals(run_id)
    assert bundles.interval_coverage(run_id) == reg.interval_coverage(run_id)
    assert bundles.model_card(run_id) == reg.model_card(run_id)
    assert bundles.backtest_metrics(run_id).equals(reg.backtest_metrics(run_id))
    assert bundles.holdout_predictions(run_id).equals(reg.holdout_predictions(run_id))
    assert bundles.search_runs("bundle-exp") == ([], None)

    # Same predictions: row-level and through the full forecast path, with and without a scenario.
    frame = build_feature_frame(panel)
    rows = frame.tail(6)[FEATURE_COLUMNS]
    mlflow_model, bundle_model = reg.load_model(version), bundles.load_model(version)
    np.testing.assert_allclose(
        np.asarray(bundle_model.predict(rows), dtype=float),
        np.asarray(mlflow_model.predict(rows), dtype=float),
    )
    spec = mv.method_spec
    intervals = reg.intervals(run_id)
    for scenario in ({}, {"brent": {"2025-02": 80.0, "2025-03": 82.5}}):
        a = make_forecast(mlflow_model, spec, panel, horizon=3, scenario=scenario, intervals=intervals)
        b = make_forecast(bundle_model, spec, panel, horizon=3, scenario=scenario, intervals=intervals)
        assert [p.prediction for p in a.points] == [p.prediction for p in b.points]
        assert [p.intervals for p in a.points] == [p.intervals for p in b.points]
        assert [p.kind for p in a.points] == [p.kind for p in b.points]


def test_tree_models_round_trip_and_re_export_is_idempotent(trained):
    reg, _, _ = trained
    store = DictStore()
    gbm_run = next(c for c in reg.search_runs("bundle-exp", 10)[0] if c["candidate"] == "small_gbm")
    model = reg.load_model_from_run(gbm_run["run_id"])
    payload, trusted = dump_skops(model.get_raw_model() if hasattr(model, "get_raw_model") else model)
    assert "sklearn.tree._tree.Tree" in trusted
    assert load_skops(payload, trusted) is not None

    first = export_bundle(reg, store)
    second = export_bundle(reg, store)  # nothing rewritten, pointer refreshed
    assert second["written"] == [] and set(second["skipped"]) == set(first["written"])
    assert json.loads(store.get(INDEX_KEY))["versions"] == [first["version"]]


def test_bundle_registry_refuses_writes_and_reports_missing_bundles(trained):
    reg, _, cfg = trained
    empty = BundleRegistry(DictStore(), "Bundle_Model")
    assert empty.production_version() is None and empty.list_versions() == []
    with pytest.raises(RegistryError):
        empty.ping()
    with pytest.raises(RegistryError):
        empty.get_version("1")
    store = DictStore()
    export_bundle(reg, store)
    bundles = BundleRegistry(store, "Bundle_Model")
    with pytest.raises(BundleOperationUnavailable):
        bundles.set_production("1")
    with pytest.raises(BundleOperationUnavailable):
        bundles.validate_candidate("1", None, cfg.promotion)


def test_bundle_loading_enforces_the_allow_list(trained):
    reg, result, _ = trained
    store = DictStore()
    export_bundle(reg, store)
    version = result.registered_version
    meta_key = bundle_key(version, "meta.json")
    meta = json.loads(store.get(meta_key))
    meta["skops_trusted_types"] = ["builtins.eval"]
    store.put(meta_key, json.dumps(meta).encode(), "application/json", overwrite=True)
    with pytest.raises(UntrustedModelType):
        BundleRegistry(store, "Bundle_Model").load_model(version)
    # A payload needing a non-allow-listed type is refused regardless of what meta claims.
    import skops.io as sio

    class Odd:
        pass

    with pytest.raises(UntrustedModelType):
        load_skops(sio.dumps({"x": Odd()}), [])
