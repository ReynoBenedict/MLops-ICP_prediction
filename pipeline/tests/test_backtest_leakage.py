"""As-of guarantees of the rolling-origin backtest (Agent D, mission v6).

A forecast made at origin L may only use information available at L:
* ``hold_last`` mode: nothing after L at all (ICP, benchmarks, FX);
* ``observed`` mode: realised benchmark/FX averages of the target months are allowed by design
  (they are the "elapsed month awaiting publication" situation), ICP after L never is.

Each test perturbs the panel after the origin and checks the predictions do not move.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from icp_ml.backtest import BacktestConfig, forecast_path, rolling_origin
from icp_ml.features import BENCHMARK_COLUMNS, FEATURE_COLUMNS, FX_COLUMN, TARGET_COLUMN, build_feature_frame
from icp_ml.methods import MethodSpec, find_method

FULL = find_method("ridge_full")  # reads every feature, including usd_idr
METHODS = (find_method("naive_last"), find_method("spread_brent_6m"), find_method("ridge_benchmarks"), FULL)


def _perturb_after(panel: pd.DataFrame, origin: pd.Period, columns: list[str], seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    out = panel.copy()
    mask = out.index > origin
    for c in columns:
        out.loc[mask, c] = out.loc[mask, c] * (1 + rng.uniform(0.2, 0.5, mask.sum()))
    return out


def _fit(spec: MethodSpec, panel: pd.DataFrame, origin: pd.Period):
    frame = build_feature_frame(panel.loc[:origin], spec.requires)
    return spec.build(1).fit(frame[FEATURE_COLUMNS], frame[TARGET_COLUMN])


@pytest.mark.parametrize("spec", METHODS, ids=lambda s: s.name)
@pytest.mark.parametrize("horizons", [(1, 2, 3), (3,), (2, 5)])
def test_hold_last_path_uses_nothing_after_the_origin(synthetic_panel, spec, horizons):
    origin = synthetic_panel[TARGET_COLUMN].dropna().index.max() - 8
    model = _fit(spec, synthetic_panel, origin)
    base = forecast_path(model, spec, synthetic_panel, origin, horizons, "hold_last")
    shaken = _perturb_after(synthetic_panel, origin, [TARGET_COLUMN, *BENCHMARK_COLUMNS, FX_COLUMN])
    moved = forecast_path(model, spec, shaken, origin, horizons, "hold_last")
    assert [r["y_pred"] for r in base] == pytest.approx([r["y_pred"] for r in moved], nan_ok=True)
    for r in base:
        assert set(r["sources"].values()) <= {"assumed_hold_last", "unavailable"}
        assert r["fx_source"] in ("assumed_hold_last", "unavailable")


@pytest.mark.parametrize("spec", METHODS, ids=lambda s: s.name)
def test_observed_path_never_uses_icp_after_the_origin(synthetic_panel, spec):
    origin = synthetic_panel[TARGET_COLUMN].dropna().index.max() - 8
    model = _fit(spec, synthetic_panel, origin)
    base = forecast_path(model, spec, synthetic_panel, origin, (1, 2, 3), "observed")
    shaken = _perturb_after(synthetic_panel, origin, [TARGET_COLUMN])
    moved = forecast_path(model, spec, shaken, origin, (1, 2, 3), "observed")
    assert [r["y_pred"] for r in base] == pytest.approx([r["y_pred"] for r in moved], nan_ok=True)


def test_rolling_origin_training_rows_end_at_the_origin(synthetic_panel):
    """Refit at each origin: a perturbation of every value after the last origin + horizon cannot
    change any backtest prediction (fit rows and naive scale stop at the origin)."""
    cfg = BacktestConfig(horizons=(1, 2), min_train_rows=24, modes=("hold_last",))
    last = synthetic_panel[TARGET_COLUMN].dropna().index.max()
    base = rolling_origin(synthetic_panel, (FULL,), cfg, last_origin=last - 6)
    # values strictly after the last target month of the last origin are irrelevant
    shaken = _perturb_after(synthetic_panel, last - 6 + 2, [TARGET_COLUMN, *BENCHMARK_COLUMNS, FX_COLUMN])
    moved = rolling_origin(shaken, (FULL,), cfg, last_origin=last - 6)
    pd.testing.assert_series_equal(base.predictions["y_pred"], moved.predictions["y_pred"])
    pd.testing.assert_series_equal(base.predictions["naive_scale"], moved.predictions["naive_scale"])
