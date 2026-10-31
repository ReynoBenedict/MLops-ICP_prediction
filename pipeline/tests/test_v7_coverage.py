"""Out-of-sample interval coverage uses an embargo between fit and test (audit v7 ML-01)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from icp_ml.backtest import split_coverage
from icp_ml.evaluation import embargoed_coverage

LEVELS = (0.5, 0.8, 0.95)


def _pred_df(n_origins: int = 30, horizons: tuple[int, ...] = (1, 2, 3, 6)) -> pd.DataFrame:
    rng = np.random.default_rng(7)
    start = pd.Period("2020-01", freq="M")
    rows = []
    for i in range(n_origins):
        origin = start + i
        for mode in ("observed", "hold_last"):
            for h in horizons:
                y_true = 80 + rng.normal(0, 5)
                err = rng.normal(0, 2 + h)
                rows.append(
                    {
                        "method": "m",
                        "mode": mode,
                        "origin": str(origin),
                        "horizon": h,
                        "target": str(origin + h),
                        "y_true": y_true,
                        "y_pred": y_true + err,
                        "error": err,
                        "last_official": 80.0,
                    }
                )
    return pd.DataFrame(rows)


def _cut(df: pd.DataFrame) -> str:
    origins = sorted(df["origin"].unique())
    return origins[int(len(origins) * 0.6)]


def test_fit_cases_whose_target_reaches_the_test_window_do_not_shape_the_intervals():
    df = _pred_df()
    cut = _cut(df)
    base = split_coverage(df, "m", LEVELS)
    leaky = (df["origin"] < cut) & (df["target"] > cut)
    assert leaky.any()  # h >= 2 cases of late fit origins overlap the test targets
    shocked = df.copy()
    shocked.loc[leaky, "error"] = shocked.loc[leaky, "error"] * 50
    assert split_coverage(shocked, "m", LEVELS) == base


def test_without_embargo_the_overlap_changes_the_result():
    """Documents the pre-v7 behaviour that stored coverage of older models was computed with."""
    df = _pred_df()
    cut = _cut(df)
    leaky = (df["origin"] < cut) & (df["target"] > cut)
    shocked = df.copy()
    shocked.loc[leaky, "error"] = shocked.loc[leaky, "error"] * 50
    assert split_coverage(shocked, "m", LEVELS, embargo=False) != split_coverage(
        df, "m", LEVELS, embargo=False
    )


def test_horizon_one_is_unaffected_by_the_embargo():
    df = _pred_df()
    with_e = split_coverage(df, "m", LEVELS)
    without = split_coverage(df, "m", LEVELS, embargo=False)
    for mode in ("observed", "hold_last"):
        assert with_e[mode]["1"] == without[mode]["1"]


def test_embargoed_coverage_recomputes_from_stored_predictions_of_one_method():
    df = _pred_df()
    assert embargoed_coverage(df, LEVELS) == split_coverage(df, "m", LEVELS)
    assert embargoed_coverage(pd.DataFrame(), LEVELS) == {}
    assert embargoed_coverage(_pred_df(n_origins=5), LEVELS) == {}  # too few origins: no check
