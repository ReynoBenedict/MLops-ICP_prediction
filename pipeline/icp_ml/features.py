"""Calendar-aware, as-of feature construction shared by training, backtesting and forecasting.

Forecasting problem (see docs/FORECASTING_METHODOLOGY.md)
--------------------------------------------------------
* **Target**: official ICP for calendar month *t* (USD/bbl), published early in *t+1*.
* **Benchmarks for month t** (WTI/Brent/Dubai monthly averages) are known once month *t* has
  ended. For a month that has elapsed but whose ICP is not yet published they are *observed*;
  for a genuinely future month they must be *assumed* (user scenario or a declared rule).
* **ICP lags** use official values up to the forecast origin and model predictions after it.
* Lags are computed on a complete monthly calendar; a missing month is never bridged.

The panel is a frame indexed by ``PeriodIndex`` with the raw monthly series; features are
derived from it by :func:`build_feature_frame` (training) and :func:`feature_row` (inference),
which are guaranteed to agree (tested).
"""

from __future__ import annotations

import math
from collections.abc import Iterable

import pandas as pd

TARGET_COLUMN = "icp_price"
BENCHMARK_COLUMNS: tuple[str, ...] = ("wti_price", "brent_price", "dubai_price")
FX_COLUMN = "usd_idr"
PANEL_COLUMNS: tuple[str, ...] = (TARGET_COLUMN, *BENCHMARK_COLUMNS, FX_COLUMN)

# All derivable features. Methods declare the subset they need; rows are complete per subset.
FEATURE_COLUMNS: list[str] = [
    "wti_price",
    "brent_price",
    "dubai_price",  # same-month benchmark averages (observed or assumed)
    "icp_lag_1",
    "icp_lag_2",
    "icp_lag_3",  # official ICP (or predictions beyond the origin)
    "icp_mean_3",
    "wti_lag_1",
    "brent_lag_1",
    "dubai_lag_1",
    "spread_wti_lag_1",
    "spread_brent_lag_1",
    "spread_dubai_lag_1",  # ICP - benchmark, previous month
    "spread_wti_mean_3",
    "spread_brent_mean_3",
    "spread_dubai_mean_3",
    "usd_idr",
]
# Compatibility: the pre-2026-10 models used this WTI-only set.
LEGACY_FEATURE_COLUMNS: list[str] = [
    "lag_1",
    "lag_3",
    "lag_6",
    "rolling_mean_3",
    "wti_price",
    "wti_lag_1",
    "wti_rolling_mean_3",
]
MAX_LOOKBACK_MONTHS = 3


class MissingFeatureData(ValueError):
    """Raised when inputs needed to build a feature row are not available."""

    def __init__(self, target: pd.Period, missing: list[str]):
        self.target = target
        self.missing = missing
        super().__init__(f"Cannot build features for {target}: missing {', '.join(missing)}")


def build_panel(
    icp: pd.Series, benchmarks: dict[str, pd.Series] | None = None, fx: pd.Series | None = None
) -> pd.DataFrame:
    """Align monthly series on one calendar (NaN where a series has no observation)."""
    cols = {TARGET_COLUMN: icp.astype(float)}
    for name, s in (benchmarks or {}).items():
        col = name if name.endswith("_price") else f"{name}_price"
        cols[col] = s.astype(float)
    if fx is not None:
        cols[FX_COLUMN] = fx.astype(float)
    start = min(s.index.min() for s in cols.values())
    end = max(s.index.max() for s in cols.values())
    idx = pd.period_range(start, end, freq="M")
    panel = pd.DataFrame(index=idx)
    for c in PANEL_COLUMNS:
        panel[c] = cols[c].reindex(idx) if c in cols else float("nan")
    panel.index.name = "period"
    return panel


def _derive(panel: pd.DataFrame) -> pd.DataFrame:
    y = panel[TARGET_COLUMN]
    f = pd.DataFrame(index=panel.index)
    for b in BENCHMARK_COLUMNS:
        f[b] = panel[b]
    f["icp_lag_1"], f["icp_lag_2"], f["icp_lag_3"] = y.shift(1), y.shift(2), y.shift(3)
    f["icp_mean_3"] = y.shift(1).rolling(3, min_periods=3).mean()
    for b, short in zip(BENCHMARK_COLUMNS, ("wti", "brent", "dubai"), strict=True):
        f[f"{short}_lag_1"] = panel[b].shift(1)
        spread = y - panel[b]
        f[f"spread_{short}_lag_1"] = spread.shift(1)
        f[f"spread_{short}_mean_3"] = spread.shift(1).rolling(3, min_periods=3).mean()
    f[FX_COLUMN] = panel[FX_COLUMN]
    f[TARGET_COLUMN] = y
    return f


def build_feature_frame(panel: pd.DataFrame, required: Iterable[str] = FEATURE_COLUMNS) -> pd.DataFrame:
    """Rows where the target and every ``required`` feature are available (never bridged)."""
    required = list(required)
    frame = _derive(panel)
    keep = [TARGET_COLUMN, *[c for c in FEATURE_COLUMNS if c in required or c in frame.columns]]
    frame = frame[[c for c in dict.fromkeys(keep)]]
    mask = frame[[TARGET_COLUMN, *required]].notna().all(axis=1)
    out = frame[mask].astype(float)
    out.index.name = "period"
    return out


def feature_row(
    panel: pd.DataFrame, target: pd.Period, required: Iterable[str] = FEATURE_COLUMNS
) -> dict[str, float]:
    """Feature values for one target month from a panel that may contain assumed/predicted values.

    Equals the corresponding row of ``build_feature_frame`` for the same panel (property-tested).
    """
    required = list(required)
    if target not in panel.index:
        panel = panel.reindex(pd.period_range(panel.index.min(), max(panel.index.max(), target), freq="M"))
    window = panel.loc[:target]
    derived = _derive(window)
    row = derived.loc[target]
    missing = [c for c in required if c in row.index and (row[c] is None or math.isnan(float(row[c])))]
    if missing:
        raise MissingFeatureData(target, missing)
    return {c: float(row[c]) for c in FEATURE_COLUMNS if c in row.index and not math.isnan(float(row[c]))} | {
        c: float(row[c]) for c in required
    }


def chronological_split(frame: pd.DataFrame, test_ratio: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split a feature frame into train/test without shuffling (test = most recent rows)."""
    if not 0.0 < test_ratio < 1.0:
        raise ValueError("test_ratio must be in (0, 1)")
    n_test = max(1, round(len(frame) * test_ratio))
    if len(frame) - n_test < 2:
        raise ValueError(f"Not enough rows ({len(frame)}) for a chronological split.")
    return frame.iloc[:-n_test], frame.iloc[-n_test:]
