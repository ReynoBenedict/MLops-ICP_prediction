"""Rolling-origin backtesting with horizon-specific metrics and two benchmark-input modes.

For each forecast origin *L* (the latest official ICP month available to the forecaster) a
method is fitted on rows with target <= L and asked for ICP(L+1 .. L+H).

* ``observed`` mode: benchmark averages for the target months are the realised values. This is
  the relevant error for *estimates of elapsed months awaiting publication* (and isolates how
  well the method maps benchmarks to ICP).
* ``hold_last`` mode: benchmark averages beyond *L* are held at the last value known at the
  origin. This is the relevant error for *genuine future forecasts* when the user supplies no
  scenario. (The month-*L* benchmark average itself is known at the origin because month L has
  ended by the time ICP(L) is published.)

In both modes ICP lags beyond the origin are the method's own earlier predictions (recursive).
Metrics: MAE, RMSE, bias (mean error), sMAPE, MASE (scaled by the in-sample one-step naive MAE at
each origin), directional accuracy vs ICP(L), and empirical residual quantiles for intervals.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from icp_ml.features import (
    BENCHMARK_COLUMNS,
    FEATURE_COLUMNS,
    TARGET_COLUMN,
    MissingFeatureData,
    build_feature_frame,
    feature_row,
)
from icp_ml.methods import MethodSpec

MODES = ("observed", "hold_last")


@dataclass
class BacktestConfig:
    horizons: tuple[int, ...] = (1, 2, 3, 4, 5, 6)
    min_train_rows: int = 24
    first_origin: str | None = None  # e.g. "2021-12"; default = first origin with min_train_rows
    modes: tuple[str, ...] = MODES
    seed: int = 42


@dataclass
class BacktestResult:
    predictions: pd.DataFrame  # method, mode, origin, horizon, target, y_true, y_pred, error
    metrics: pd.DataFrame  # method, mode, horizon, n, mae, rmse, bias, smape, mase, dir_acc
    origins: list[str] = field(default_factory=list)
    skipped: dict[str, list[str]] = field(default_factory=dict)  # method -> origins skipped (missing inputs)


def _naive_scale(panel: pd.DataFrame, origin: pd.Period) -> float:
    """In-sample MAE of the one-step naive forecast up to ``origin`` (MASE denominator)."""
    y = panel.loc[:origin, TARGET_COLUMN].dropna()
    # Only consecutive months count (a gap is not a one-month change).
    consecutive = [abs(y[p] - y[p - 1]) for p in y.index if (p - 1) in y.index]
    return float(np.mean(consecutive)) if consecutive else float("nan")


def forecast_path(
    method: Any,
    spec: MethodSpec,
    panel: pd.DataFrame,
    origin: pd.Period,
    horizons: tuple[int, ...],
    mode: str,
    assumptions: dict[str, dict[pd.Period, float]] | None = None,
) -> list[dict[str, Any]]:
    """Recursive multi-step path from ``origin``. Returns one row per horizon.

    ``assumptions`` (used by live forecasting) maps panel column -> {period: value} and overrides
    the ``mode`` rule for those months. Each row records where every benchmark input came from.
    """
    last_target = origin + max(horizons)
    idx = pd.period_range(panel.index.min(), max(panel.index.max(), last_target), freq="M")
    # Working copy: everything after the origin is unknown to the forecaster until we fill it in.
    work = panel.reindex(idx).copy()
    work.loc[work.index > origin, TARGET_COLUMN] = np.nan
    last_known = {b: panel.loc[:origin, b].dropna() for b in BENCHMARK_COLUMNS}
    required = spec.requires
    rows = []
    for h in horizons:
        target = origin + h
        sources: dict[str, str] = {}
        for b in BENCHMARK_COLUMNS:
            observed = target in panel.index and not np.isnan(panel.loc[target, b])
            if assumptions and b in assumptions and target in assumptions[b]:
                work.loc[target, b] = assumptions[b][target]
                sources[b] = "scenario"
            elif mode == "observed" and observed:
                sources[b] = "observed"
            elif last_known[b].empty:
                work.loc[target, b] = np.nan
                sources[b] = "unavailable"
            else:
                work.loc[target, b] = float(last_known[b].iloc[-1])
                sources[b] = "assumed_hold_last"
        try:
            feats = feature_row(work, target, required)
        except MissingFeatureData as exc:
            rows.append(
                {"horizon": h, "target": str(target), "y_pred": np.nan, "sources": sources, "error": str(exc)}
            )
            continue
        x = pd.DataFrame([{c: feats.get(c, np.nan) for c in FEATURE_COLUMNS}])
        y_hat = float(np.asarray(method.predict(x)).reshape(-1)[0])
        # Recursive: this prediction becomes the ICP lag for later horizons (never the true value).
        work.loc[target, TARGET_COLUMN] = y_hat
        rows.append(
            {
                "horizon": h,
                "target": str(target),
                "y_pred": y_hat,
                "sources": sources,
                "benchmarks": {
                    b: (None if np.isnan(work.loc[target, b]) else float(work.loc[target, b]))
                    for b in BENCHMARK_COLUMNS
                },
            }
        )
    return rows


def rolling_origin(
    panel: pd.DataFrame,
    methods: tuple[MethodSpec, ...],
    config: BacktestConfig = BacktestConfig(),
    last_origin: pd.Period | None = None,
) -> BacktestResult:
    y_all = panel[TARGET_COLUMN]
    official = y_all.dropna()
    last_origin = last_origin or official.index.max()
    preds: list[dict[str, Any]] = []
    skipped: dict[str, list[str]] = {}
    origins: list[pd.Period] = []
    # candidate origins: official months with enough complete training rows for the richest method
    for origin in official.index:
        if origin >= last_origin or (config.first_origin and str(origin) < config.first_origin):
            continue
        if (official.index <= origin).sum() < config.min_train_rows:
            continue
        origins.append(origin)
    for spec in methods:
        req = spec.requires
        for origin in origins:
            train_panel = panel.loc[:origin]
            frame = build_feature_frame(train_panel, req)
            if len(frame) < config.min_train_rows:
                skipped.setdefault(spec.name, []).append(str(origin))
                continue
            model = spec.build(config.seed).fit(frame[FEATURE_COLUMNS], frame[TARGET_COLUMN])
            scale = _naive_scale(panel, origin)
            for mode in config.modes:
                for r in forecast_path(model, spec, panel, origin, config.horizons, mode):
                    target = pd.Period(r["target"], freq="M")
                    y_true = y_all.get(target)
                    if y_true is None or np.isnan(y_true) or np.isnan(r["y_pred"]):
                        continue
                    preds.append(
                        {
                            "method": spec.name,
                            "mode": mode,
                            "origin": str(origin),
                            "horizon": r["horizon"],
                            "target": r["target"],
                            "y_true": float(y_true),
                            "y_pred": r["y_pred"],
                            "error": r["y_pred"] - float(y_true),
                            "naive_scale": scale,
                            "last_official": float(official[origin]),
                        }
                    )
    pred_df = pd.DataFrame(preds)
    return BacktestResult(pred_df, summarize(pred_df), [str(o) for o in origins], skipped)


def summarize(pred_df: pd.DataFrame) -> pd.DataFrame:
    if pred_df.empty:
        return pd.DataFrame(
            columns=["method", "mode", "horizon", "n", "mae", "rmse", "bias", "smape", "mase", "dir_acc"]
        )
    rows = []
    for (method, mode, h), g in pred_df.groupby(["method", "mode", "horizon"]):
        e = g["error"].to_numpy()
        denom = (np.abs(g["y_true"]) + np.abs(g["y_pred"])).to_numpy()
        smape = float(np.mean(2 * np.abs(e) / np.where(denom == 0, np.nan, denom)) * 100)
        scale = g["naive_scale"].to_numpy()
        mase = float(np.mean(np.abs(e) / np.where(scale == 0, np.nan, scale)))
        up_true = np.sign(g["y_true"] - g["last_official"])
        up_pred = np.sign(g["y_pred"] - g["last_official"])
        moved = up_true != 0
        dir_acc = float(np.mean((up_true == up_pred)[moved])) if moved.any() else float("nan")
        rows.append(
            {
                "method": method,
                "mode": mode,
                "horizon": int(h),
                "n": len(g),
                "mae": float(np.mean(np.abs(e))),
                "rmse": float(np.sqrt(np.mean(e**2))),
                "bias": float(np.mean(e)),
                "smape": smape,
                "mase": mase,
                "dir_acc": dir_acc,
                "q05": float(np.quantile(e, 0.05)),
                "q10": float(np.quantile(e, 0.10)),
                "q90": float(np.quantile(e, 0.90)),
                "q95": float(np.quantile(e, 0.95)),
            }
        )
    return pd.DataFrame(rows).sort_values(["method", "mode", "horizon"]).reset_index(drop=True)


def common_subset(pred_df: pd.DataFrame) -> pd.DataFrame:
    """Rows for (mode, origin, horizon) keys that *every* method produced - the fair comparison set."""
    if pred_df.empty:
        return pred_df
    n_methods = pred_df["method"].nunique()
    counts = pred_df.groupby(["mode", "origin", "horizon"])["method"].nunique()
    good = set(counts[counts == n_methods].index)
    keys = list(zip(pred_df["mode"], pred_df["origin"], pred_df["horizon"], strict=True))
    return pred_df[[k in good for k in keys]].reset_index(drop=True)


def leaderboard(metrics: pd.DataFrame, mode: str, horizons: tuple[int, ...], metric: str) -> pd.DataFrame:
    """Primary score per method: ``metric`` averaged over ``horizons`` in ``mode`` (NaN if incomplete)."""
    rows = []
    for method, g in metrics[metrics["mode"] == mode].groupby("method"):
        sub = g[g["horizon"].isin(horizons)]
        score = float(sub[metric].mean()) if len(sub) == len(horizons) else float("nan")
        rows.append({"method": method, "score": score, "n_min": int(sub["n"].min()) if len(sub) else 0})
    return pd.DataFrame(rows).sort_values("score", na_position="last").reset_index(drop=True)


def interval_tables(
    pred_df: pd.DataFrame, method: str, levels: tuple[float, ...], min_n: int = 8
) -> dict[str, dict[str, dict[str, list[float]]]]:
    """``{mode: {horizon: {level: [lower_offset, upper_offset]}}}`` from empirical error quantiles."""
    out: dict[str, dict[str, dict[str, list[float]]]] = {}
    g = pred_df[pred_df["method"] == method]
    for (mode, h), gh in g.groupby(["mode", "horizon"]):
        e = gh["error"].to_numpy()
        if len(e) < min_n:
            continue
        table = {}
        for lv in levels:
            lo, hi = np.quantile(e, (1 - lv) / 2), np.quantile(e, 1 - (1 - lv) / 2)
            # offsets are added to the point forecast: lower = y + q_lo of (pred - true) negated
            table[f"{lv:g}"] = [round(float(-hi), 4), round(float(-lo), 4)]
        out.setdefault(str(mode), {})[str(int(h))] = table
    return out


def split_coverage(
    pred_df: pd.DataFrame, method: str, levels: tuple[float, ...], fit_share: float = 0.6
) -> dict[str, dict[str, dict[str, float]]]:
    """Honest coverage: intervals fitted on the earliest ``fit_share`` of origins, evaluated on the rest."""
    g = pred_df[pred_df["method"] == method]
    if g.empty:
        return {}
    origins = sorted(g["origin"].unique())
    cut = origins[int(len(origins) * fit_share)] if len(origins) >= 10 else None
    if cut is None:
        return {}
    fit, test = g[g["origin"] < cut], g[g["origin"] >= cut]
    tables = interval_tables(fit, method, levels)
    out: dict[str, dict[str, dict[str, float]]] = {}
    for (mode, h), gh in test.groupby(["mode", "horizon"]):
        table = tables.get(str(mode), {}).get(str(int(h)))
        if not table:
            continue
        cov = {}
        for lv, (lo, hi) in table.items():
            inside = (gh["y_true"] >= gh["y_pred"] + lo) & (gh["y_true"] <= gh["y_pred"] + hi)
            cov[lv] = round(float(inside.mean()), 3)
        cov["n"] = len(gh)
        out.setdefault(str(mode), {})[str(int(h))] = cov
    return out
