"""Point-forecast error metrics (backtest aggregation lives in ``icp_ml.backtest``)."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd


def rmse(y_true: np.ndarray | pd.Series, y_pred: np.ndarray | pd.Series) -> float:
    a, b = np.asarray(y_true, dtype=float), np.asarray(y_pred, dtype=float)
    return float(math.sqrt(np.mean((a - b) ** 2)))


def mae(y_true: np.ndarray | pd.Series, y_pred: np.ndarray | pd.Series) -> float:
    a, b = np.asarray(y_true, dtype=float), np.asarray(y_pred, dtype=float)
    return float(np.mean(np.abs(a - b)))


def smape(y_true: np.ndarray | pd.Series, y_pred: np.ndarray | pd.Series) -> float:
    a, b = np.asarray(y_true, dtype=float), np.asarray(y_pred, dtype=float)
    denom = np.abs(a) + np.abs(b)
    return float(
        np.mean(np.where(denom == 0, 0.0, 2 * np.abs(a - b) / np.where(denom == 0, 1, denom))) * 100.0
    )


def bias(y_true: np.ndarray | pd.Series, y_pred: np.ndarray | pd.Series) -> float:
    a, b = np.asarray(y_true, dtype=float), np.asarray(y_pred, dtype=float)
    return float(np.mean(b - a))


def regression_metrics(y_true: np.ndarray | pd.Series, y_pred: np.ndarray | pd.Series) -> dict[str, float]:
    return {
        "rmse": rmse(y_true, y_pred),
        "mae": mae(y_true, y_pred),
        "smape": smape(y_true, y_pred),
        "bias": bias(y_true, y_pred),
    }


def baseline_by_step(pred_df: pd.DataFrame) -> dict[str, dict[str, dict[str, float | int]]]:
    """Naive-persistence MAE on exactly the backtest cases of ``pred_df`` (one method's predictions).

    The naive forecast from origin L is ICP(L) for every horizon (``last_official``), so its error
    can be recomputed from stored predictions without refitting: ``{mode: {horizon: {mae, n}}}``."""
    out: dict[str, dict[str, dict[str, float | int]]] = {}
    if pred_df is None or pred_df.empty or "last_official" not in pred_df:
        return out
    for (mode, h), g in pred_df.groupby(["mode", "horizon"]):
        err = (g["y_true"].astype(float) - g["last_official"].astype(float)).abs()
        out.setdefault(str(mode), {})[str(int(h))] = {"mae": float(err.mean()), "n": len(g)}
    return out


def embargoed_coverage(
    pred_df: pd.DataFrame | None, levels: tuple[float, ...] = (0.5, 0.8, 0.95), fit_share: float = 0.6
) -> dict[str, dict[str, dict[str, float]]]:
    """Out-of-sample coverage with the fit/test embargo (``icp_ml.backtest.split_coverage``),
    recomputed from one method's stored backtest predictions without refitting (audit v7 ML-01).
    Empty when the predictions are missing or too short for the check."""
    from icp_ml.backtest import split_coverage

    if pred_df is None or pred_df.empty or "method" not in pred_df or "target" not in pred_df:
        return {}
    methods = pred_df["method"].unique()
    if len(methods) != 1:
        return {}
    return split_coverage(pred_df, str(methods[0]), levels, fit_share=fit_share, embargo=True)


def coverage_window(pred_df: pd.DataFrame, fit_share: float = 0.6) -> dict[str, str] | None:
    """Origins whose cases were used for the out-of-sample interval coverage check
    (same rule as ``icp_ml.backtest.split_coverage``); None when that check was not possible."""
    if pred_df is None or pred_df.empty:
        return None
    origins = sorted(str(o) for o in pred_df["origin"].unique())
    if len(origins) < 10:
        return None
    return {"origin_start": origins[int(len(origins) * fit_share)], "origin_end": origins[-1]}
