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
