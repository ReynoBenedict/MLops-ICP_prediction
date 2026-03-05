# Preprocessing routines for time-series dataset.
from __future__ import annotations

import logging
from typing import Optional, Tuple, Union

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, StandardScaler

logger = logging.getLogger(__name__)


def sort_by_date(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    return df.sort_values(date_col).reset_index(drop=True)


def handle_missing_values(
    df: pd.DataFrame,
    method: str = "ffill",
    numeric_cols: Optional[list[str]] = None,
) -> pd.DataFrame:
    df = df.copy()
    if numeric_cols is None:
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if method == "ffill":
        df[numeric_cols] = df[numeric_cols].ffill().bfill()
    elif method == "bfill":
        df[numeric_cols] = df[numeric_cols].bfill().ffill()
    elif method == "interpolate":
        df[numeric_cols] = df[numeric_cols].interpolate(method="linear").ffill().bfill()
    else:
        raise ValueError(f"Unknown method: '{method}'. Use ffill, bfill, or interpolate.")
    return df


def normalize_features(
    df: pd.DataFrame,
    feature_cols: list[str],
    method: str = "minmax",
) -> Tuple[pd.DataFrame, Union[MinMaxScaler, StandardScaler]]:
    df = df.copy()
    scaler = MinMaxScaler() if method == "minmax" else StandardScaler()
    df[feature_cols] = scaler.fit_transform(df[feature_cols])
    return df, scaler


def preprocess(
    df: pd.DataFrame,
    date_col: str = "date",
    target_col: str = "icp_price",
    feature_cols: Optional[list[str]] = None,
    fill_method: str = "ffill",
    normalize: bool = True,
    normalize_method: str = "minmax",
) -> Tuple[pd.DataFrame, Optional[Union[MinMaxScaler, StandardScaler]]]:
    if feature_cols is None:
        feature_cols = [
            c for c in df.columns
            if c not in (date_col, target_col) and pd.api.types.is_numeric_dtype(df[c])
        ]
    df = sort_by_date(df, date_col=date_col)
    df = handle_missing_values(df, method=fill_method)
    scaler = None
    if normalize and feature_cols:
        df, scaler = normalize_features(df, feature_cols=feature_cols, method=normalize_method)
    return df, scaler


def train_test_split_timeseries(
    df: pd.DataFrame,
    test_ratio: float = 0.2,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    split_idx = int(len(df) * (1 - test_ratio))
    train = df.iloc[:split_idx].reset_index(drop=True)
    test = df.iloc[split_idx:].reset_index(drop=True)
    logger.info("Split: %d train / %d test rows.", len(train), len(test))
    return train, test
