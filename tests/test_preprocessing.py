# test_preprocessing.py — unit tests for src/preprocessing.py

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_loader import _generate_dummy_data
from preprocessing import (
    handle_missing_values,
    normalize_features,
    preprocess,
    sort_by_date,
    train_test_split_timeseries,
)


@pytest.fixture
def sample_df():
    return _generate_dummy_data(n_rows=60)


class TestSortByDate:
    def test_sorted_ascending(self, sample_df):
        shuffled = sample_df.sample(frac=1, random_state=0)
        result = sort_by_date(shuffled)
        assert result["date"].is_monotonic_increasing


class TestHandleMissingValues:
    def test_no_nulls_after_ffill(self, sample_df):
        df = sample_df.copy()
        df.loc[[5, 10], "icp_price"] = np.nan
        result = handle_missing_values(df, method="ffill")
        assert result["icp_price"].isnull().sum() == 0

    def test_invalid_method_raises(self, sample_df):
        with pytest.raises(ValueError):
            handle_missing_values(sample_df, method="unknown_method")


class TestNormalizeFeatures:
    def test_minmax_range(self, sample_df):
        cols = ["brent_price", "usd_idr"]
        result, _ = normalize_features(sample_df, feature_cols=cols, method="minmax")
        assert result[cols].min().min() >= -1e-6
        assert result[cols].max().max() <= 1 + 1e-6

    def test_standard_mean_zero(self, sample_df):
        cols = ["brent_price", "usd_idr"]
        result, _ = normalize_features(sample_df, feature_cols=cols, method="standard")
        assert (result[cols].mean().abs() < 1e-6).all()


class TestPreprocess:
    def test_returns_dataframe_and_scaler(self, sample_df):
        df, scaler = preprocess(sample_df)
        assert isinstance(df, pd.DataFrame)
        assert scaler is not None

    def test_no_nulls_in_output(self, sample_df):
        df, _ = preprocess(sample_df)
        assert df.isnull().sum().sum() == 0


class TestTrainTestSplit:
    def test_split_proportions(self, sample_df):
        train, test = train_test_split_timeseries(sample_df, test_ratio=0.2)
        assert len(train) + len(test) == len(sample_df)
        assert abs(len(test) / len(sample_df) - 0.2) < 0.05

    def test_no_shuffle(self, sample_df):
        train, test = train_test_split_timeseries(sample_df)
        assert train.index[-1] < len(sample_df) - len(test)
