# test_data_loader.py — unit tests for src/data_loader.py

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_loader import _generate_dummy_data, load_icp_dataset


class TestGenerateDummyData:
    def test_default_shape(self):
        df = _generate_dummy_data()
        assert df.shape == (120, 4), f"Expected (120, 4), got {df.shape}"

    def test_custom_n_rows(self):
        df = _generate_dummy_data(n_rows=24)
        assert len(df) == 24

    def test_column_names(self):
        df = _generate_dummy_data()
        assert set(df.columns) == {"date", "icp_price", "brent_price", "usd_idr"}

    def test_no_nulls(self):
        df = _generate_dummy_data()
        assert df.isnull().sum().sum() == 0

    def test_numeric_columns_are_positive(self):
        df = _generate_dummy_data()
        assert (df[["icp_price", "brent_price", "usd_idr"]] > 0).all().all()

    def test_date_is_datetime(self):
        df = _generate_dummy_data()
        assert pd.api.types.is_datetime64_any_dtype(df["date"])

    def test_dates_are_sorted(self):
        df = _generate_dummy_data()
        assert df["date"].is_monotonic_increasing

    def test_reproducibility(self):
        df1 = _generate_dummy_data(seed=42)
        df2 = _generate_dummy_data(seed=42)
        pd.testing.assert_frame_equal(df1, df2)


class TestLoadIcpDataset:
    def test_returns_dataframe(self):
        df = load_icp_dataset()
        assert isinstance(df, pd.DataFrame)

    def test_required_columns_present(self):
        # When a real CSV exists it has date + icp_price; dummy data has all four columns
        df = load_icp_dataset()
        assert "date" in df.columns
        assert "icp_price" in df.columns

    def test_date_column_is_datetime(self):
        df = load_icp_dataset()
        assert pd.api.types.is_datetime64_any_dtype(df["date"])

    def test_fallback_when_no_csv(self, tmp_path):
        df = load_icp_dataset(csv_path=str(tmp_path / "nonexistent.csv"))
        assert len(df) > 0

    def test_load_from_csv(self, tmp_path):
        dummy = _generate_dummy_data(n_rows=20)
        csv_path = tmp_path / "icp_data.csv"
        dummy.to_csv(csv_path, index=False)
        df = load_icp_dataset(csv_path=str(csv_path))
        assert len(df) == 20
