# Muat dataset atau buat data dummy jika file tidak ada
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_CSV = _PROJECT_ROOT / "data" / "processed" / "icp_dataset.csv"


def _generate_dummy_data(n_rows: int = 120, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.date_range(start="2014-01-01", periods=n_rows, freq="MS")
    brent = np.clip(70.0 + rng.normal(0, 2.5, n_rows).cumsum(), 20, 150)
    icp = np.clip(brent + rng.normal(-2, 1.0, n_rows), 15, 145)
    usd_idr = np.clip(14000.0 + rng.normal(0, 150, n_rows).cumsum(), 10000, 20000)
    return pd.DataFrame({
        "date": dates,
        "icp_price": np.round(icp, 2),
        "brent_price": np.round(brent, 2),
        "usd_idr": np.round(usd_idr, 0),
    })


def load_icp_dataset(csv_path: Optional[str] = None) -> pd.DataFrame:
    path = Path(csv_path) if csv_path else _DEFAULT_CSV

    if path.exists():
        try:
            df = pd.read_csv(path)
            logger.info("Dimuat %d baris dari %s", len(df), path)
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            return df
        except Exception as exc:
            logger.warning("Gagal membaca %s: %s", path, exc)

    logger.warning("Dataset tidak ditemukan — menggunakan data dummy.")
    return _generate_dummy_data()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    df = load_icp_dataset()
    print(df.head(10))
    print(f"\nShape: {df.shape}")
