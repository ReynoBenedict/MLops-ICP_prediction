from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from icp_ml.features import build_panel

ROOT = Path(__file__).resolve().parents[2]


def _make_series(start: str, values: list[float] | np.ndarray, name: str) -> pd.Series:
    idx = pd.period_range(start, periods=len(values), freq="M")
    return pd.Series(np.asarray(values, dtype=float), index=idx, name=name)


@pytest.fixture
def make_series():
    return _make_series


def synthetic_inputs(seed: int = 0, n: int = 72) -> dict[str, pd.Series]:
    """Benchmarks as a random walk (6 months before and 2 after ICP), ICP = Brent + 1.5 + noise, FX drifting."""
    rng = np.random.default_rng(seed)
    brent = 70 + np.cumsum(rng.normal(0, 3, n + 8))
    wti = brent - 4 + rng.normal(0, 0.6, n + 8)
    dubai = brent - 2 + rng.normal(0, 0.5, n + 8)
    icp = brent[6 : 6 + n] + 1.5 + rng.normal(0, 0.5, n)
    fx = 14000 + np.cumsum(rng.normal(20, 60, n + 8))
    return {
        "icp": _make_series("2019-01", icp, "icp_price"),
        "wti": _make_series("2018-07", wti, "wti_price"),
        "brent": _make_series("2018-07", brent, "brent_price"),
        "dubai": _make_series("2018-07", dubai, "dubai_price"),
        "usd_idr": _make_series("2018-07", fx, "usd_idr"),
    }


@pytest.fixture
def synthetic() -> dict[str, pd.Series]:
    return synthetic_inputs()


@pytest.fixture
def synthetic_panel(synthetic) -> pd.DataFrame:
    s = synthetic
    return build_panel(s["icp"], {"wti": s["wti"], "brent": s["brent"], "dubai": s["dubai"]}, s["usd_idr"])


@pytest.fixture
def repo_data() -> tuple[bytes, bytes]:
    icp = ROOT / "data" / "raw" / "dataset.csv"
    wti = ROOT / "data" / "raw" / "wti.csv"
    if not icp.exists() or not wti.exists():
        pytest.skip("repository datasets not present (run `dvc pull`)")
    return icp.read_bytes(), wti.read_bytes()
