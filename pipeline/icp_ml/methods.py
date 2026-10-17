"""Forecasting methods behind one interface: baselines, spread rules and ML regressors.

Every method is an sklearn-compatible estimator over the *named* feature frame produced by
``icp_ml.features`` (``fit(X: DataFrame, y)``, ``predict(X: DataFrame)``). Each declares the
feature columns it requires, so backtests compare all candidates on identical rows, and the
chosen method - whether a one-line rule or a boosted ensemble - is registered, validated,
promoted and served through exactly the same path.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from icp_ml.features import FEATURE_COLUMNS

BENCH = {"wti": "wti_price", "brent": "brent_price", "dubai": "dubai_price"}


class NaiveLast(BaseEstimator, RegressorMixin):
    """ICP(t) = ICP(t-1). The persistence baseline every method must beat."""

    requires = ["icp_lag_1"]

    def fit(self, X: pd.DataFrame, y: pd.Series) -> NaiveLast:
        self.is_fitted_ = True
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        return X["icp_lag_1"].to_numpy(dtype=float)


class SpreadRule(BaseEstimator, RegressorMixin):
    """ICP(t) = benchmark(t) + mean(ICP - benchmark) over the last ``window`` training months.

    ``window=None`` uses the whole training history (the former "WTI + average spread" baseline);
    a short window adapts to the drifting ICP-benchmark differential.
    """

    def __init__(self, benchmark: str = "wti", window: int | None = None):
        self.benchmark = benchmark
        self.window = window

    @property
    def requires(self) -> list[str]:
        return [BENCH[self.benchmark]]

    def fit(self, X: pd.DataFrame, y: pd.Series) -> SpreadRule:
        spread = np.asarray(y, dtype=float) - X[BENCH[self.benchmark]].to_numpy(dtype=float)
        if self.window:
            spread = spread[-self.window :]
        self.spread_ = float(np.mean(spread))
        self.n_fit_ = len(spread)
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        return X[BENCH[self.benchmark]].to_numpy(dtype=float) + self.spread_


class BenchmarkBlend(BaseEstimator, RegressorMixin):
    """ICP(t) = w1*Brent(t) + w2*Dubai(t) + w3*WTI(t) + c, least squares with non-negative weights.

    A transparent "formula-like" model mirroring how ICP tracks a basket of benchmarks.
    """

    requires = ["brent_price", "dubai_price", "wti_price"]

    def fit(self, X: pd.DataFrame, y: pd.Series) -> BenchmarkBlend:
        from scipy.optimize import nnls

        A = np.column_stack([X[c].to_numpy(dtype=float) for c in self.requires])
        yy = np.asarray(y, dtype=float)
        # Fit weights on de-meaned data, then intercept = mean residual.
        w, _ = nnls(A - A.mean(axis=0), yy - yy.mean())
        self.weights_ = w
        self.intercept_ = float(yy.mean() - A.mean(axis=0) @ w)
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        A = np.column_stack([X[c].to_numpy(dtype=float) for c in self.requires])
        return A @ self.weights_ + self.intercept_


def _sklearn(kind: str, features: list[str], params: dict[str, Any], seed: int) -> Pipeline:
    select = ColumnTransformer([("keep", "passthrough", features)], remainder="drop")
    if kind == "linear":
        model: Any = LinearRegression(**params)
        return Pipeline([("select", select), ("scale", StandardScaler()), ("model", model)])
    if kind == "ridge":
        return Pipeline(
            [("select", select), ("scale", StandardScaler()), ("model", Ridge(**{"alpha": 1.0, **params}))]
        )
    if kind == "random_forest":
        return Pipeline(
            [
                ("select", select),
                (
                    "model",
                    RandomForestRegressor(
                        random_state=seed, n_jobs=1, **{"n_estimators": 200, "min_samples_leaf": 2, **params}
                    ),
                ),
            ]
        )
    if kind == "gradient_boosting":
        return Pipeline(
            [
                ("select", select),
                (
                    "model",
                    GradientBoostingRegressor(
                        random_state=seed,
                        **{"n_estimators": 200, "max_depth": 2, "learning_rate": 0.05, **params},
                    ),
                ),
            ]
        )
    raise ValueError(f"unknown sklearn kind {kind}")


@dataclass(frozen=True)
class MethodSpec:
    name: str
    family: str  # naive | spread_rule | blend | linear | ridge | random_forest | gradient_boosting
    params: dict[str, Any] = field(default_factory=dict)
    features: list[str] = field(default_factory=list)  # ML methods only
    description: str = ""

    def build(self, seed: int = 42) -> Any:
        if self.family == "naive":
            return NaiveLast()
        if self.family == "spread_rule":
            return SpreadRule(**self.params)
        if self.family == "blend":
            return BenchmarkBlend()
        feats = self.features or FEATURE_COLUMNS
        unknown = set(feats) - set(FEATURE_COLUMNS)
        if unknown:
            raise ValueError(f"{self.name}: unknown features {sorted(unknown)}")
        return _sklearn(self.family, feats, dict(self.params), seed)

    @property
    def requires(self) -> list[str]:
        if self.family == "naive":
            return list(NaiveLast.requires)
        if self.family == "spread_rule":
            return [BENCH[self.params.get("benchmark", "wti")]]
        if self.family == "blend":
            return list(BenchmarkBlend.requires)
        return list(self.features or FEATURE_COLUMNS)

    @property
    def benchmarks_used(self) -> list[str]:
        """Which benchmark inputs the method needs for a target month (drives scenario inputs)."""
        req = set(self.requires)
        return [
            b
            for b, col in BENCH.items()
            if col in req or f"{b}_lag_1" in req or f"spread_{b}_lag_1" in req or f"spread_{b}_mean_3" in req
        ]

    @property
    def is_rule_based(self) -> bool:
        return self.family in ("naive", "spread_rule", "blend")


DEFAULT_METHODS: tuple[MethodSpec, ...] = (
    MethodSpec("naive_last", "naive", description="ICP bulan lalu (acuan paling sederhana)."),
    MethodSpec(
        "spread_wti_all",
        "spread_rule",
        {"benchmark": "wti", "window": None},
        description="WTI + rata-rata selisih seluruh riwayat.",
    ),
    MethodSpec(
        "spread_wti_12m",
        "spread_rule",
        {"benchmark": "wti", "window": 12},
        description="WTI + rata-rata selisih 12 bulan terakhir.",
    ),
    MethodSpec(
        "spread_brent_12m",
        "spread_rule",
        {"benchmark": "brent", "window": 12},
        description="Brent + rata-rata selisih 12 bulan terakhir.",
    ),
    MethodSpec(
        "spread_brent_6m",
        "spread_rule",
        {"benchmark": "brent", "window": 6},
        description="Brent + rata-rata selisih 6 bulan terakhir.",
    ),
    MethodSpec(
        "spread_dubai_12m",
        "spread_rule",
        {"benchmark": "dubai", "window": 12},
        description="Dubai + rata-rata selisih 12 bulan terakhir.",
    ),
    MethodSpec(
        "spread_dubai_6m",
        "spread_rule",
        {"benchmark": "dubai", "window": 6},
        description="Dubai + rata-rata selisih 6 bulan terakhir.",
    ),
    MethodSpec(
        "blend_benchmarks", "blend", description="Kombinasi tertimbang Brent/Dubai/WTI (bobot non-negatif)."
    ),
    MethodSpec(
        "ridge_benchmarks",
        "ridge",
        {"alpha": 3.0},
        ["brent_price", "dubai_price", "wti_price", "icp_lag_1", "spread_brent_lag_1"],
        "Regresi ridge: benchmark bulan ini + ICP bulan lalu + selisih terakhir.",
    ),
    MethodSpec(
        "ridge_full",
        "ridge",
        {"alpha": 10.0},
        FEATURE_COLUMNS,
        "Regresi ridge dengan seluruh fitur (termasuk kurs).",
    ),
    MethodSpec(
        "gbm_benchmarks",
        "gradient_boosting",
        {},
        ["brent_price", "dubai_price", "wti_price", "icp_lag_1", "spread_brent_lag_1"],
        "Gradient boosting pada benchmark + lag ICP.",
    ),
)


def find_method(name: str, methods: tuple[MethodSpec, ...] = DEFAULT_METHODS) -> MethodSpec:
    for m in methods:
        if m.name == name:
            return m
    raise KeyError(name)
