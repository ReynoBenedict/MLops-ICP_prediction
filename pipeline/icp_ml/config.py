"""Training / backtest configuration loading and validation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml

from icp_ml.backtest import MODES, BacktestConfig
from icp_ml.features import FEATURE_COLUMNS
from icp_ml.methods import DEFAULT_METHODS, MethodSpec

SUPPORTED_FAMILIES = {
    "naive",
    "spread_rule",
    "blend",
    "linear",
    "ridge",
    "random_forest",
    "gradient_boosting",
}
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "configs" / "training.yaml"


@dataclass(frozen=True)
class SelectionPolicy:
    """How the winning method is chosen from the rolling-origin leaderboard."""

    mode: str = "observed"  # backtest mode the primary score is computed in
    horizons: tuple[int, ...] = (1, 2, 3)  # averaged over these horizons
    metric: str = "mae"


@dataclass(frozen=True)
class PromotionPolicy:
    require_beats_naive_baseline: bool = True
    # Candidate primary score may be at most this much worse (percent) than the production version.
    max_regression_pct: float = 5.0
    # In hold_last mode (no scenario) the 1-month error may not exceed naive by more than this (percent).
    max_hold_last_vs_naive_pct: float = 10.0


@dataclass(frozen=True)
class TrainingConfig:
    experiment_name: str
    model_name: str
    random_seed: int
    min_feature_rows: int
    methods: tuple[MethodSpec, ...]
    backtest: BacktestConfig
    selection: SelectionPolicy
    promotion: PromotionPolicy
    interval_levels: tuple[float, ...] = (0.5, 0.8, 0.95)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(json.dumps(self.to_dict(), sort_keys=True, default=str).encode()).hexdigest()


def _parse_methods(raw: Any) -> tuple[MethodSpec, ...]:
    if raw in (None, "default"):
        return DEFAULT_METHODS
    if not isinstance(raw, list) or not raw:
        raise ValueError("methods must be 'default' or a non-empty list")
    out: list[MethodSpec] = []
    for m in raw:
        if isinstance(m, str):  # reference to a default method by name
            match = [d for d in DEFAULT_METHODS if d.name == m]
            if not match:
                raise ValueError(f"Unknown default method '{m}'")
            out.append(match[0])
            continue
        family = str(m["family"])
        if family not in SUPPORTED_FAMILIES:
            raise ValueError(f"Unsupported method family '{family}'")
        feats = list(m.get("features") or [])
        unknown = set(feats) - set(FEATURE_COLUMNS)
        if unknown:
            raise ValueError(f"Method {m['name']} uses unknown features {sorted(unknown)}")
        out.append(
            MethodSpec(
                str(m["name"]), family, dict(m.get("params") or {}), feats, str(m.get("description", ""))
            )
        )
    if len({x.name for x in out}) != len(out):
        raise ValueError("Method names must be unique")
    return tuple(out)


def parse_config(raw: dict[str, Any]) -> TrainingConfig:
    bt = raw.get("backtest") or {}
    horizons = tuple(int(h) for h in bt.get("horizons", [1, 2, 3, 4, 5, 6]))
    if not horizons or min(horizons) < 1 or max(horizons) > 12:
        raise ValueError("backtest.horizons must be within 1..12")
    modes = tuple(str(m) for m in bt.get("modes", list(MODES)))
    if set(modes) - set(MODES):
        raise ValueError(f"backtest.modes must be a subset of {MODES}")
    sel = raw.get("selection") or {}
    selection = SelectionPolicy(
        str(sel.get("mode", "observed")),
        tuple(int(h) for h in sel.get("horizons", [1, 2, 3])),
        str(sel.get("metric", "mae")),
    )
    if selection.mode not in modes or set(selection.horizons) - set(horizons):
        raise ValueError("selection must use a backtested mode and horizons")
    if selection.metric not in ("mae", "rmse", "mase", "smape"):
        raise ValueError("selection.metric must be one of mae, rmse, mase, smape")
    promo = raw.get("promotion") or {}
    levels = tuple(float(x) for x in raw.get("interval_levels", [0.5, 0.8, 0.95]))
    if any(not 0 < lv < 1 for lv in levels):
        raise ValueError("interval_levels must be in (0, 1)")
    return TrainingConfig(
        experiment_name=str(raw.get("experiment_name", "icp-price-forecasting")),
        model_name=str(raw.get("model_name", "ICP_Price_Model")),
        random_seed=int(raw.get("random_seed", 42)),
        min_feature_rows=int(raw.get("min_feature_rows", 30)),
        methods=_parse_methods(raw.get("methods", "default")),
        backtest=BacktestConfig(
            horizons=horizons,
            min_train_rows=int(bt.get("min_train_rows", 24)),
            first_origin=(str(bt["first_origin"]) if bt.get("first_origin") else None),
            modes=modes,
            seed=int(raw.get("random_seed", 42)),
        ),
        selection=selection,
        promotion=PromotionPolicy(
            require_beats_naive_baseline=bool(promo.get("require_beats_naive_baseline", True)),
            max_regression_pct=float(
                promo.get("max_regression_pct", promo.get("max_rmse_regression_pct", 5.0))
            ),
            max_hold_last_vs_naive_pct=float(promo.get("max_hold_last_vs_naive_pct", 10.0)),
        ),
        interval_levels=levels,
    )


def load_config(path: str | Path | None = None) -> TrainingConfig:
    p = Path(path) if path else DEFAULT_CONFIG_PATH
    with open(p, encoding="utf-8") as fh:
        return parse_config(yaml.safe_load(fh) or {})
