"""Backtest every candidate method, log the evidence to MLflow, register the winner.

Run structure in MLflow (experiment from the training config):

* parent run ``training-<job id or timestamp>`` - config, dataset provenance, leaderboard,
  the complete backtest tables (``evaluation/backtest_metrics.csv``, ``.../backtest_predictions.csv``)
* one nested run per method - params, horizon-specific metrics in both input modes, its own
  backtest predictions, empirical interval table, model card and the model artifact (fitted on
  every complete row up to the last official month)

Selection is by rolling-origin error on the rows *all* methods produced (see ``config.selection``),
with a 2% preference for rule-based methods over ML when they are that close. Only the winner is
registered. Registration never moves the production alias; promotion is a separate, validated
step (``registry.validate_candidate``).
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import mlflow
import numpy as np
import pandas as pd
import sklearn
import yaml
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient

from icp_ml.backtest import (
    BacktestResult,
    common_subset,
    interval_tables,
    leaderboard,
    rolling_origin,
    split_coverage,
    summarize,
)
from icp_ml.config import TrainingConfig
from icp_ml.features import FEATURE_COLUMNS, TARGET_COLUMN, build_feature_frame
from icp_ml.methods import DEFAULT_METHODS, MethodSpec

# skops serialization and the trusted-type allow-list live in icp_ml.serialization (shared with the
# exported-bundle registry); SKOPS_ALLOWED_TYPES stays importable from here.
from icp_ml.serialization import skops_trusted_types

logger = logging.getLogger(__name__)

NAIVE = next(m for m in DEFAULT_METHODS if m.family == "naive")
RULE_PREFERENCE_PCT = 2.0
CORE_INPUTS = ["wti_price", "brent_price", "dubai_price", "icp_lag_1"]


@dataclass(frozen=True)
class DatasetRef:
    """Provenance of a dataset used for training."""

    name: str
    version_id: str
    sha256: str


@dataclass
class CandidateResult:
    name: str
    family: str
    run_id: str
    score: float
    metrics: dict[str, float]
    params: dict[str, Any]
    rule_based: bool


@dataclass
class TrainingResult:
    experiment_id: str
    parent_run_id: str
    n_rows: int
    n_origins: int
    fit_period: tuple[str, str]
    backtest_period: tuple[str, str]
    naive: dict[str, float]
    candidates: list[CandidateResult] = field(default_factory=list)
    best: CandidateResult | None = None
    registered_version: str | None = None
    selection: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class TrainingDataError(ValueError):
    pass


def ensure_experiment(client: MlflowClient, name: str, artifact_root: str | None) -> str:
    exp = client.get_experiment_by_name(name)
    if exp is not None:
        if exp.lifecycle_stage == "deleted":
            client.restore_experiment(exp.experiment_id)
        return exp.experiment_id
    location = None
    if artifact_root:
        location = artifact_root if "://" in artifact_root else Path(artifact_root).resolve().as_uri()
    return client.create_experiment(name, artifact_location=location)


def git_commit() -> str | None:
    """Commit of the training code, for provenance (``GIT_COMMIT`` env var wins, e.g. in CI)."""
    if os.getenv("GIT_COMMIT"):
        return os.environ["GIT_COMMIT"]
    git = shutil.which("git")
    if git is None:
        return None
    try:
        out = subprocess.run(  # noqa: S603 - fixed argv
            [git, "rev-parse", "HEAD"], capture_output=True, text=True, timeout=5, check=False
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return (out.stdout.strip() or None) if out.returncode == 0 else None


def _pip_requirements() -> list[str]:
    import skops

    return [
        f"scikit-learn=={sklearn.__version__}",
        f"pandas=={pd.__version__}",
        f"numpy=={np.__version__}",
        f"skops=={skops.__version__}",
        f"scipy=={__import__('scipy').__version__}",
    ]


def check_training_data(panel: pd.DataFrame, config: TrainingConfig) -> pd.DataFrame:
    frame = build_feature_frame(panel, CORE_INPUTS)
    if len(frame) < config.min_feature_rows:
        raise TrainingDataError(
            f"Only {len(frame)} complete rows (ICP + same-month WTI/Brent/Dubai) available; at least {config.min_feature_rows} required. "
            "Missing months in any input remove rows (lags are calendar-aware)."
        )
    return frame


def _metric_dict(metrics: pd.DataFrame, method: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for r in metrics[metrics["method"] == method].itertuples():
        for k in ("mae", "rmse", "bias", "smape", "mase", "dir_acc", "n"):
            v = getattr(r, k)
            if v is not None and not (isinstance(v, float) and np.isnan(v)):
                out[f"bt_{r.mode}_h{int(r.horizon)}_{k}"] = float(v)
    return out


def select_best(board: pd.DataFrame, methods: dict[str, MethodSpec]) -> str:
    valid = board.dropna(subset=["score"])
    if valid.empty:
        raise TrainingDataError("No method produced forecasts for every selection horizon.")
    best = valid.iloc[0]
    if not methods[best["method"]].is_rule_based:
        limit = best["score"] * (1 + RULE_PREFERENCE_PCT / 100)
        rules = valid[[methods[m].is_rule_based for m in valid["method"]] & (valid["score"] <= limit)]
        if not rules.empty:
            return str(rules.iloc[0]["method"])
    return str(best["method"])


def model_card(
    spec: MethodSpec,
    config: TrainingConfig,
    metrics: pd.DataFrame,
    naive: pd.DataFrame,
    fit_period: tuple[str, str],
    n_rows: int,
    origins: list[str],
    intervals: dict[str, Any],
    coverage: dict[str, Any],
    datasets: list[DatasetRef],
    extra: dict[str, Any],
) -> str:
    def table(mode: str) -> str:
        m = metrics[metrics["mode"] == mode].set_index("horizon")
        nv = naive[naive["mode"] == mode].set_index("horizon")
        lines = [
            "| Horizon | n | MAE | RMSE | Bias | sMAPE % | MASE | Direction acc. | Naive MAE |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        for h in sorted(m.index):
            r = m.loc[h]
            nm = nv.loc[h, "mae"] if h in nv.index else float("nan")
            lines.append(
                f"| {h} | {int(r['n'])} | {r['mae']:.2f} | {r['rmse']:.2f} | {r['bias']:+.2f} | {r['smape']:.2f} | {r['mase']:.2f} | "
                f"{(r['dir_acc'] * 100 if not np.isnan(r['dir_acc']) else float('nan')):.0f}% | {nm:.2f} |"
            )
        return "\n".join(lines)

    ds = (
        "\n".join(f"- `{d.name}`: version `{d.version_id}`, sha256 `{d.sha256[:16]}…`" for d in datasets)
        or "- (none recorded)"
    )
    cov = (
        json.dumps(coverage, indent=2)
        if coverage
        else "not enough origins for an out-of-sample coverage check"
    )
    return f"""# Model card - {spec.name}

**Family:** {spec.family} (rule-based: {spec.is_rule_based})
**Description:** {spec.description or "-"}
**Parameters:** `{json.dumps(spec.params)}`
**Inputs required:** `{json.dumps(spec.requires)}` (benchmarks used: {", ".join(spec.benchmarks_used) or "none"})
**Fitted on:** {n_rows} complete monthly rows, {fit_period[0]}..{fit_period[1]}
**Backtest:** {len(origins)} rolling origins ({origins[0]}..{origins[-1]}), refit at every origin, horizons {list(config.backtest.horizons)}
**Selection rule:** {config.selection.metric} averaged over horizons {list(config.selection.horizons)} in `{config.selection.mode}` mode, on rows all methods produced
**Code commit:** {extra.get("git_commit") or "unknown"} - config sha256 `{config.sha256[:16]}…`

## Intended use
Target: the official monthly Indonesian Crude Price (USD/bbl) published by ESDM. Two situations:

1. **Estimate for an elapsed month awaiting publication** - the month's benchmark averages are
   observed. Use the `observed` errors below.
2. **Future month** - benchmarks are unknown; the output is conditional on a scenario (or on the
   last known benchmark level when no scenario is given). Use the `hold_last` errors below, which
   are typically close to naive persistence: without a benchmark view, no method forecasts ICP
   much better than "same as last month".

Not intended for intra-month trading decisions or as a substitute for the official decree.

## Backtest errors - observed benchmarks (elapsed month)
{table("observed")}

## Backtest errors - benchmarks held at last known value (true future, no scenario)
{table("hold_last")}

## Prediction intervals
Empirical quantiles of the backtest errors per mode and horizon (levels {list(config.interval_levels)}), stored in
`evaluation/intervals.json`. Out-of-sample coverage (intervals fitted on the first 60% of origins, checked on the rest):

```json
{cov}
```

## Training data
{ds}

## Limitations
- Monthly series with few years of history; errors are dominated by regime shifts (2020, 2022, 2026).
- Multi-step paths reuse the method's own earlier predictions for ICP lags (errors compound).
- Benchmarks are monthly averages from public sources (see docs/DATA_SOURCES.md); revisions are rare but possible.
- Historical ICP values are only as good as their verification status (see docs/DATA_RECONCILIATION.md).
"""


def train_and_register(
    panel: pd.DataFrame,
    config: TrainingConfig,
    tracking_uri: str,
    artifact_root: str | None,
    datasets: list[DatasetRef],
    job_id: str | None = None,
    register: bool = True,
) -> TrainingResult:
    # MLflow otherwise records the *names* of environment variables it sees during logging into the
    # model artifact (e.g. our secret variable names); the model needs none of them.
    os.environ["MLFLOW_RECORD_ENV_VARS_IN_MODEL_LOGGING"] = "false"
    core = check_training_data(panel, config)
    methods = tuple(config.methods) + (() if any(m.family == "naive" for m in config.methods) else (NAIVE,))
    by_name = {m.name: m for m in methods}
    bt: BacktestResult = rolling_origin(panel, methods, config.backtest)
    if bt.predictions.empty:
        raise TrainingDataError("The rolling-origin backtest produced no forecasts (too little history).")
    common = common_subset(bt.predictions)
    metrics_common = summarize(common)
    board = leaderboard(
        metrics_common, config.selection.mode, config.selection.horizons, config.selection.metric
    )
    naive_name = next(m.name for m in methods if m.family == "naive")
    naive_metrics = metrics_common[metrics_common["method"] == naive_name]
    best_name = select_best(board, by_name)
    last_official = panel[TARGET_COLUMN].dropna().index.max()

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_registry_uri(tracking_uri)  # the registry follows the tracking store of *this* run
    client = MlflowClient(tracking_uri=tracking_uri)
    experiment_id = ensure_experiment(client, config.experiment_name, artifact_root)
    commit = git_commit()
    common_tags = {
        "icp.training_job_id": job_id or "",
        "icp.git_commit": commit or "unknown",
        "icp.config_sha256": config.sha256,
        "icp.features": json.dumps(FEATURE_COLUMNS),
        "icp.last_official_period": str(last_official),
        "icp.backtest_origins": f"{bt.origins[0]}..{bt.origins[-1]}" if bt.origins else "",
        "icp.selection": f"{config.selection.metric}@{config.selection.mode}:h{','.join(map(str, config.selection.horizons))}",
    }
    for ds in datasets:
        common_tags[f"icp.dataset.{ds.name}.version_id"] = ds.version_id
        common_tags[f"icp.dataset.{ds.name}.sha256"] = ds.sha256

    result = TrainingResult(
        experiment_id,
        "",
        len(core),
        len(bt.origins),
        (str(core.index.min()), str(core.index.max())),
        (bt.origins[0], bt.origins[-1]) if bt.origins else ("", ""),
        {f"{r.mode}_h{int(r.horizon)}_mae": float(r.mae) for r in naive_metrics.itertuples()},
        selection=common_tags["icp.selection"],
    )

    run_name = f"training-{job_id}" if job_id else "training-manual"
    with mlflow.start_run(
        experiment_id=experiment_id, run_name=run_name, tags={**common_tags, "icp.run_kind": "parent"}
    ) as parent:
        result.parent_run_id = parent.info.run_id
        mlflow.log_text(
            yaml.safe_dump(config.to_dict(), sort_keys=False, default_flow_style=False),
            "config/training.yaml",
        )
        mlflow.log_params(
            {
                "n_rows": len(core),
                "n_origins": len(bt.origins),
                "n_methods": len(methods),
                "random_seed": config.random_seed,
            }
        )
        mlflow.log_text(bt.metrics.to_csv(index=False), "evaluation/backtest_metrics.csv")
        mlflow.log_text(metrics_common.to_csv(index=False), "evaluation/backtest_metrics_common.csv")
        mlflow.log_text(bt.predictions.to_csv(index=False), "evaluation/backtest_predictions.csv")
        mlflow.log_dict(
            {
                "leaderboard": board.to_dict(orient="records"),
                "skipped_origins": bt.skipped,
                "best": best_name,
            },
            "evaluation/leaderboard.json",
        )
        for r in naive_metrics.itertuples():
            mlflow.log_metric(f"naive_{r.mode}_h{int(r.horizon)}_mae", float(r.mae))

        fitted: dict[str, Any] = {}
        for spec in methods:
            score_row = board[board["method"] == spec.name]
            score = float(score_row["score"].iloc[0]) if len(score_row) else float("nan")
            frame = build_feature_frame(panel, spec.requires)
            with mlflow.start_run(
                experiment_id=experiment_id,
                run_name=spec.name,
                nested=True,
                tags={
                    **common_tags,
                    "icp.run_kind": "candidate",
                    "icp.candidate": spec.name,
                    "icp.method": spec.name,
                    "icp.method_family": spec.family,
                    "icp.method_spec": json.dumps(asdict(spec)),
                    "icp.method_requires": json.dumps(spec.requires),
                    "icp.benchmarks_used": json.dumps(spec.benchmarks_used),
                    "icp.fit_period": f"{frame.index.min()}..{frame.index.max()}",
                    "icp.rule_based": str(spec.is_rule_based).lower(),
                },
            ) as child:
                params = {
                    "family": spec.family,
                    "rule_based": spec.is_rule_based,
                    **{f"hp_{k}": v for k, v in spec.params.items()},
                    "features": ",".join(spec.features) if spec.features else "",
                    "n_fit_rows": len(frame),
                }
                mlflow.log_params(params)
                own = _metric_dict(bt.metrics, spec.name)
                own_common = {f"common_{k}": v for k, v in _metric_dict(metrics_common, spec.name).items()}
                h1 = bt.metrics[
                    (bt.metrics["method"] == spec.name)
                    & (bt.metrics["mode"] == "observed")
                    & (bt.metrics["horizon"] == 1)
                ]
                hl1 = bt.metrics[
                    (bt.metrics["method"] == spec.name)
                    & (bt.metrics["mode"] == "hold_last")
                    & (bt.metrics["horizon"] == 1)
                ]
                nv1 = naive_metrics[(naive_metrics["mode"] == "observed") & (naive_metrics["horizon"] == 1)]
                nvh1 = naive_metrics[(naive_metrics["mode"] == "hold_last") & (naive_metrics["horizon"] == 1)]
                compat = {
                    "score": score,
                    "mae": float(h1["mae"].iloc[0]) if len(h1) else float("nan"),
                    "rmse": float(h1["rmse"].iloc[0]) if len(h1) else float("nan"),
                    "smape": float(h1["smape"].iloc[0]) if len(h1) else float("nan"),
                    "hold_last_h1_mae": float(hl1["mae"].iloc[0]) if len(hl1) else float("nan"),
                    "baseline_naive_last_mae": float(nv1["mae"].iloc[0]) if len(nv1) else float("nan"),
                    "baseline_naive_last_rmse": float(nv1["rmse"].iloc[0]) if len(nv1) else float("nan"),
                    "baseline_naive_hold_last_h1_mae": float(nvh1["mae"].iloc[0])
                    if len(nvh1)
                    else float("nan"),
                }
                mlflow.log_metrics(
                    {k: v for k, v in {**own, **own_common, **compat}.items() if not np.isnan(v)}
                )
                own_preds = bt.predictions[bt.predictions["method"] == spec.name]
                mlflow.log_text(own_preds.to_csv(index=False), "evaluation/backtest_predictions.csv")
                mlflow.log_text(
                    bt.metrics[bt.metrics["method"] == spec.name].to_csv(index=False),
                    "evaluation/backtest_metrics.csv",
                )
                intervals = interval_tables(bt.predictions, spec.name, config.interval_levels)
                coverage = split_coverage(bt.predictions, spec.name, config.interval_levels)
                mlflow.log_dict(intervals, "evaluation/intervals.json")
                mlflow.log_dict(coverage, "evaluation/interval_coverage.json")
                for mode, hs in coverage.items():
                    for h, cov in hs.items():
                        for lv, c in cov.items():
                            if lv != "n":
                                mlflow.log_metric(f"coverage_{mode}_h{h}_p{lv.replace('0.', '')}", float(c))

                final = spec.build(config.random_seed).fit(frame[FEATURE_COLUMNS], frame[TARGET_COLUMN])
                x_example = frame[FEATURE_COLUMNS].tail(3)
                signature = infer_signature(x_example, np.asarray(final.predict(x_example), dtype=float))
                model_info = mlflow.sklearn.log_model(
                    sk_model=final,
                    name="model",
                    signature=signature,
                    input_example=x_example.iloc[:1],
                    pip_requirements=_pip_requirements(),
                    serialization_format="skops",
                    skops_trusted_types=skops_trusted_types(final),
                )
                card = model_card(
                    spec,
                    config,
                    bt.metrics[bt.metrics["method"] == spec.name],
                    bt.metrics[bt.metrics["method"] == naive_name],
                    (str(frame.index.min()), str(frame.index.max())),
                    len(frame),
                    bt.origins,
                    intervals,
                    coverage,
                    datasets,
                    {"git_commit": commit},
                )
                mlflow.log_text(card, "model_card.md")
                if spec.family == "spread_rule":
                    mlflow.log_metric("fitted_spread", float(final.spread_))
                if spec.family == "blend":
                    mlflow.log_dict(
                        {
                            "weights": dict(zip(final.requires, map(float, final.weights_), strict=True)),
                            "intercept": final.intercept_,
                        },
                        "evaluation/blend_weights.json",
                    )
                fitted[spec.name] = model_info
                result.candidates.append(
                    CandidateResult(
                        spec.name,
                        spec.family,
                        child.info.run_id,
                        score,
                        {**compat},
                        params,
                        spec.is_rule_based,
                    )
                )
                logger.info("method %s score=%.4f h1 mae=%.4f", spec.name, score, compat["mae"])

        best = next(c for c in result.candidates if c.name == best_name)
        result.best = best
        mlflow.log_metrics({"best_score": best.score, "best_h1_mae": best.metrics["mae"]})
        mlflow.set_tag("icp.best_candidate", best.name)

        if register:
            spec = by_name[best.name]
            frame = build_feature_frame(panel, spec.requires)
            mv = mlflow.register_model(
                fitted[best.name].model_uri,
                config.model_name,
                tags={
                    "icp.run_id": best.run_id,
                    "icp.parent_run_id": parent.info.run_id,
                    "icp.candidate": best.name,
                    "icp.method": best.name,
                    "icp.method_family": spec.family,
                    "icp.method_spec": json.dumps(asdict(spec)),
                    "icp.method_requires": json.dumps(spec.requires),
                    "icp.benchmarks_used": json.dumps(spec.benchmarks_used),
                    "icp.rule_based": str(spec.is_rule_based).lower(),
                    "icp.fit_period": f"{frame.index.min()}..{frame.index.max()}",
                    "icp.score": f"{best.score:.6f}",
                    "icp.h1_mae": f"{best.metrics['mae']:.6f}",
                    "icp.baseline_naive_last_mae": f"{best.metrics['baseline_naive_last_mae']:.6f}",
                    **common_tags,
                },
            )
            result.registered_version = str(mv.version)
            mlflow.set_tag("icp.registered_version", mv.version)
    return result
