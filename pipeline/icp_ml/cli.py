"""Offline pipeline CLI (``icp-ml``). The backend uses the same modules for its jobs.

Examples::

    icp-ml validate                       # data-quality report for the input CSVs
    icp-ml features --out data/processed/features.csv
    icp-ml backtest --out var/backtest    # rolling-origin leaderboard for every method
    icp-ml train --no-register            # backtest + fit + log to MLFLOW_TRACKING_URI
    icp-ml ingest-icp-pdf --local         # re-extract ICP prices from local PDFs (optional extra)

Inputs are monthly CSVs in the layouts accepted by ``icp_ml.schema`` (``--icp`` is required for
training; ``--wti/--brent/--dubai/--fx`` are optional, methods needing a missing input are skipped
by the backtest).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

import pandas as pd

from icp_ml.config import load_config
from icp_ml.features import build_feature_frame, build_panel
from icp_ml.schema import load_series, parse_csv_bytes, sha256_bytes

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ICP = ROOT / "data" / "raw" / "dataset.csv"
DEFAULT_WTI = ROOT / "data" / "raw" / "wti.csv"
INPUTS = ("icp", "wti", "brent", "dubai", "fx")


def _inputs(args: argparse.Namespace) -> dict[str, Path]:
    out = {}
    for name in INPUTS:
        p = getattr(args, name, None)
        if p and Path(p).exists():
            out[name] = Path(p)
    if "icp" not in out:
        raise SystemExit(f"ICP dataset not found: {getattr(args, 'icp', None)}")
    return out


def _panel(paths: dict[str, Path]) -> pd.DataFrame:
    icp = load_series(paths["icp"].read_bytes(), "icp")
    bench: dict[str, pd.Series] = {
        k: load_series(paths[k].read_bytes(), k) for k in ("wti", "brent", "dubai") if k in paths
    }
    fx = load_series(paths["fx"].read_bytes(), "usd_idr") if "fx" in paths else None
    return build_panel(icp, bench, fx)


def _cmd_validate(args: argparse.Namespace) -> int:
    ok = True
    for name, path in _inputs(args).items():
        _, report = parse_csv_bytes(path.read_bytes(), "usd_idr" if name == "fx" else name)
        print(json.dumps({"input": name, **report.to_dict()}, indent=2))
        ok = ok and report.ok
    return 0 if ok else 1


def _cmd_features(args: argparse.Namespace) -> int:
    frame = build_feature_frame(_panel(_inputs(args)), ["icp_lag_1"])
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(out, index_label="period")
    print(f"{len(frame)} feature rows ({frame.index.min()}..{frame.index.max()}) -> {out}")
    return 0


def _cmd_backtest(args: argparse.Namespace) -> int:
    from icp_ml.backtest import common_subset, leaderboard, rolling_origin, summarize

    cfg = load_config(args.config)
    panel = _panel(_inputs(args))
    res = rolling_origin(panel, cfg.methods, cfg.backtest)
    common = summarize(common_subset(res.predictions))
    board = leaderboard(common, cfg.selection.mode, cfg.selection.horizons, cfg.selection.metric)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    res.predictions.to_csv(out / "backtest_predictions.csv", index=False)
    res.metrics.to_csv(out / "backtest_metrics.csv", index=False)
    board.to_csv(out / "leaderboard.csv", index=False)
    pd.set_option("display.width", 200)
    print(
        f"{len(res.origins)} origins {res.origins[0]}..{res.origins[-1]}; skipped: { {k: len(v) for k, v in res.skipped.items()} }"
    )
    print(board.to_string(index=False))
    for mode in cfg.backtest.modes:
        print(f"\nMAE by horizon - {mode}")
        print(
            common[common["mode"] == mode]
            .pivot(index="method", columns="horizon", values="mae")
            .round(2)
            .to_string()
        )
    return 0


def _cmd_train(args: argparse.Namespace) -> int:
    from icp_ml.training import DatasetRef, train_and_register

    paths = _inputs(args)
    tracking = os.getenv(
        "MLFLOW_TRACKING_URI", f"sqlite:///{(ROOT / 'var' / 'mlflow' / 'mlflow.db').as_posix()}"
    )
    artifacts = os.getenv("MLFLOW_ARTIFACT_ROOT", str(ROOT / "var" / "mlflow" / "artifacts"))
    if tracking.startswith("sqlite:///"):
        Path(tracking.removeprefix("sqlite:///")).parent.mkdir(parents=True, exist_ok=True)
    result = train_and_register(
        _panel(paths),
        load_config(args.config),
        tracking_uri=tracking,
        artifact_root=artifacts,
        datasets=[DatasetRef(name, "file", sha256_bytes(p.read_bytes())) for name, p in paths.items()],
        register=not args.no_register,
    )
    print(json.dumps(result.to_dict(), indent=2, default=str))
    return 0


def _cmd_ingest_pdf(args: argparse.Namespace) -> int:
    from icp_ml.ingestion import icp_pdf

    icp_pdf.main(["--local"] if args.local else [])
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(
        prog="icp-ml", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    def data_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--icp", default=str(DEFAULT_ICP))
        p.add_argument("--wti", default=str(DEFAULT_WTI))
        p.add_argument("--brent", default=None)
        p.add_argument("--dubai", default=None)
        p.add_argument("--fx", default=None, help="USD/IDR monthly average CSV")

    p = sub.add_parser("validate", help="print data-quality reports")
    data_args(p)
    p.set_defaults(func=_cmd_validate)
    p = sub.add_parser("features", help="export the calendar-aware feature frame")
    data_args(p)
    p.add_argument("--out", default=str(ROOT / "data" / "processed" / "features.csv"))
    p.set_defaults(func=_cmd_features)
    p = sub.add_parser("backtest", help="rolling-origin backtest of every configured method")
    data_args(p)
    p.add_argument("--config", default=None)
    p.add_argument("--out", default=str(ROOT / "var" / "backtest"))
    p.set_defaults(func=_cmd_backtest)
    p = sub.add_parser("train", help="backtest, fit, log and (optionally) register the winning method")
    data_args(p)
    p.add_argument("--config", default=None)
    p.add_argument("--no-register", action="store_true")
    p.set_defaults(func=_cmd_train)
    p = sub.add_parser("ingest-icp-pdf", help="download/extract ICP prices from ESDM PDFs")
    p.add_argument("--local", action="store_true", help="only use PDFs already in data/raw")
    p.set_defaults(func=_cmd_ingest_pdf)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
