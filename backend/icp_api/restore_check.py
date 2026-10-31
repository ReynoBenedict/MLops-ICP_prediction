"""Read-only verification of a local backup (``icp-api restore-check <backup-dir>``, audit v7 OPS-01).

A backup written by ``icp-api backup`` (``var/backups/<UTC stamp>/``) contains ``metadata.db``,
optionally ``mlflow.db``, ``objects/`` and ``mlflow-artifacts/``. This check never restores and never
writes: SQLite files are opened with ``mode=ro&immutable=1`` (no journal, WAL or ``-shm`` files are
created in the backup), the live database with ``mode=ro``. It reports

* ``PRAGMA integrity_check`` and the schema version of each database,
* row counts of the allow-listed tables of each database (metadata and MLflow have separate lists;
  other tables are not counted, only their number is reported), and the difference from the live
  metadata store and live MLflow store (informational),
* that the MLflow history tables exist in the backup (experiments, runs, registered_models,
  model_versions) - losing them is a failure,
* SHA-256 of every dataset object referenced by ``dataset_versions`` (missing or altered = failure),
* the exported model bundle the backup points at, verified against its sha256 manifest when present.

``ok`` is true when the backup could be restored as-is: both databases pass ``integrity_check``, the
schema version is known to this code, the MLflow history tables are present, and every referenced
dataset object is present and intact.

Run it as ``icp-api restore-check <dir> [--live-db ...]`` or, with every option,
``python -m icp_api.restore_check <dir> [--live-db ...] [--live-mlflow ...]``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

from icp_api.storage.migrations import expected_version


def _open_ro(path: Path, immutable: bool) -> sqlite3.Connection:
    uri = f"{path.resolve().as_uri()}?mode=ro" + ("&immutable=1" if immutable else "")
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    return [str(r["name"]) for r in rows]


# Constant statements only (no SQL is built from schema names). Tables missing from a list are not
# counted (their number is reported); add new migrations' tables to keep the comparison complete.
_COUNT_SQL: dict[str, str] = {
    "alert_events": "SELECT COUNT(*) FROM alert_events",
    "alert_rules": "SELECT COUNT(*) FROM alert_rules",
    "api_keys": "SELECT COUNT(*) FROM api_keys",
    "app_schema": "SELECT COUNT(*) FROM app_schema",
    "audit_events": "SELECT COUNT(*) FROM audit_events",
    "dataset_versions": "SELECT COUNT(*) FROM dataset_versions",
    "deletion_requests": "SELECT COUNT(*) FROM deletion_requests",
    "forecast_runs": "SELECT COUNT(*) FROM forecast_runs",
    "ingestion_runs": "SELECT COUNT(*) FROM ingestion_runs",
    "jobs": "SELECT COUNT(*) FROM jobs",
    "model_promotions": "SELECT COUNT(*) FROM model_promotions",
    "organizations": "SELECT COUNT(*) FROM organizations",
    "plan_change_requests": "SELECT COUNT(*) FROM plan_change_requests",
    "report_runs": "SELECT COUNT(*) FROM report_runs",
    "saved_scenarios": "SELECT COUNT(*) FROM saved_scenarios",
    "sessions": "SELECT COUNT(*) FROM sessions",
    "source_documents": "SELECT COUNT(*) FROM source_documents",
    "source_evidence": "SELECT COUNT(*) FROM source_evidence",
    "usage_events": "SELECT COUNT(*) FROM usage_events",
    "users": "SELECT COUNT(*) FROM users",
}


# MLflow tracking store (SQLite backend): the tables that carry the experiment/registry history.
_MLFLOW_COUNT_SQL: dict[str, str] = {
    "experiments": "SELECT COUNT(*) FROM experiments",
    "latest_metrics": "SELECT COUNT(*) FROM latest_metrics",
    "metrics": "SELECT COUNT(*) FROM metrics",
    "model_versions": "SELECT COUNT(*) FROM model_versions",
    "params": "SELECT COUNT(*) FROM params",
    "registered_model_aliases": "SELECT COUNT(*) FROM registered_model_aliases",
    "registered_models": "SELECT COUNT(*) FROM registered_models",
    "runs": "SELECT COUNT(*) FROM runs",
    "tags": "SELECT COUNT(*) FROM tags",
}
MLFLOW_REQUIRED = ("experiments", "runs", "registered_models", "model_versions")


def _counts(conn: sqlite3.Connection, allow: dict[str, str]) -> tuple[dict[str, int], int]:
    out: dict[str, int] = {}
    skipped = 0
    for table in _tables(conn):
        sql = allow.get(table)
        if sql is None:
            skipped += 1
            continue
        out[table] = int(conn.execute(sql).fetchone()[0])
    return out, skipped


def _diff(backup: dict[str, Any], live: dict[str, Any]) -> dict[str, dict[str, int | None]]:
    b_counts, l_counts = backup.get("row_counts", {}), live.get("row_counts", {})
    return {
        t: {"backup": b_counts.get(t), "live": l_counts.get(t)}
        for t in sorted(set(b_counts) | set(l_counts))
        if b_counts.get(t) != l_counts.get(t)
    }


def _sqlite_report(
    path: Path, immutable: bool = True, allow: dict[str, str] = _COUNT_SQL
) -> tuple[dict[str, Any], sqlite3.Connection | None]:
    if not path.is_file():
        return {"present": False, "error": f"{path.name} not found"}, None
    try:
        conn = _open_ro(path, immutable)
        integrity = [str(r[0]) for r in conn.execute("PRAGMA integrity_check").fetchall()]
        counts, skipped = _counts(conn, allow)
        report: dict[str, Any] = {
            "present": True,
            "integrity": "ok" if integrity == ["ok"] else "; ".join(integrity[:5]),
            "row_counts": counts,
            "tables_not_counted": skipped,
        }
        return report, conn
    except sqlite3.Error as exc:
        return {"present": True, "error": f"{type(exc).__name__}: {exc}"}, None


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _objects_report(objects: Path, conn: sqlite3.Connection | None) -> dict[str, Any]:
    report: dict[str, Any] = {"present": objects.is_dir(), "datasets_verified": 0, "datasets_bad": []}
    if conn is None:
        return report
    try:
        rows = conn.execute("SELECT id, object_key, sha256 FROM dataset_versions ORDER BY id").fetchall()
    except sqlite3.Error:
        rows = []
    for r in rows:
        path = objects / str(r["object_key"])
        if not path.is_file():
            report["datasets_bad"].append({"id": r["id"], "key": r["object_key"], "problem": "missing"})
        elif _sha256(path) != r["sha256"]:
            report["datasets_bad"].append(
                {"id": r["id"], "key": r["object_key"], "problem": "sha256 mismatch"}
            )
        else:
            report["datasets_verified"] += 1
    pointer = objects / "models" / "production.json"
    if pointer.is_file():
        bundle: dict[str, Any] = {}
        try:
            ptr = json.loads(pointer.read_text(encoding="utf-8"))
            version = str(ptr.get("version"))
            bundle["version"] = version
            folder = objects / "models" / "bundles" / version
            meta_path = folder / "meta.json"
            meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.is_file() else {}
            bundle["model_present"] = (folder / "model.skops").is_file()
            manifest = meta.get("files")
            if manifest:
                bad = [
                    name
                    for name, digest in manifest.items()
                    if not (folder / name).is_file() or _sha256(folder / name) != digest
                ]
                bundle["manifest"] = "ok" if not bad else f"mismatch: {', '.join(sorted(bad))}"
            else:
                bundle["manifest"] = "absent (exported before the sha256 manifest)"
        except (OSError, ValueError) as exc:
            bundle["error"] = f"{type(exc).__name__}: {exc}"
        report["model_bundle"] = bundle
    return report


def restore_check(
    backup_dir: Path, live_db: Path | None = None, live_mlflow: Path | None = None
) -> dict[str, Any]:
    backup_dir = Path(backup_dir)
    report: dict[str, Any] = {"backup_dir": str(backup_dir), "expected_schema_version": expected_version()}
    meta, conn = _sqlite_report(backup_dir / "metadata.db")
    if conn is not None:
        try:
            row = conn.execute("SELECT MAX(version) FROM app_schema").fetchone()
            meta["schema_version"] = int(row[0] or 0)
        except sqlite3.Error as exc:
            meta["schema_version"] = None
            meta["error"] = f"no app_schema: {exc}"
    report["metadata"] = meta
    mlflow_path = backup_dir / "mlflow.db"
    if mlflow_path.is_file():
        ml, ml_conn = _sqlite_report(mlflow_path, allow=_MLFLOW_COUNT_SQL)
        if ml_conn is not None:
            ml["missing_history_tables"] = [t for t in MLFLOW_REQUIRED if t not in _tables(ml_conn)]
            ml_conn.close()
        report["mlflow"] = ml
    report["objects"] = _objects_report(backup_dir / "objects", conn)
    if conn is not None:
        conn.close()
    if live_db is not None:
        live, live_conn = _sqlite_report(Path(live_db), immutable=False)
        if live_conn is not None:
            live_conn.close()
        report["comparison"] = {
            "live_db": str(live_db),
            "live_integrity": live.get("integrity") or live.get("error"),
            "row_count_diff": _diff(meta, live),
        }
    if live_mlflow is not None:
        live_ml, live_ml_conn = _sqlite_report(Path(live_mlflow), immutable=False, allow=_MLFLOW_COUNT_SQL)
        if live_ml_conn is not None:
            live_ml_conn.close()
        report["mlflow_comparison"] = {
            "live_mlflow": str(live_mlflow),
            "live_integrity": live_ml.get("integrity") or live_ml.get("error"),
            "row_count_diff": _diff(report.get("mlflow") or {}, live_ml),
        }
    problems = []
    if meta.get("integrity") != "ok":
        problems.append("metadata.db failed integrity_check or is missing")
    version = meta.get("schema_version")
    if version is None or not 1 <= int(version) <= expected_version():
        problems.append(f"schema version {version} is not usable by this code (expects {expected_version()})")
    if "mlflow" in report and report["mlflow"].get("integrity") != "ok":
        problems.append("mlflow.db failed integrity_check")
    missing_history = (report.get("mlflow") or {}).get("missing_history_tables") or []
    if missing_history:
        problems.append(f"MLflow history tables missing from mlflow.db: {', '.join(missing_history)}")
    if report["objects"]["datasets_bad"]:
        problems.append(f"{len(report['objects']['datasets_bad'])} dataset object(s) missing or altered")
    bundle = report["objects"].get("model_bundle") or {}
    if bundle.get("error") or str(bundle.get("manifest", "")).startswith("mismatch"):
        problems.append("model bundle does not verify")
    report["problems"] = problems
    report["ok"] = not problems
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m icp_api.restore_check", description="Read-only verification of a local backup."
    )
    parser.add_argument("backup_dir")
    parser.add_argument("--live-db", default=None, help="live metadata.db to compare with (read-only)")
    parser.add_argument(
        "--live-mlflow", default=None, help="live MLflow SQLite store to compare with (read-only)"
    )
    args = parser.parse_args(argv)
    report = restore_check(
        Path(args.backup_dir),
        Path(args.live_db) if args.live_db else None,
        Path(args.live_mlflow) if args.live_mlflow else None,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
