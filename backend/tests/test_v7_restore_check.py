"""`icp-api restore-check <backup-dir>`: read-only verification of a local backup (audit v7 OPS-01)."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

from icp_api.restore_check import restore_check
from icp_api.storage.migrations import apply_local, expected_version
from icp_api.storage.sql import SqliteExecutor


def _live(tmp_path: Path) -> tuple[Path, Path]:
    db_path = tmp_path / "live" / "metadata.db"
    db = SqliteExecutor(db_path)
    apply_local(db)
    objects = tmp_path / "live" / "objects"
    data = b"period,icp_price\n2026-09,112.81\n"
    digest = hashlib.sha256(data).hexdigest()
    key = f"datasets/icp/{digest}.csv"
    (objects / key).parent.mkdir(parents=True)
    (objects / key).write_bytes(data)
    db.execute(
        "INSERT INTO dataset_versions (id, dataset, version, object_key, sha256, size_bytes, row_count, source, "
        "quality_json, is_active, created_at, created_by) VALUES ('dsv_1', 'icp', 1, ?, ?, ?, 1, 'test', '{}', 1, "
        "'2026-10-20T00:00:00.000Z', 'op')",
        (key, digest, len(data)),
    )
    return db_path, objects


def _backup(live_db: Path, live_objects: Path, dest: Path) -> Path:
    import shutil

    dest.mkdir(parents=True)
    src = sqlite3.connect(str(live_db))
    with sqlite3.connect(str(dest / "metadata.db")) as dst:
        src.backup(dst)
    src.close()
    shutil.copytree(live_objects, dest / "objects")
    return dest


def _files(root: Path) -> set[str]:
    return {str(p.relative_to(root)) for p in root.rglob("*")}


def test_a_good_backup_passes_and_is_compared_with_the_live_store(tmp_path):
    live_db, live_objects = _live(tmp_path)
    backup = _backup(live_db, live_objects, tmp_path / "backups" / "20261020T000000Z")
    before = _files(backup)
    report = restore_check(backup, live_db=live_db)
    assert report["ok"] is True, json.dumps(report, indent=2)
    assert report["metadata"]["integrity"] == "ok"
    assert report["metadata"]["schema_version"] == expected_version()
    assert report["metadata"]["row_counts"]["dataset_versions"] == 1
    assert report["objects"]["datasets_verified"] == 1 and report["objects"]["datasets_bad"] == []
    assert report["comparison"]["row_count_diff"] == {}
    assert _files(backup) == before  # strictly read-only: no journal, -shm or report files


def test_a_corrupted_dataset_object_fails_the_check(tmp_path):
    live_db, live_objects = _live(tmp_path)
    backup = _backup(live_db, live_objects, tmp_path / "b")
    obj = next((backup / "objects" / "datasets" / "icp").glob("*.csv"))
    obj.write_bytes(b"period,icp_price\n2026-09,99.99\n")
    report = restore_check(backup, live_db=live_db)
    assert report["ok"] is False
    assert report["objects"]["datasets_bad"][0]["problem"] == "sha256 mismatch"


def test_differences_from_the_live_store_are_reported(tmp_path):
    live_db, live_objects = _live(tmp_path)
    backup = _backup(live_db, live_objects, tmp_path / "b")
    SqliteExecutor(live_db).execute(
        "INSERT INTO audit_events (id, created_at, actor, action, outcome) VALUES ('a1', 'x', 'op', 't', 'success')"
    )
    report = restore_check(backup, live_db=live_db)
    assert report["ok"] is True  # differences are informational, not a backup defect
    assert report["comparison"]["row_count_diff"]["audit_events"] == {"backup": 0, "live": 1}


def test_a_missing_or_broken_metadata_file_fails(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    assert restore_check(empty)["ok"] is False
    broken = tmp_path / "broken"
    broken.mkdir()
    (broken / "metadata.db").write_bytes(b"not a database")
    report = restore_check(broken)
    assert report["ok"] is False and report["metadata"]["error"]


def test_cli_prints_the_report_and_sets_the_exit_code(tmp_path, capsys):
    from icp_api.cli import main

    live_db, live_objects = _live(tmp_path)
    backup = _backup(live_db, live_objects, tmp_path / "b")
    assert main(["restore-check", str(backup), "--live-db", str(live_db)]) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True
    assert main(["restore-check", str(tmp_path / "nope")]) == 1


# --- MLflow history (coordinator follow-up): own allow-list, skipped tables, live comparison --------------
MLFLOW_TABLES = {
    "experiments": 2,
    "runs": 3,
    "metrics": 5,
    "params": 4,
    "tags": 2,
    "registered_models": 1,
    "model_versions": 2,
    "registered_model_aliases": 1,
    "latest_metrics": 3,
}


def _mlflow_db(path: Path, extra_runs: int = 0) -> Path:
    conn = sqlite3.connect(str(path))
    for table, n in MLFLOW_TABLES.items():
        conn.execute(f"CREATE TABLE {table} (x INTEGER)")
        rows = n + (extra_runs if table == "runs" else 0)
        conn.executemany(f"INSERT INTO {table} (x) VALUES (?)", [(i,) for i in range(rows)])
    # tables outside the allow-list, incl. one whose name collides with a metadata table
    conn.execute("CREATE TABLE alembic_version (v TEXT)")
    conn.execute("CREATE TABLE jobs (x INTEGER)")
    conn.execute("INSERT INTO jobs (x) VALUES (1)")
    conn.commit()
    conn.close()
    return path


def test_mlflow_history_is_counted_with_its_own_allow_list(tmp_path):
    live_db, live_objects = _live(tmp_path)
    backup = _backup(live_db, live_objects, tmp_path / "b")
    _mlflow_db(backup / "mlflow.db")
    before = _files(backup)
    report = restore_check(backup, live_db=live_db)
    ml = report["mlflow"]
    assert ml["integrity"] == "ok"
    assert ml["row_counts"] == MLFLOW_TABLES  # exact: no -1, no metadata 'jobs'
    assert ml["tables_not_counted"] == 2  # alembic_version, jobs
    assert all(v >= 0 for v in report["metadata"]["row_counts"].values())
    assert report["ok"] is True
    assert _files(backup) == before


def test_live_mlflow_comparison_reports_differences(tmp_path):
    live_db, live_objects = _live(tmp_path)
    backup = _backup(live_db, live_objects, tmp_path / "b")
    _mlflow_db(backup / "mlflow.db")
    live_ml = _mlflow_db(tmp_path / "live" / "mlflow.db", extra_runs=4)
    report = restore_check(backup, live_db=live_db, live_mlflow=live_ml)
    cmp = report["mlflow_comparison"]
    assert cmp["live_integrity"] == "ok"
    assert cmp["row_count_diff"] == {"runs": {"backup": 3, "live": 7}}
    same = restore_check(backup, live_mlflow=_mlflow_db(tmp_path / "same.db"))
    assert same["mlflow_comparison"]["row_count_diff"] == {}


def test_a_backup_whose_mlflow_db_lacks_the_history_tables_fails(tmp_path):
    live_db, live_objects = _live(tmp_path)
    backup = _backup(live_db, live_objects, tmp_path / "b")
    conn = sqlite3.connect(str(backup / "mlflow.db"))
    conn.execute("CREATE TABLE experiments (x INTEGER)")
    conn.commit()
    conn.close()
    report = restore_check(backup)
    assert report["ok"] is False
    assert any("MLflow" in p and "runs" in p for p in report["problems"])


def test_module_cli_accepts_live_mlflow(tmp_path, capsys):
    """`python -m icp_api.restore_check <dir> --live-db .. --live-mlflow ..` (cli.py wiring: see notes)."""
    from icp_api.restore_check import main

    live_db, live_objects = _live(tmp_path)
    backup = _backup(live_db, live_objects, tmp_path / "b")
    _mlflow_db(backup / "mlflow.db")
    live_ml = _mlflow_db(tmp_path / "live" / "mlflow.db")
    assert main([str(backup), "--live-mlflow", str(live_ml)]) == 0
    assert json.loads(capsys.readouterr().out)["mlflow_comparison"]["row_count_diff"] == {}
