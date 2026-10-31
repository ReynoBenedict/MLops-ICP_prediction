"""Forward migration on a populated database preserves every row (audit v7 DATA-04).

Starting from 0001, each step fills every table that exists at that point with a synthetic row
(values derived from the live schema: CHECK lists, foreign keys, types), snapshots all tables,
applies the next migration exactly as ``icp-api migrate`` does, and requires that every row and
every pre-existing column value survives - including the 0002 rebuild of ``dataset_versions``.
"""

from __future__ import annotations

import re
from typing import Any

from icp_api.storage.migrations import apply_local, current_version, discover
from icp_api.storage.sql import SqliteExecutor


def _tables(db: SqliteExecutor) -> list[str]:
    rows = db.query(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )
    return [r["name"] for r in rows]


def _table_sql(db: SqliteExecutor, table: str) -> str:
    return str(db.query("SELECT sql FROM sqlite_master WHERE name = ?", (table,))[0]["sql"])


def _allowed(sql: str, column: str) -> str | None:
    m = re.search(rf"\b{column}\b[^,]*?CHECK\s*\(\s*{column}\s+IN\s*\(([^)]*)\)", sql)
    if not m:
        return None
    return m.group(1).split(",")[0].strip().strip("'")


def _fill(db: SqliteExecutor, table: str, step: int) -> bool:
    """Insert one synthetic row; False when a referenced parent table has no row yet."""
    if table == "app_schema" or db.query(f"SELECT COUNT(*) AS n FROM {table}")[0]["n"]:
        return True
    sql = _table_sql(db, table)
    fks = {r["from"]: r["table"] for r in db.query(f"PRAGMA foreign_key_list({table})")}
    values: dict[str, Any] = {}
    for col in db.query(f"PRAGMA table_info({table})"):
        name, ctype = col["name"], (col["type"] or "").upper()
        if name in fks:
            parent = db.query(f"SELECT id FROM {fks[name]} LIMIT 1")
            if not parent:
                return False
            values[name] = parent[0]["id"]
            continue
        allowed = _allowed(sql, name)
        if allowed is not None:
            values[name] = int(allowed) if allowed.isdigit() else allowed
        elif "INT" in ctype or "REAL" in ctype:
            values[name] = 0
        else:
            values[name] = f"{table}-{name}-s{step}"
    cols = ", ".join(values)
    marks = ", ".join("?" for _ in values)
    db.execute(f"INSERT INTO {table} ({cols}) VALUES ({marks})", tuple(values.values()))
    return True


def _fill_all(db: SqliteExecutor, step: int) -> None:
    pending = _tables(db)
    while pending:  # parents before children (foreign keys are enforced)
        left = [t for t in pending if not _fill(db, t, step)]
        assert len(left) < len(pending), f"cannot satisfy foreign keys of {left}"
        pending = left


def _snapshot(db: SqliteExecutor) -> dict[str, list[dict[str, Any]]]:
    return {t: db.query(f"SELECT * FROM {t} ORDER BY rowid") for t in _tables(db) if t != "app_schema"}


def test_every_migration_preserves_existing_rows_on_a_populated_database(tmp_path):
    db = SqliteExecutor(tmp_path / "upgrade.db")
    migs = discover()
    db.executescript(migs[0].path.read_text(encoding="utf-8"))
    for mig in migs[1:]:
        _fill_all(db, mig.version - 1)
        before = _snapshot(db)
        assert all(before[t] for t in before), "every table must hold a row before the upgrade"
        db.executescript(mig.path.read_text(encoding="utf-8"))
        assert current_version(db) == mig.version
        after = _snapshot(db)
        for table, rows in before.items():
            assert table in after, f"{mig.name} dropped table {table}"
            assert len(after[table]) == len(rows), f"{mig.name} changed the row count of {table}"
            for old, new in zip(rows, after[table], strict=True):
                assert {k: new.get(k) for k in old} == old, f"{mig.name} changed a row of {table}"
    # the fully upgraded database is what a fresh install produces (same tables, same version)
    fresh = SqliteExecutor(tmp_path / "fresh.db")
    apply_local(fresh)
    assert _tables(fresh) == _tables(db)
    assert current_version(fresh) == current_version(db) == len(migs)
    for table in _tables(db):
        cols = [c["name"] for c in db.query(f"PRAGMA table_info({table})")]
        assert cols == [c["name"] for c in fresh.query(f"PRAGMA table_info({table})")], table


def test_the_0002_rebuild_keeps_dataset_versions_and_their_constraints(tmp_path):
    db = SqliteExecutor(tmp_path / "v1.db")
    migs = discover()
    db.executescript(migs[0].path.read_text(encoding="utf-8"))
    for n, ds in enumerate(("icp", "wti"), start=1):
        db.execute(
            "INSERT INTO dataset_versions (id, dataset, version, object_key, sha256, size_bytes, row_count, "
            "source, quality_json, is_active, created_at, created_by) VALUES (?, ?, 1, ?, ?, 10, 5, 'seed', "
            "'{}', 1, '2026-01-0' || ?, 'op')",
            (f"dsv_{ds}", ds, f"datasets/{ds}/{n}.csv", str(n) * 64, n),
        )
    db.executescript(migs[1].path.read_text(encoding="utf-8"))
    rows = db.query("SELECT id, dataset, is_active, provenance_json FROM dataset_versions ORDER BY id")
    assert rows == [
        {"id": "dsv_icp", "dataset": "icp", "is_active": 1, "provenance_json": None},
        {"id": "dsv_wti", "dataset": "wti", "is_active": 1, "provenance_json": None},
    ]
    # the uniqueness constraints survived the rebuild; the dataset CHECK list was removed on purpose
    indexes = db.query("PRAGMA index_list(dataset_versions)")
    assert sum(1 for i in indexes if i["unique"]) >= 3  # PK + (dataset, version) + (dataset, sha256)
    db.execute(
        "INSERT INTO dataset_versions (id, dataset, version, object_key, sha256, size_bytes, row_count, "
        "source, quality_json, created_at, created_by) VALUES ('dsv_b', 'brent', 1, 'k', 'b', 1, 1, 's', '{}', "
        "'2026-01-03', 'op')"
    )


def test_readiness_flags_a_database_newer_than_the_code(client, admin, monkeypatch):
    from icp_api.routers import health

    monkeypatch.setattr(health, "expected_version", lambda: 1)
    body = client.get("/health/ready", headers=admin).json()
    checks = {c["name"]: c for c in body["checks"]}
    assert checks["metadata_store"]["status"] == "ok"
    assert checks["schema_version"]["status"] == "degraded"
    assert "newer than this code" in checks["schema_version"]["detail"]
    assert body["status"] != "ready"
    monkeypatch.undo()
    checks = {c["name"]: c for c in client.get("/health/ready", headers=admin).json()["checks"]}
    assert checks["schema_version"]["status"] == "ok"


def test_0008_billing_is_additive_and_independent_of_0007(tmp_path):
    """Wave E: 0008 only needs organizations (0004); it applies on a v6 database without 0007."""
    from icp_api.storage.migrations import MIGRATIONS_DIR

    migs = {m.version: m for m in discover()}
    assert 8 in migs and migs[8].name == "0008_billing_sandbox"
    db = SqliteExecutor(tmp_path / "v6.db")
    for v in range(1, 7):
        db.executescript(migs[v].path.read_text(encoding="utf-8"))
    db.executescript((MIGRATIONS_DIR / "0008_billing_sandbox.sql").read_text(encoding="utf-8"))
    assert {"billing_orders", "billing_events"} <= set(_tables(db))
