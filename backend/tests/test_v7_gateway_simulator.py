"""The gateway simulator enforces the Worker's data-plane SQL allow-list (audit v7 SEC-10).

The vectors are the same as apps/gateway/test/v7-security.test.ts, so the Python mirror and the Worker
cannot drift silently. Repository tests that run over the simulator (test_storage.py, gateway variant)
additionally prove that every statement the backend issues is accepted by the real Worker.
"""

from __future__ import annotations

import json

import httpx
import pytest

ALLOWED = [
    "SELECT * FROM users WHERE email = ?",
    "  select 1 as ok",
    'INSERT OR IGNORE INTO "jobs" (id) VALUES (?)',
    "INSERT INTO usage_events (id) SELECT ? WHERE (SELECT COUNT(*) FROM usage_events) < ?",
    "UPDATE users SET name = CASE WHEN ? THEN ? ELSE name END WHERE id = ?",
    "DELETE FROM organizations WHERE id = ? AND NOT EXISTS (SELECT 1 FROM users WHERE org_id = ?)",
    "WITH x AS (SELECT 1) SELECT * FROM x",
    'PRAGMA table_info("users")',
    "SELECT 'DROP TABLE users; --' AS literal",
    "SELECT * FROM jobs WHERE status = 'rollback';",
    "SELECT created_at, dropped FROM t",
]
REFUSED = [
    "DROP TABLE users",
    "CREATE TABLE x (a)",
    "ALTER TABLE users ADD COLUMN x",
    "ATTACH DATABASE 'file.db' AS x",
    "PRAGMA foreign_keys=OFF",
    "PRAGMA writable_schema = 1",
    "VACUUM",
    "BEGIN",
    "SELECT 1; DROP TABLE users",
    "SELECT 1; SELECT 2",
    "WITH x AS (SELECT 1) SELECT 1; DELETE FROM users",
    "/* c */ DROP TABLE users",
    "SELECT 1 /* ; */ ; ATTACH 'x' AS y",
    "REPLACE INTO users VALUES (1)",
]


@pytest.mark.parametrize("sql", ALLOWED)
def test_allow_list_accepts_the_statement_kinds_the_backend_issues(sql: str, gatewaysim) -> None:
    assert gatewaysim.sql_rejection(sql) is None


@pytest.mark.parametrize("sql", REFUSED)
def test_allow_list_refuses_ddl_pragma_transactions_and_stacked_statements(sql: str, gatewaysim) -> None:
    assert gatewaysim.sql_rejection(sql) is not None


def _post(gw, sql: str, params: list | None = None) -> httpx.Response:
    with httpx.Client(transport=gw.transport, base_url="https://gw.test") as c:
        return c.post(
            "/api/internal/d1/query",
            headers={"x-gateway-secret": gw.SECRET},
            content=json.dumps({"sql": sql, "params": params or []}),
        )


def test_simulator_answers_403_without_touching_the_database(tmp_path, gatewaysim) -> None:
    gw = gatewaysim(tmp_path / "gw.db")
    gw.apply_migrations()
    before = gw.d1.calls
    r = _post(gw, "DROP TABLE users")
    assert r.status_code == 403
    assert r.json()["success"] is False
    assert gw.d1.calls == before  # never reached SQLite
    assert gw.rejected == ["DROP TABLE users"]
    # the table still exists
    ok = _post(gw, "SELECT COUNT(*) AS n FROM users")
    assert ok.status_code == 200
    assert ok.json()["result"][0]["results"][0]["n"] == 0


def test_simulator_caps_bound_parameters_like_d1(tmp_path, gatewaysim) -> None:
    gw = gatewaysim(tmp_path / "gw.db")
    gw.apply_migrations()
    cap = gw.MAX_SQL_PARAMS
    assert cap == 100  # Cloudflare D1: max bound parameters per query
    placeholders = ",".join("?" * cap)
    assert _post(gw, f"SELECT 1 WHERE 1 IN ({placeholders})", [1] * cap).status_code == 200
    over = ",".join("?" * (cap + 1))
    assert _post(gw, f"SELECT 1 WHERE 1 IN ({over})", [1] * (cap + 1)).status_code == 400
