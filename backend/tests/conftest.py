"""Backend test fixtures.

A session-scoped environment points every store at a temporary directory (SQLite metadata,
local object store, SQLite MLflow) and uses a fast two-candidate training config. The job
worker subprocess inherits the same environment variables.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[2]
ADMIN_TOKEN = "test-admin-token-0123456789abcdefghijklmnop"
GATEWAY_SECRET = "test-gateway-secret-0123456789abcdefghijkl"
# Test inputs come only from files tracked in Git, so a clean CI checkout (no `dvc pull`) runs the
# same suite as a developer machine: the ICP history (data/raw/dataset.csv is tracked) and the
# EIA-based WTI seed (data/seed/wti.csv). data/raw/wti.csv is DVC-only and absent in CI.
SEED_ICP = ROOT / "data" / "raw" / "dataset.csv"
SEED_WTI = ROOT / "data" / "seed" / "wti.csv"


def require_data(*paths: Path) -> None:
    """Skip when tracked test data is missing, or fail when ICP_REQUIRE_DATA=1 (set in CI)."""
    missing = [str(p.relative_to(ROOT)) for p in paths if not p.is_file()]
    if not missing:
        return
    message = f"test datasets missing: {', '.join(missing)}"
    if os.getenv("ICP_REQUIRE_DATA") == "1":
        pytest.fail(
            message + " (ICP_REQUIRE_DATA=1: data-dependent tests must not be skipped)", pytrace=False
        )
    pytest.skip(message)


@pytest.fixture(scope="session")
def env_dir(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Path]:
    base = tmp_path_factory.mktemp("icp")
    config = {
        "experiment_name": "icp-test",
        "model_name": "ICP_Test_Model",
        "min_feature_rows": 30,
        "methods": [
            "naive_last",
            "spread_wti_12m",
            "spread_brent_6m",
            {
                "name": "ridge_small",
                "family": "ridge",
                "params": {"alpha": 3.0},
                "features": ["brent_price", "wti_price", "icp_lag_1"],
            },
        ],
        "backtest": {"horizons": [1, 2, 3], "min_train_rows": 24},
        "selection": {"mode": "observed", "horizons": [1, 2], "metric": "mae"},
    }
    (base / "training.yaml").write_text(yaml.safe_dump(config), encoding="utf-8")
    env = {
        "APP_ENV": "test",
        "LOG_LEVEL": "WARNING",
        "SQLITE_PATH": str(base / "metadata.db"),
        "LOCAL_OBJECT_DIR": str(base / "objects"),
        "MLFLOW_TRACKING_URI": f"sqlite:///{(base / 'mlflow.db').as_posix()}",
        "MLFLOW_ARTIFACT_ROOT": str(base / "mlartifacts"),
        "MODEL_NAME": "ICP_Test_Model",
        "SEED_DIR": str(ROOT / "data" / "seed"),
        "SEED_ICP_PATH": str(SEED_ICP),
        "SEED_WTI_PATH": str(SEED_WTI),
        "TRAINING_CONFIG_PATH": str(base / "training.yaml"),
        "ADMIN_API_TOKENS": f"tester:{ADMIN_TOKEN}",
        "GATEWAY_SHARED_SECRET": GATEWAY_SECRET,
        "MODEL_REFRESH_SECONDS": "0",
        "JOB_HEARTBEAT_SECONDS": "1",
        "RATE_LIMIT_FORECAST_PER_MINUTE": "1000",
        "RATE_LIMIT_DEFAULT_PER_MINUTE": "1000",
        "RATE_LIMIT_ADMIN_PER_MINUTE": "1000",
        "RATE_LIMIT_AUTH_PER_MINUTE": "1000",
    }
    saved = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    yield base
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


@pytest.fixture(scope="session")
def app(env_dir: Path) -> Any:
    require_data(SEED_ICP, SEED_WTI)
    from icp_api.main import create_app
    from icp_api.settings import load_settings

    return create_app(load_settings())


@pytest.fixture(scope="session")
def client(app: Any) -> Iterator[TestClient]:
    with TestClient(app) as c:
        yield c


@pytest.fixture
def admin() -> dict[str, str]:
    return {"Authorization": f"Bearer {ADMIN_TOKEN}"}


CUSTOMER_PASSWORD = "Pelanggan-Uji-2026!"


@pytest.fixture(scope="session")
def customer(client: TestClient) -> dict[str, str]:
    """Bearer header of a signed-in owner of a fresh trial organisation (the customer workspace
    requires sign-in since access policy v4)."""
    r = client.post(
        "/api/v1/auth/register",
        json={
            "organization_name": "PT Pelanggan Uji",
            "email": "pelanggan@pelanggan-uji.test",
            "name": "Pelanggan",
            "password": CUSTOMER_PASSWORD,
            "accept_terms": True,
        },
    )
    assert r.status_code == 201, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


@pytest.fixture(scope="session")
def trained_version(client: TestClient) -> str:
    """Train once per session through the real job API and return the registered version."""
    headers = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
    r = client.post("/api/v1/jobs", json={"job_type": "train", "params": {}}, headers=headers)
    assert r.status_code == 202, r.text
    job = client.app.state.jobs.wait(r.json()["id"], timeout=300)
    assert job["status"] == "succeeded", job
    return str(job["result"]["registered_version"])


# --- D1 REST API simulator ---------------------------------------------------------------------
class D1Simulator:
    """Implements the D1 `/query` request/response protocol on top of a real SQLite database."""

    def __init__(self, path: Path, fail_first: int = 0, status_on_fail: int = 503):
        self.conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.fail_first = fail_first
        self.status_on_fail = status_on_fail
        self.calls = 0

    def apply_migrations(self) -> None:
        from icp_api.storage.migrations import discover

        for m in discover():  # what `wrangler d1 migrations apply` does
            self.conn.executescript(m.path.read_text(encoding="utf-8"))

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        assert request.headers["authorization"] == "Bearer d1-token"
        if self.calls <= self.fail_first:
            return httpx.Response(
                self.status_on_fail, json={"success": False, "errors": [{"message": "busy"}]}
            )
        body = json.loads(request.content)
        try:
            cur = self.conn.execute(body["sql"], body.get("params", []))
            rows = [dict(r) for r in cur.fetchall()]
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "errors": [],
                    "messages": [],
                    "result": [
                        {
                            "success": True,
                            "results": rows,
                            "meta": {"changes": max(cur.rowcount, 0), "duration": 0.1},
                        }
                    ],
                },
            )
        except sqlite3.Error as exc:
            return httpx.Response(
                400,
                json={
                    "success": False,
                    "errors": [{"code": 7500, "message": str(exc)}],
                    "messages": [],
                    "result": [],
                },
            )


@pytest.fixture
def d1sim() -> type[D1Simulator]:
    return D1Simulator


# --- Worker internal data plane simulator -----------------------------------------------------
# Mirror of the Worker's statement allow-list (apps/gateway/src/index.ts: SQL_ALLOWED_START,
# SQL_PRAGMA_TABLE_INFO, SQL_FORBIDDEN_WORD, sqlSkeleton, sqlRejection, MAX_SQL_PARAMS). Keep in sync:
# every repository test that runs over the gateway simulator then proves the backend only issues
# statements the real Worker accepts.
SQL_ALLOWED_START = re.compile(r"^(SELECT|INSERT|UPDATE|DELETE|WITH)\b", re.IGNORECASE)
SQL_PRAGMA_TABLE_INFO = re.compile(
    r'^PRAGMA\s+table_info\(\s*"?[A-Za-z_][A-Za-z0-9_]*"?\s*\)\s*;?\s*$', re.IGNORECASE
)
SQL_FORBIDDEN_WORD = re.compile(
    r"\b(ATTACH|DETACH|PRAGMA|VACUUM|REINDEX|ANALYZE|CREATE|DROP|ALTER|BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)\b",
    re.IGNORECASE,
)
MAX_SQL_PARAMS = 100


def sql_skeleton(sql: str) -> str:
    """SQL with string literals, quoted identifiers and comments blanked out (keywords remain)."""
    out: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch in "'\"`[":
            close = "]" if ch == "[" else ch
            j = i + 1
            while j < n:
                if sql[j] == close:
                    if close != "]" and j + 1 < n and sql[j + 1] == close:  # doubled quote
                        j += 2
                        continue
                    break
                j += 1
            out.append(" _ ")
            i = j
        elif ch == "-" and sql[i + 1 : i + 2] == "-":
            while i < n and sql[i] != "\n":
                i += 1
            out.append(" ")
        elif ch == "/" and sql[i + 1 : i + 2] == "*":
            end = sql.find("*/", i + 2)
            i = n if end < 0 else end + 1
            out.append(" ")
        else:
            out.append(ch)
        i += 1
    return "".join(out)


def sql_rejection(sql: str) -> str | None:
    """Same decision as the Worker's `sqlRejection`: an error message, or None when allowed."""
    trimmed = sql.strip()
    if SQL_PRAGMA_TABLE_INFO.match(trimmed):
        return None
    skeleton = sql_skeleton(trimmed).strip()
    if not SQL_ALLOWED_START.match(skeleton):
        return "statement type not allowed on the data plane"
    semicolon = skeleton.find(";")
    if semicolon >= 0 and skeleton[semicolon + 1 :].strip():
        return "only one statement per request"
    if SQL_FORBIDDEN_WORD.search(skeleton):
        return "statement contains a keyword not allowed on the data plane"
    return None


def _d1_error(status: int, message: str) -> httpx.Response:
    return httpx.Response(
        status,
        json={"success": False, "errors": [{"code": 7500, "message": message}], "messages": [], "result": []},
    )


class GatewaySimulator:
    """Behaves like apps/gateway `/api/internal/*`: secret check, D1 envelope over SQLite, KV objects.

    Mirrors the Worker implementation (apps/gateway/src/index.ts) so the Python transports are
    tested against the same request/response shapes the real Worker produces.
    """

    # Exposed for tests (conftest is not importable as a module under --import-mode=importlib).
    SECRET = GATEWAY_SECRET
    MAX_SQL_PARAMS = MAX_SQL_PARAMS
    sql_rejection = staticmethod(sql_rejection)

    def __init__(self, path: Path, secret: str = GATEWAY_SECRET, fail_first: int = 0, kv: bool = True):
        self.d1 = D1Simulator(path, fail_first=fail_first)
        self.secret = secret
        self.kv = kv
        self.objects: dict[str, tuple[bytes, str]] = {}
        self.calls: list[tuple[str, str]] = []
        self.rejected: list[str] = []  # statements refused by the allow-list (a backend bug if non-empty)

    def apply_migrations(self) -> None:
        self.d1.apply_migrations()

    @property
    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.calls.append((request.method, path))
        assert "authorization" not in request.headers  # no Cloudflare API token is ever sent
        if request.headers.get("x-gateway-secret") != self.secret:
            return httpx.Response(401, json={"error": {"code": "GATEWAY_AUTH_REQUIRED"}})
        if path == "/api/internal/health":
            return httpx.Response(
                200, json={"status": "ok", "environment": "test", "d1": True, "kv": self.kv}
            )
        if path == "/api/internal/d1/query":
            if request.method != "POST":
                return httpx.Response(405)
            try:
                body = json.loads(request.content)
            except ValueError:
                return _d1_error(400, "request body is not JSON")
            sql, params = body.get("sql"), body.get("params") or []
            if not isinstance(sql, str) or not sql.strip():
                return _d1_error(400, "sql must be a non-empty string")
            if not isinstance(params, list) or len(params) > MAX_SQL_PARAMS:
                return _d1_error(400, "params must be an array of string/number/boolean/null")
            rejected = sql_rejection(sql)
            if rejected:
                self.rejected.append(sql)
                return _d1_error(403, rejected)
            fake = httpx.Request(
                "POST", request.url, headers={"authorization": "Bearer d1-token"}, content=request.content
            )
            return self.d1.handler(fake)
        prefix = "/api/internal/objects/"
        if path.startswith(prefix) and self.kv:
            key = path[len(prefix) :]
            if request.method == "PUT":
                if request.headers.get("if-none-match") == "*" and key in self.objects:
                    return httpx.Response(412, json={"error": {"code": "OBJECT_EXISTS"}})
                self.objects[key] = (
                    bytes(request.content),
                    request.headers.get("content-type", "application/octet-stream"),
                )
                return httpx.Response(201, json={"key": key, "size": len(request.content)})
            if request.method in ("GET", "HEAD"):
                if key not in self.objects:
                    return httpx.Response(404, json={"error": {"code": "OBJECT_NOT_FOUND"}})
                data, ctype = self.objects[key]
                return httpx.Response(
                    200, content=b"" if request.method == "HEAD" else data, headers={"content-type": ctype}
                )
            return httpx.Response(405)
        return httpx.Response(503 if path.startswith(prefix) else 404, json={"error": {"code": "NOT_FOUND"}})


@pytest.fixture
def gatewaysim() -> type[GatewaySimulator]:
    return GatewaySimulator
