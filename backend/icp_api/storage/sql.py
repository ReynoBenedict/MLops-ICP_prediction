"""SQL executors: one interface, two transports with identical SQL.

* ``SqliteExecutor`` - local development and tests (a file under ``var/``).
* ``D1HttpExecutor`` - Cloudflare D1 via the REST query endpoint
  ``POST /client/v4/accounts/{account}/d1/database/{db}/query`` using an API token with
  D1 Edit permission.
* ``GatewayD1Executor`` - Cloudflare D1 through the project's own Worker
  (``POST <gateway>/api/internal/d1/query``, authenticated with the gateway shared secret). The
  Worker holds the D1 binding, so the backend needs no Cloudflare API token; the response
  envelope is the same as the REST API's. This is the zero-cost deployment path (docs/DEPLOYMENT.md).

Parity rules: only single statements are executed (D1 offers no interactive transactions),
only SQLite syntax that D1 supports is used, and parameters are always bound (never
interpolated). Material differences that remain - network latency, Cloudflare API rate limits,
eventual 5xx/429 responses - are handled by bounded retries in the D1 executor and documented
in docs/ARCHITECTURE.md.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Protocol

import httpx

logger = logging.getLogger(__name__)

Params = Sequence[Any]


class StorageError(RuntimeError):
    """The metadata store is unavailable or returned an unexpected error."""


class IntegrityError(StorageError):
    """A uniqueness or check constraint was violated."""


class SqlExecutor(Protocol):
    backend: str

    def query(self, sql: str, params: Params = ()) -> list[dict[str, Any]]: ...

    def execute(self, sql: str, params: Params = ()) -> int: ...

    def ping(self) -> None: ...


def _is_integrity_message(msg: str) -> bool:
    m = msg.upper()
    return "UNIQUE CONSTRAINT" in m or "CHECK CONSTRAINT" in m or "SQLITE_CONSTRAINT" in m


class SqliteExecutor:
    backend = "sqlite"

    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(str(self.path), timeout=10, isolation_level=None, check_same_thread=True)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=10000")
            conn.execute("PRAGMA foreign_keys=ON")
            self._local.conn = conn
        return conn

    def query(self, sql: str, params: Params = ()) -> list[dict[str, Any]]:
        try:
            return [dict(r) for r in self._conn().execute(sql, tuple(params)).fetchall()]
        except sqlite3.IntegrityError as exc:
            raise IntegrityError(str(exc)) from exc
        except sqlite3.Error as exc:
            raise StorageError(f"sqlite error: {exc}") from exc

    def execute(self, sql: str, params: Params = ()) -> int:
        try:
            return self._conn().execute(sql, tuple(params)).rowcount
        except sqlite3.IntegrityError as exc:
            raise IntegrityError(str(exc)) from exc
        except sqlite3.Error as exc:
            raise StorageError(f"sqlite error: {exc}") from exc

    def executescript(self, script: str) -> None:
        """Local-only: apply a migration file atomically (D1 migrations are applied by wrangler)."""
        conn = self._conn()
        try:
            conn.executescript("BEGIN;\n" + script + "\nCOMMIT;")
        except sqlite3.Error as exc:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise StorageError(f"migration failed: {exc}") from exc

    def ping(self) -> None:
        self.query("SELECT 1 AS ok")


class _D1HttpBase:
    """Shared request/retry/parse logic for the two D1 HTTP transports (same envelope)."""

    backend = "d1"
    _RETRY_STATUS = {429, 502, 503, 504}
    _label = "D1"

    def __init__(
        self,
        url: str,
        headers: dict[str, str],
        timeout: float = 15.0,
        max_attempts: int = 3,
        transport: httpx.BaseTransport | None = None,
    ):
        self.url = url
        self._client = httpx.Client(
            timeout=timeout, headers={"Content-Type": "application/json", **headers}, transport=transport
        )
        self.max_attempts = max_attempts

    def _post(self, sql: str, params: Params) -> dict[str, Any]:
        body = {"sql": sql, "params": list(params)}
        last: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                resp = self._client.post(self.url, json=body)
            except httpx.ConnectError as exc:
                # The request never reached the server, so retrying cannot double-apply a write.
                last = exc
            except httpx.TimeoutException as exc:
                # Ambiguous for writes (may have been applied); do not retry.
                raise StorageError(f"{self._label} request timed out") from exc
            else:
                if resp.status_code in self._RETRY_STATUS:
                    last = StorageError(f"{self._label} HTTP {resp.status_code}")
                else:
                    return self._parse(resp)
            if attempt < self.max_attempts:
                time.sleep(0.25 * 2 ** (attempt - 1))
        raise StorageError(f"{self._label} unavailable after {self.max_attempts} attempts: {last}")

    @classmethod
    def _parse(cls, resp: httpx.Response) -> dict[str, Any]:
        try:
            payload = resp.json()
        except ValueError as exc:
            raise StorageError(f"{cls._label} returned non-JSON response (HTTP {resp.status_code})") from exc
        if not payload.get("success") or resp.status_code >= 400:
            msg = (
                "; ".join(str(e.get("message", e)) for e in payload.get("errors") or [])
                or f"HTTP {resp.status_code}"
            )
            if _is_integrity_message(msg):
                raise IntegrityError(msg)
            raise StorageError(f"{cls._label} error: {msg}")
        results = payload.get("result") or []
        if not results:
            raise StorageError(f"{cls._label} response contained no result")
        first = results[0]
        if first.get("success") is False:
            raise StorageError(f"{cls._label} statement failed")
        return dict(first)

    def query(self, sql: str, params: Params = ()) -> list[dict[str, Any]]:
        return list(self._post(sql, params).get("results") or [])

    def execute(self, sql: str, params: Params = ()) -> int:
        return int((self._post(sql, params).get("meta") or {}).get("changes", 0))

    def executescript(self, script: str) -> None:
        raise StorageError(
            "Migrations are not applied over HTTP; use `npx wrangler d1 migrations apply <DB> --remote` "
            "(see docs/DEPLOYMENT.md)."
        )

    def ping(self) -> None:
        self.query("SELECT 1 AS ok")


class D1HttpExecutor(_D1HttpBase):
    """D1 REST API with a Cloudflare API token (D1 Edit)."""

    backend = "d1"

    def __init__(
        self,
        account_id: str,
        database_id: str,
        api_token: str,
        timeout: float = 15.0,
        max_attempts: int = 3,
        base_url: str = "https://api.cloudflare.com/client/v4",
        transport: httpx.BaseTransport | None = None,
    ):
        super().__init__(
            f"{base_url}/accounts/{account_id}/d1/database/{database_id}/query",
            {"Authorization": f"Bearer {api_token}"},
            timeout,
            max_attempts,
            transport,
        )


class GatewayD1Executor(_D1HttpBase):
    """D1 through the Worker's internal data plane (no Cloudflare API token on the backend)."""

    backend = "d1_gateway"
    _label = "D1 (gateway)"

    def __init__(
        self,
        gateway_url: str,
        shared_secret: str,
        timeout: float = 15.0,
        max_attempts: int = 3,
        transport: httpx.BaseTransport | None = None,
    ):
        if not shared_secret:
            raise ValueError("GatewayD1Executor requires the gateway shared secret")
        super().__init__(
            internal_url(gateway_url, "d1/query"),
            {"x-gateway-secret": shared_secret},
            timeout,
            max_attempts,
            transport,
        )


def internal_url(gateway_url: str, path: str) -> str:
    """``https://gateway.example`` + ``objects/a/b`` -> ``https://gateway.example/api/internal/objects/a/b``."""
    base = gateway_url.rstrip("/")
    if base.endswith("/api/internal"):
        base = base[: -len("/api/internal")]
    return f"{base}/api/internal/{path.lstrip('/')}"
