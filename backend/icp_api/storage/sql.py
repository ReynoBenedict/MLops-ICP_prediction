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
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Protocol

import httpx

logger = logging.getLogger(__name__)

Params = Sequence[Any]


class StorageError(RuntimeError):
    """The metadata store is unavailable or returned an unexpected error."""


class IntegrityError(StorageError):
    """A uniqueness or check constraint was violated."""


# -- request deadline ----------------------------------------------------------------------------
# The request middleware answers 504 REQUEST_TIMEOUT after REQUEST_TIMEOUT_SECONDS, but a synchronous
# endpoint keeps running in its worker thread after that answer. The data-plane clients (D1 below, KV
# in ``objects.GatewayObjectStore``) therefore bound their retries, back-off and read timeouts by the
# same deadline:
# * a read gets at most the time that is left, and no read starts once the deadline has passed (work
#   that only gathers data for an answer nobody receives stops there);
# * no retry and no back-off sleep starts once the deadline has passed (or would pass during the sleep);
# * a write's FIRST attempt always starts, with its full timeout, even after the deadline. Refusing it
#   would cut a write sequence in the middle (a report's files stored without its report row, an action
#   without its audit row), and cutting it short would turn a write that may still land into an
#   ambiguous failure. A write's retries are bounded like any other retry.
# Outside a request (jobs, CLI) there is no deadline and the clients behave exactly as before.
_request_deadline: ContextVar[float | None] = ContextVar("icp_request_deadline", default=None)


class RequestDeadlineExceeded(StorageError):
    """The request's time budget ran out before a data-plane attempt could start."""


@contextmanager
def request_deadline(seconds: float) -> Iterator[None]:
    """Give the data-plane calls made inside this block (and its worker threads) ``seconds`` in total."""
    token = _request_deadline.set(time.monotonic() + seconds)
    try:
        yield
    finally:
        _request_deadline.reset(token)


def remaining_request_seconds() -> float | None:
    """Seconds left before the current request's deadline (negative once passed); None outside a request."""
    deadline = _request_deadline.get()
    return None if deadline is None else deadline - time.monotonic()


def attempt_timeout(
    configured: float, idempotent: bool, label: str, attempt: int, last: Exception | None = None
) -> float:
    """The timeout for attempt number ``attempt`` (1-based), or ``RequestDeadlineExceeded`` when the
    request has no time left for it. A write's first attempt is never refused (see above)."""
    left = remaining_request_seconds()
    if left is None or (not idempotent and attempt == 1):
        return configured
    if left <= 0:
        cause = f" (last error: {last})" if last is not None else ""
        raise RequestDeadlineExceeded(f"{label}: the request deadline passed before the next attempt{cause}")
    return min(configured, left) if idempotent else configured


def backoff_or_give_up(delay: float, label: str, attempts: int, last: Exception | None) -> None:
    """Sleep before a retry, unless the sleep would end after the request deadline."""
    left = remaining_request_seconds()
    if left is not None and left <= delay:
        raise RequestDeadlineExceeded(
            f"{label} unavailable after {attempts} attempt(s) within the request deadline: {last}"
        )
    time.sleep(delay)


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
    # A write may already be applied when a 502/503/504 comes back (the Worker or the API failed
    # after D1 committed), so writes are retried only on 429 (rejected before execution) and on
    # connection errors (never sent). Reads are retried on every transient status (audit v7 DATA-01).
    _WRITE_RETRY_STATUS = {429}
    _READ_PREFIXES = ("SELECT", "PRAGMA", "EXPLAIN")
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
        self.timeout = timeout
        self._client = httpx.Client(
            timeout=timeout, headers={"Content-Type": "application/json", **headers}, transport=transport
        )
        self.max_attempts = max_attempts

    @classmethod
    def _is_read(cls, sql: str) -> bool:
        return sql.lstrip().upper().startswith(cls._READ_PREFIXES)

    def _post(self, sql: str, params: Params) -> dict[str, Any]:
        body = {"sql": sql, "params": list(params)}
        is_read = self._is_read(sql)
        retry_status = self._RETRY_STATUS if is_read else self._WRITE_RETRY_STATUS
        last: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            # bounded by the request deadline, if any (see ``request_deadline``)
            timeout = attempt_timeout(self.timeout, is_read, self._label, attempt, last)
            try:
                resp = self._client.post(self.url, json=body, timeout=timeout)
            except httpx.ConnectError as exc:
                # The request never reached the server, so retrying cannot double-apply a write.
                last = exc
            except httpx.TimeoutException as exc:
                # Ambiguous for writes (may have been applied); do not retry.
                raise StorageError(f"{self._label} request timed out") from exc
            else:
                if resp.status_code in retry_status:
                    last = StorageError(f"{self._label} HTTP {resp.status_code}")
                elif resp.status_code in self._RETRY_STATUS:
                    # Ambiguous for a write: report it instead of replaying the statement.
                    raise StorageError(
                        f"{self._label} HTTP {resp.status_code}; the write may or may not have been applied"
                    )
                else:
                    return self._parse(resp)
            if attempt < self.max_attempts:
                backoff_or_give_up(0.25 * 2 ** (attempt - 1), self._label, attempt, last)
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
