"""Shared plumbing for external data-source connectors.

Every connector returns ``SourceFetch`` objects: a monthly (or daily) frame plus the provenance
needed to audit each value later (source URL, retrieval time, document checksum, extraction
method, verification status). Nothing here invents values; completeness flags make partial
months explicit so they can never be mistaken for finished monthly observations.

HTTP policy (applies to every connector):
* bounded connect/read timeouts;
* exponential-backoff retries on connection errors, 429 and 5xx;
* a per-host minimum interval (politeness / published rate limits);
* an on-disk cache with conditional requests (ETag / Last-Modified) so repeated runs do not
  re-download unchanged documents, and so that the exact bytes behind a value are kept.
"""

from __future__ import annotations

import calendar
import hashlib
import json
import logging
import os
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

USER_AGENT = "ICP-Intelligence/2.0 (+data-provenance pipeline; contact: operator)"
DEFAULT_TIMEOUT = (15, 90)
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CACHE_DIR = Path(os.getenv("SOURCE_CACHE_DIR", _PROJECT_ROOT / "var" / "cache" / "sources"))


class SourceError(RuntimeError):
    """The upstream source could not be read or returned something unusable."""


class SourceUnavailable(SourceError):
    """Transient: network / 5xx / rate limited. Safe to retry later."""


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def month_end(period: pd.Period) -> date:
    return date(period.year, period.month, calendar.monthrange(period.year, period.month)[1])


def business_days_in_month(period: pd.Period) -> int:
    start = date(period.year, period.month, 1)
    return len(pd.bdate_range(start, month_end(period)))


@dataclass
class SourceDocument:
    """One retrieved artefact (HTML page, PDF, XLSX, XML response)."""

    url: str
    sha256: str
    bytes: int
    content_type: str
    retrieved_at: str
    local_path: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    from_cache: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ValueEvidence:
    """Where one monthly value came from."""

    period: str
    value: float
    source_url: str
    document_sha256: str | None
    extraction_method: str  # text-layer | ocr | html | xlsx | api | web-service
    verification_status: str  # verified | machine_extracted | conflict | legacy_unverified
    retrieved_at: str
    reference: str | None = None  # e.g. Kepmen number, series id, sheet/column
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class MonthlyObservation:
    period: str
    value: float
    n_obs: int
    expected_obs: int
    complete: bool
    first_obs: str | None = None
    last_obs: str | None = None


@dataclass
class SourceFetch:
    dataset: str
    provider: str
    series_id: str
    unit: str
    frequency: str  # monthly | daily->monthly
    definition: str
    monthly: pd.DataFrame  # columns: period, value, n_obs, expected_obs, complete
    documents: list[SourceDocument] = field(default_factory=list)
    evidence: list[ValueEvidence] = field(default_factory=list)
    retrieved_at: str = field(default_factory=utcnow_iso)
    license_note: str = ""
    warnings: list[str] = field(default_factory=list)

    def complete_series(self) -> pd.Series:
        """Complete months only, as a PeriodIndex series (the only thing allowed into a dataset)."""
        df = self.monthly[self.monthly["complete"]]
        idx = pd.PeriodIndex([pd.Period(p, freq="M") for p in df["period"]], freq="M")
        return pd.Series(df["value"].to_numpy(dtype=float), index=idx, name=self.dataset).sort_index()

    def partial_months(self) -> list[str]:
        return [str(p) for p in self.monthly.loc[~self.monthly["complete"], "period"]]


class HttpClient:
    """requests.Session with retries, per-host politeness and a conditional on-disk cache."""

    def __init__(
        self,
        cache_dir: Path | None = None,
        min_interval_s: float = 1.0,
        timeout: tuple[int, int] = DEFAULT_TIMEOUT,
        retries: int = 4,
    ):
        self.cache_dir = Path(cache_dir or DEFAULT_CACHE_DIR)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.min_interval_s = min_interval_s
        self.timeout = timeout
        self._last_call: dict[str, float] = {}
        self._lock = threading.Lock()
        self.session = requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        retry = Retry(
            total=retries,
            connect=retries,
            read=min(2, retries),
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
            respect_retry_after_header=True,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))

    def _throttle(self, url: str) -> None:
        host = urlparse(url).netloc
        with self._lock:
            wait = self.min_interval_s - (time.monotonic() - self._last_call.get(host, 0.0))
            if wait > 0:
                time.sleep(wait)
            self._last_call[host] = time.monotonic()

    def _cache_paths(self, url: str) -> tuple[Path, Path]:
        key = hashlib.sha256(url.encode()).hexdigest()[:32]
        return self.cache_dir / f"{key}.body", self.cache_dir / f"{key}.json"

    def get(
        self, url: str, *, max_age_s: int | None = None, params: dict[str, Any] | None = None
    ) -> tuple[bytes, SourceDocument]:
        """GET with conditional revalidation. ``max_age_s`` serves the cached copy without a request."""
        if params:
            url = requests.Request("GET", url, params=params).prepare().url or url
        body_path, meta_path = self._cache_paths(url)
        meta: dict[str, Any] = (
            json.loads(meta_path.read_text()) if meta_path.exists() and body_path.exists() else {}
        )
        if meta and max_age_s is not None:
            age = time.time() - meta.get("stored_at", 0)
            if age < max_age_s:
                body = body_path.read_bytes()
                return body, SourceDocument(
                    url,
                    meta["sha256"],
                    len(body),
                    meta.get("content_type", ""),
                    meta["retrieved_at"],
                    str(body_path),
                    meta.get("etag"),
                    meta.get("last_modified"),
                    from_cache=True,
                )
        headers = {}
        if meta.get("etag"):
            headers["If-None-Match"] = meta["etag"]
        if meta.get("last_modified"):
            headers["If-Modified-Since"] = meta["last_modified"]
        self._throttle(url)
        try:
            resp = self.session.get(url, headers=headers, timeout=self.timeout)
        except requests.RequestException as exc:
            if meta:
                logger.warning("source unreachable, using cached copy of %s (%s)", url, type(exc).__name__)
                body = body_path.read_bytes()
                return body, SourceDocument(
                    url,
                    meta["sha256"],
                    len(body),
                    meta.get("content_type", ""),
                    meta["retrieved_at"],
                    str(body_path),
                    meta.get("etag"),
                    meta.get("last_modified"),
                    from_cache=True,
                )
            raise SourceUnavailable(f"{url}: {type(exc).__name__}") from exc
        if resp.status_code == 304 and meta:
            body = body_path.read_bytes()
            meta["stored_at"] = time.time()
            meta_path.write_text(json.dumps(meta))
            return body, SourceDocument(
                url,
                meta["sha256"],
                len(body),
                meta.get("content_type", ""),
                meta["retrieved_at"],
                str(body_path),
                meta.get("etag"),
                meta.get("last_modified"),
                from_cache=True,
            )
        if resp.status_code >= 500 or resp.status_code == 429:
            raise SourceUnavailable(f"{url}: HTTP {resp.status_code}")
        if resp.status_code >= 400:
            raise SourceError(f"{url}: HTTP {resp.status_code}")
        body = resp.content
        doc = SourceDocument(
            url,
            sha256_bytes(body),
            len(body),
            resp.headers.get("Content-Type", ""),
            utcnow_iso(),
            str(body_path),
            resp.headers.get("ETag"),
            resp.headers.get("Last-Modified"),
        )
        body_path.write_bytes(body)
        meta_path.write_text(json.dumps({**doc.to_dict(), "stored_at": time.time()}))
        return body, doc


def aggregate_daily_to_monthly(
    daily: pd.DataFrame,
    value_col: str,
    how: str,
    today: date | None = None,
    min_coverage: float = 0.6,
    settle_days: int = 7,
) -> pd.DataFrame:
    """Aggregate daily observations to calendar months with explicit completeness.

    ``how`` is ``mean`` (monthly average) or ``last`` (month-end observation).
    A month is complete only when (a) the month has ended, (b) enough business days were observed
    (``min_coverage`` of the business days), and (c) either the last observation lies in the final
    week of the month or at least ``settle_days`` have passed since month end (publication delay).
    """
    today = today or date.today()
    df = daily.dropna(subset=[value_col]).copy()
    df["date"] = pd.to_datetime(df["date"])
    df["period"] = df["date"].dt.to_period("M")
    rows = []
    for period, grp in df.groupby("period"):
        grp = grp.sort_values("date")
        value = float(grp[value_col].mean()) if how == "mean" else float(grp[value_col].iloc[-1])
        expected = business_days_in_month(period)
        last = grp["date"].iloc[-1].date()
        ended = today > month_end(period)
        settled = (month_end(period) - last).days <= 7 or (today - month_end(period)).days >= settle_days
        complete = ended and len(grp) >= max(1, int(min_coverage * expected)) and settled
        rows.append(
            MonthlyObservation(
                str(period),
                round(value, 4),
                len(grp),
                expected,
                bool(complete),
                str(grp["date"].iloc[0].date()),
                str(last),
            )
        )
    out = pd.DataFrame([asdict(r) for r in rows])
    return out.sort_values("period").reset_index(drop=True)
