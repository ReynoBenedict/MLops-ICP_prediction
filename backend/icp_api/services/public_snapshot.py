"""Public market snapshot extension (contract C1): recent official history, coverage and evidence.

Everything here is anonymous and therefore limited to published facts:

* ``recent``: the last N months (setting ``PUBLIC_HISTORY_MONTHS``) ending at the latest official ICP
  month. ICP values are the official values of the active dataset only - a month without one is ``null``
  (never 0, never an estimate), and so is a month whose official documents disagree (verification basis
  ``conflict``: "not used until the difference is resolved") - next to the stored Brent monthly average.
  No forecast values.
* ``coverage``: span and size of the official ICP series, with ``confirmed_months`` computed by the same
  verification service as ``GET /data/verification`` (bases in ``verification.CONFIRMED``).
* ``icp_evidence``: the official ESDM documents stored for the latest month (one entry per document, so
  repeated retrievals of the same file count once). A decree number is shown only when it is verifiably
  the decree's own number; announcements have no number of their own, so their ``reference`` is ``null``.

A month whose official documents disagree is withheld everywhere in the answer: in ``recent`` and, when it
is the latest (or previous) month, in the headline fields as well (``icp_usd``, ``mom_change_pct``,
``icp_idr``; ``previous_usd``), while ``icp_evidence`` names the month's basis without listing documents
(SEC-C-04).

The answer is the same for every caller, so it is memoised in-process (``SnapshotMemo``, SEC-C-01): an
anonymous caller could otherwise make every call read every ICP evidence row (about 300-440 rows on the
local store), which on Cloudflare D1 counts against the free plan's daily rows-read allowance. The memo
key is the history setting and the process's snapshot write counter (``snapshot_data_generation``,
advanced by every dataset activation, evidence write and system forecast in this process, and by the
job runner after every job attempt, since jobs write from a worker process), so a memo hit reads
nothing from the database. An entry lives at most ``SNAPSHOT_MEMO_SECONDS``, which bounds how long a
change written by any other process (the operator CLI) or a date-dependent field (``freshness``) stays
unseen. ``generated_at`` is always the time of the response.
"""

from __future__ import annotations

import re
import threading
import time
from collections.abc import Callable, Hashable
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

import pandas as pd

from icp_api.services.verification import (
    CONFIRMED,
    LEGEND,
    NOT_LIVE,
    OFFICIAL_KINDS,
    TOLERANCE,
    VerificationService,
    doc_key,
)
from icp_api.storage.repository import snapshot_data_generation, utcnow
from icp_ml.sources.esdm_icp import DECREE_KINDS, DECREE_NUMBER_SHAPE

if TYPE_CHECKING:
    from icp_api.container import Container

DOCUMENT_KIND = {
    **dict.fromkeys(DECREE_KINDS, "decree"),
    **dict.fromkeys(OFFICIAL_KINDS - DECREE_KINDS, "announcement"),
}
_BULAN = {
    "januari": 1,
    "februari": 2,
    "maret": 3,
    "april": 4,
    "mei": 5,
    "juni": 6,
    "juli": 7,
    "agustus": 8,
    "september": 9,
    "oktober": 10,
    "november": 11,
    "desember": 12,
}
_DATE = re.compile(r"(\d{1,2}) ([A-Za-z]+) (20\d\d)")
# Verification bases whose dataset value is not shown to guests as an official ICP value (``recent`` carries
# ``null``): official documents disagree, so the month "is not used until the difference is resolved".
# ``no_evidence`` is not withheld: it means the documents are not recorded in this store (for example an
# environment seeded without the provenance store), not that the official value is in doubt.
WITHHELD_BASES = frozenset({"conflict"})
# How long one computed snapshot is reused at most (see the module docstring).
SNAPSHOT_MEMO_SECONDS = 60.0
SOURCE = "Kementerian ESDM (ICP resmi); EIA/World Bank (acuan); Bank Indonesia JISDOR (kurs)"


def _number(v: Any) -> float | None:
    """A stored value, or ``None`` for a missing/NaN one (a gap is never shown as 0)."""
    if v is None or pd.isna(v):
        return None
    return round(float(v), 4)


def recent_months(
    icp: pd.Series | None,
    brent: pd.Series | None,
    months: int,
    withheld: frozenset[str] | set[str] = frozenset(),
) -> list[dict[str, Any]]:
    """The last ``months`` calendar months ending at the latest official ICP month (never earlier than
    the first official month), ascending. Missing months, and the ``withheld`` periods (months whose
    official documents disagree), are ``None``."""
    if icp is None or months <= 0:
        return []
    observed = icp.dropna()
    if observed.empty:
        return []
    last = observed.index.max()
    first = max(observed.index.min(), last - (months - 1))
    out = []
    for p in pd.period_range(first, last, freq="M"):
        out.append(
            {
                "period": str(p),
                "icp_usd": None if str(p) in withheld else _number(icp.get(p)),
                "brent_usd": _number(brent.get(p)) if brent is not None else None,
            }
        )
    return out


def coverage(icp: pd.Series | None, verification_months: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Span of the official ICP series; ``confirmed_months`` counts the months whose verification basis
    is in the confirmed set (exactly what ``/data/verification`` reports per month)."""
    if icp is None:
        return None
    observed = icp.dropna()
    if observed.empty:
        return None
    return {
        "first_period": str(observed.index.min()),
        "last_period": str(observed.index.max()),
        "months": len(observed),
        "confirmed_months": sum(
            1 for m in verification_months if m["value"] is not None and m["basis"] in CONFIRMED
        ),
    }


def _published_on(note: Any) -> str | None:
    """ISO date of an announcement when its stored note is exactly its publication date ("13 Oktober 2026")."""
    m = _DATE.fullmatch(str(note or "").strip())
    if not m:
        return None
    month = _BULAN.get(m.group(2).lower())
    if month is None:
        return None
    try:
        return pd.Timestamp(year=int(m.group(3)), month=month, day=int(m.group(1))).strftime("%Y-%m-%d")
    except ValueError:
        return None


def _public_url(url: Any) -> str | None:
    """Stored source URL, only when it is a public page of an Indonesian government site (https, *.go.id)."""
    if not isinstance(url, str):
        return None
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    if parts.scheme != "https" or not host.endswith(".go.id") or parts.username or parts.password:
        return None
    return url


def trusted_decree_references(rows: list[dict[str, Any]]) -> dict[tuple[str, str], str | None]:
    """Decree number per (period, document) that can be shown as the decree's own number.

    A stored reference is withheld (``None``) when it does not look like a decree number, when the
    rows of the same document disagree, when the same number is attached to a decree of another month
    (a monthly decree names one month: a shared number is a cited regulation, not the decree itself), or
    when an announcement of the same month names a different decree number."""
    live = [r for r in rows if r.get("verification_status") not in NOT_LIVE]
    by_doc: dict[tuple[str, str], set[str | None]] = {}
    months_of_number: dict[str, set[str]] = {}
    announced: dict[str, set[str]] = {}
    for r in live:
        period, ref = str(r["period"]), r.get("reference")
        if r["kind"] in DECREE_KINDS:
            by_doc.setdefault((period, doc_key(r)), set()).add(ref or None)
            if ref:
                months_of_number.setdefault(str(ref), set()).add(period)
        elif r["kind"] == "announcement" and ref:
            announced.setdefault(period, set()).add(str(ref))
    out: dict[tuple[str, str], str | None] = {}
    for (period, doc), refs in by_doc.items():
        ref = next(iter(refs)) if len(refs) == 1 else None
        if (
            ref is None
            or not DECREE_NUMBER_SHAPE.fullmatch(ref)
            or months_of_number.get(ref, set()) != {period}
            or (announced.get(period) and announced[period] != {ref})
        ):
            out[(period, doc)] = None
        else:
            out[(period, doc)] = ref
    return out


def latest_evidence(
    period: str,
    value: float | None,
    month: dict[str, Any] | None,
    rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Official documents backing ``period`` (deduplicated by document), or ``None`` without any. A
    withheld month (documents disagree) gets its basis and label without any document: its value is not
    shown, and listing the documents of one side (whose links can name the value) would present that
    side as the month's source."""
    if month is None or value is None:
        return None
    if month["basis"] in WITHHELD_BASES:
        return {
            "period": period,
            "basis": month["basis"],
            "label": LEGEND[month["basis"]]["label"],
            "documents": [],
        }
    refs = trusted_decree_references(rows)
    docs: dict[tuple[str, str], dict[str, Any]] = {}
    for r in sorted(rows, key=lambda r: str(r.get("created_at") or "")):
        if str(r["period"]) != period or r["kind"] not in DOCUMENT_KIND:
            continue
        if r.get("verification_status") in NOT_LIVE or r.get("value") is None:
            continue
        if abs(float(r["value"]) - value) > TOLERANCE:
            continue  # a differing reading is never presented as the source of the month
        kind = DOCUMENT_KIND[r["kind"]]
        key = (kind, doc_key(r))
        if key in docs:
            continue
        docs[key] = {
            "kind": kind,
            "reference": refs.get((period, doc_key(r))) if kind == "decree" else None,
            "published_on": _published_on(r.get("note")) if r["kind"] == "announcement" else None,
            "url": _public_url(r.get("source_url")),
        }
    if not docs:
        return None
    order = {"decree": 0, "announcement": 1}
    return {
        "period": period,
        "basis": month["basis"],
        "label": LEGEND[month["basis"]]["label"],
        "documents": sorted(docs.values(), key=lambda d: order[d["kind"]]),
    }


def snapshot_extension(
    verification: VerificationService,
    icp: pd.Series | None,
    brent: pd.Series | None,
    months: int,
) -> tuple[dict[str, Any], frozenset[str]]:
    """``recent``, ``coverage`` and ``icp_evidence`` of the public snapshot (contract C1), and the
    withheld periods (months whose official documents disagree)."""
    if icp is None or icp.dropna().empty:
        return {"recent": [], "coverage": None, "icp_evidence": None}, frozenset()
    evidence_rows = verification.repo.all_evidence("icp")
    vmonths = verification.months("icp", evidence=evidence_rows)
    withheld = frozenset(m["period"] for m in vmonths if m["basis"] in WITHHELD_BASES)
    last = str(icp.dropna().index.max())
    month = next((m for m in vmonths if m["period"] == last), None)
    return {
        "recent": recent_months(icp, brent, months, withheld=withheld),
        "coverage": coverage(icp, vmonths),
        "icp_evidence": latest_evidence(last, month["value"] if month else None, month, evidence_rows),
    }, withheld


def withhold_headline(body: dict[str, Any], withheld: frozenset[str] | set[str]) -> dict[str, Any]:
    """The headline fields without a withheld month's value: guests never get a value whose official
    documents disagree, neither in ``recent`` nor as the headline ICP (SEC-C-04)."""
    out = dict(body)
    if out.get("icp_period") in withheld:
        out.update(icp_usd=None, mom_change_pct=None, icp_idr=None)
    if out.get("previous_period") in withheld:
        out.update(previous_period=None, previous_usd=None, mom_change_pct=None)
    return out


def build_snapshot(c: Container, months: int) -> dict[str, Any]:
    """Every field of ``GET /public/market-snapshot`` except ``generated_at``."""
    s = c.datasets.summary()
    icp = s["datasets"].get("icp") or {}
    brent = s["datasets"].get("brent") or {}
    fx = s["datasets"].get("usd_idr") or {}
    latest = c.repo.latest_system_forecast()
    fc = None
    if latest and latest.get("result"):
        # Teaser only: guests learn that a projection exists, never its values (sign-in required).
        fc = {
            "available": True,
            "issue_date": latest["issue_date"],
            "forecast_origin": latest["forecast_origin"],
            "horizon": latest["horizon"],
        }
    icp_active = c.datasets.optional("icp")
    brent_active = c.datasets.optional("brent")
    extension, withheld = snapshot_extension(
        c.verification,
        icp_active.series if icp_active is not None else None,
        brent_active.series if brent_active is not None else None,
        months,
    )
    headline = {
        "icp_period": icp.get("period_end"),
        "icp_usd": icp.get("latest_value"),
        "previous_period": str(pd.Period(icp["period_end"], freq="M") - 1)
        if icp.get("previous_value") is not None
        else None,
        "previous_usd": icp.get("previous_value"),
        "mom_change_pct": icp.get("mom_change_pct"),
        "brent_usd": brent.get("latest_value"),
        "brent_period": brent.get("period_end"),
        "usd_idr": fx.get("latest_value"),
        "icp_idr": (s.get("icp_idr") or {}).get("icp_idr"),
        "freshness": icp.get("freshness") or {"status": "unknown"},
        "source": SOURCE,
        "updated_at": icp.get("updated_at"),
        "latest_forecast": fc,
    }
    return {**withhold_headline(headline, withheld), **extension}


class SnapshotMemo:
    """One memoised snapshot per container (see the module docstring). Concurrent misses compute it
    once: the other callers wait for that result instead of reading the same rows again."""

    def __init__(
        self, ttl_seconds: float = SNAPSHOT_MEMO_SECONDS, clock: Callable[[], float] = time.monotonic
    ):
        self.ttl = ttl_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._build_lock = threading.Lock()
        self._entry: tuple[Hashable, float, dict[str, Any]] | None = None

    def _fresh(self, key: Hashable) -> dict[str, Any] | None:
        with self._lock:
            e = self._entry
            if e is not None and e[0] == key and self._clock() < e[1]:
                return e[2]
            return None

    def get(self, key: Hashable, build: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        hit = self._fresh(key)
        if hit is not None:
            return hit
        with self._build_lock:
            hit = self._fresh(key)
            if hit is not None:
                return hit
            value = build()
            if self.ttl > 0:
                with self._lock:
                    self._entry = (key, self._clock() + self.ttl, value)
            return value


def market_snapshot(c: Container, memo: SnapshotMemo) -> dict[str, Any]:
    """The answer of ``GET /public/market-snapshot``: the memoised body plus the response time."""
    months = c.settings.public_history_months
    body = memo.get((months, snapshot_data_generation()), lambda: build_snapshot(c, months))
    return {**body, "generated_at": utcnow()}
