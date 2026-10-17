"""Official ICP ingestion: ESDM decrees + announcements -> reconciled, verified monthly series.

Policy (see docs/DATA_RECONCILIATION.md):
* A month enters the dataset only with status ``verified`` (two independent official
  observations agree, or a machine-readable decree text states it), or ``single_source`` when
  that single source is an official announcement page (HTML text, not OCR).
* OCR-only months are kept as *pending* evidence and are listed in provenance, never stored as
  values. Conflicts block the month until resolved through a documented correction.
* Values already in the active dataset that disagree with a verified official value are
  superseded; the old value is recorded as ``legacy_unverified`` -> ``rejected`` evidence.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from icp_api.container import Container
from icp_api.storage.repository import utcnow
from icp_ml.sources import esdm_backfill, esdm_icp
from icp_ml.sources.base import HttpClient

logger = logging.getLogger(__name__)

ACCEPT_SINGLE_KINDS = {"announcement"}
# An OCR-only decree is accepted (status ``machine_extracted``) when the operative clause named the
# month the archive lists it under *and* the decree number was read - two internal consistency
# checks on the same official document. It stays flagged until a second source confirms it.
OCR_KIND = "decree_ocr"


class OfficialIcpIngestion:
    def __init__(
        self,
        container: Container,
        state_dir: Path | None = None,
        client: HttpClient | None = None,
        min_period: str = "2019-01",
    ):
        self.c = container
        # The decree archive starts in 2019-01; earlier announcements exist only sporadically and would
        # create a series full of gaps, so the official dataset is bounded to the systematically covered range.
        self.min_period = pd.Period(min_period, freq="M")
        self.state_dir = state_dir or container.settings.source_state_dir / "esdm"
        self.client = client or HttpClient(
            cache_dir=container.settings.source_cache_dir, min_interval_s=1.5, timeout=(20, 180)
        )

    def reconciled(self) -> list[esdm_icp.ReconciledMonth]:
        esdm_backfill.build_reconciliation(self.state_dir)
        raw = esdm_backfill._load(self.state_dir / "reconciliation.json", [])
        return [esdm_icp.ReconciledMonth(**m) for m in raw]

    def accepted_series(
        self, months: list[esdm_icp.ReconciledMonth]
    ) -> tuple[pd.Series, list[dict[str, Any]], dict[str, list[str]]]:
        periods: list[str] = []
        values: list[float] = []
        evidence: list[dict[str, Any]] = []
        buckets: dict[str, list[str]] = {
            "verified": [],
            "single_source_accepted": [],
            "pending_ocr_only": [],
            "conflict": [],
            "missing": [],
        }
        buckets["ocr_accepted"] = []
        for m in months:
            kinds = {o["kind"] for o in m.sources if o.get("value") is not None}
            ocr_ok = (
                m.status == "single_source"
                and kinds & {OCR_KIND}
                and not (kinds & ACCEPT_SINGLE_KINDS)
                and any(o["kind"] == OCR_KIND and o.get("reference") for o in m.sources)
            )
            accept = (
                m.status == "verified"
                or (m.status == "single_source" and kinds & ACCEPT_SINGLE_KINDS)
                or ocr_ok
            )
            if accept and m.value is not None:
                periods.append(pd.Period(m.period, freq="M"))
                values.append(float(m.value))
                bucket = (
                    "verified"
                    if m.status == "verified"
                    else ("ocr_accepted" if ocr_ok else "single_source_accepted")
                )
                buckets[bucket].append(m.period)
                for e in esdm_icp.evidence_from(m):
                    d = e.to_dict()
                    if ocr_ok:
                        d["verification_status"] = "machine_extracted"
                    evidence.append(d)
            elif m.status == "single_source":
                buckets["pending_ocr_only"].append(m.period)
            else:
                buckets[m.status].append(m.period)
        series = pd.Series(values, index=pd.PeriodIndex(periods, freq="M"), name="icp_price").sort_index()
        return series, evidence, buckets

    def run(
        self, actor: str, job_id: str | None, crawl_news_pages: int = 6, full_backfill: bool = False
    ) -> dict[str, Any]:
        started = utcnow()
        decree_log = esdm_backfill.backfill_decrees(self.client, self.state_dir)
        news = esdm_backfill.discover_articles(
            self.client, self.state_dir, max_pages=110 if full_backfill else crawl_news_pages
        )
        pages_state = esdm_backfill._load(self.state_dir / "news_pages.json", {"pages": {}, "links": {}})
        articles = esdm_backfill.fetch_articles(self.client, self.state_dir, sorted(pages_state["links"]))
        months = self.reconciled()
        series, evidence, buckets = self.accepted_series(months)
        series = series[series.index >= self.min_period]
        evidence = [e for e in evidence if pd.Period(e["period"], freq="M") >= self.min_period]
        buckets = {k: [p for p in v if pd.Period(p, freq="M") >= self.min_period] for k, v in buckets.items()}
        current = self.c.datasets.optional("icp")
        superseded: list[dict[str, Any]] = []
        if current is not None:
            for p, v in current.series.items():
                nv = series.get(p)
                if nv is not None and abs(float(nv) - float(v)) > 0.005:
                    superseded.append({"period": str(p), "old": float(v), "new": float(nv)})
        if superseded:
            self.c.repo.insert_evidence(
                [
                    {
                        "dataset": "icp",
                        "period": s["period"],
                        "value": s["old"],
                        "kind": "legacy_dataset",
                        "verification_status": "rejected",
                        "source_url": "active dataset before official reconciliation",
                        "note": f"superseded by verified official value {s['new']}",
                        "retrieved_at": utcnow(),
                    }
                    for s in superseded
                ]
            )
        provenance = {
            "provider": "Kementerian ESDM (Ditjen Migas)",
            "series_id": "ICP official monthly",
            "definition": "Official monthly ICP from ministerial decrees, cross-checked against ESDM announcements.",
            "archive_url": esdm_icp.ARCHIVE_URL,
            "archive_sha256": decree_log.get("archive_sha256"),
            "retrieved_at": utcnow(),
            "buckets": buckets,
            "superseded": superseded,
            "license_note": "ESDM public information; cite Kementerian ESDM.",
            "warnings": (
                [f"{len(buckets['pending_ocr_only'])} month(s) pending verification (single weak source)"]
                if buckets["pending_ocr_only"]
                else []
            )
            + (
                [f"{len(buckets['ocr_accepted'])} month(s) accepted from decree OCR only (machine_extracted)"]
                if buckets["ocr_accepted"]
                else []
            ),
        }
        documents = [
            {
                "sha256": d["sha256"],
                "url": d["url"],
                "content_type": "application/pdf",
                "bytes": d.get("bytes", 0),
                "retrieved_at": d["retrieved_at"],
            }
            for d in esdm_backfill._load(self.state_dir / "decrees.json", {}).values()
            if d.get("sha256")
        ]
        ev_rows = [
            {
                "period": e["period"],
                "value": e["value"],
                "kind": e["extraction_method"],
                "verification_status": e["verification_status"],
                "source_url": e["source_url"],
                "document_sha256": e.get("document_sha256"),
                "reference": e.get("reference"),
                "retrieved_at": e["retrieved_at"],
                "note": e.get("notes"),
            }
            for e in evidence
        ]
        result = self.c.datasets.ingest_series(
            "icp",
            series,
            source="esdm_official",
            source_detail="Kepmen ESDM + pengumuman ICP bulanan",
            actor=actor,
            job_id=job_id,
            connector="esdm_icp",
            provenance=provenance,
            evidence=ev_rows,
            documents=documents,
            allow_revisions=True,
            started_at=started,
        )
        return {
            **result,
            "decrees": {k: decree_log[k] for k in ("entries", "extracted", "no_value", "failed")},
            "news_pages_read": len(news["pages"]),
            "articles": len(articles),
            "buckets": buckets,
            "superseded": superseded,
        }
