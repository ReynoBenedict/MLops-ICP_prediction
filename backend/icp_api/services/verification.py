"""Per-month verification summary with plain-language labels that never overclaim.

ICP months are classified by *what actually backs the value* (source_evidence rows), not only by the
reconciliation status: "verified" in the pipeline can mean two agreeing ESDM documents, a single
machine-readable decree, or one ESDM document corroborated only by the legacy repository CSV. Market
benchmarks and FX are published by their provider (EIA, World Bank, Bank Indonesia); they are never
"verified" in the ESDM-document sense.
"""

from __future__ import annotations

import re
from typing import Any

from icp_api.services.datasets import DatasetService
from icp_api.storage.repository import MetadataRepository

OFFICIAL_KINDS = frozenset({"decree_text", "decree_ocr", "announcement", "announcement_backref"})
TOLERANCE = 0.0051

BASES = (
    "two_documents",
    "decree_text",
    "document_plus_legacy",
    "single_document",
    "ocr_single",
    "conflict",
    "no_evidence",
    "provider_published",
)

LEGEND: dict[str, dict[str, dict[str, str]]] = {
    "two_documents": {
        "label": {"id": "Dua dokumen ESDM sepakat", "en": "Two ESDM documents agree"},
        "explanation": {
            "id": "Dua atau lebih dokumen resmi ESDM yang berbeda (keputusan menteri, pengumuman bulan itu, atau rujukan pada pengumuman bulan berikutnya) menyebut nilai yang sama.",
            "en": "Two or more different official ESDM documents (ministerial decree, that month's announcement, or the reference in the following month's announcement) state the same value.",
        },
    },
    "decree_text": {
        "label": {"id": "Teks keputusan menteri", "en": "Ministerial decree text"},
        "explanation": {
            "id": "Nilai dibaca dari lapisan teks keputusan menteri (bukan pindaian). Belum tentu ada dokumen kedua yang mengonfirmasi.",
            "en": "The value was read from the text layer of the ministerial decree (not a scan). A second confirming document is not necessarily recorded.",
        },
    },
    "document_plus_legacy": {
        "label": {"id": "Satu dokumen ESDM + arsip lama", "en": "One ESDM document + legacy archive"},
        "explanation": {
            "id": "Satu dokumen resmi ESDM menyebut nilai ini dan nilai yang sama tercatat di arsip data lama; belum ada dokumen ESDM kedua.",
            "en": "One official ESDM document states this value and the same value appears in the legacy data archive; no second ESDM document yet.",
        },
    },
    "single_document": {
        "label": {"id": "Satu dokumen ESDM", "en": "One ESDM document"},
        "explanation": {
            "id": "Hanya satu dokumen resmi ESDM yang ditemukan untuk bulan ini; belum dikonfirmasi dokumen kedua.",
            "en": "Only one official ESDM document was found for this month; not yet confirmed by a second document.",
        },
    },
    "ocr_single": {
        "label": {"id": "Dibaca otomatis (OCR), satu dokumen", "en": "Machine-read (OCR), one document"},
        "explanation": {
            "id": "Nilai dibaca otomatis dari pindaian keputusan menteri; bulan dan nomor keputusan cocok, tetapi belum dikonfirmasi dokumen kedua.",
            "en": "The value was read automatically from a scanned ministerial decree; month and decree number match, but no second document confirms it yet.",
        },
    },
    "conflict": {
        "label": {"id": "Sumber tidak sepakat", "en": "Sources disagree"},
        "explanation": {
            "id": "Dokumen resmi menyebut nilai yang berbeda; nilai bulan ini tidak dipakai sampai perbedaannya diselesaikan.",
            "en": "Official documents state different values; this month is not used until the difference is resolved.",
        },
    },
    "no_evidence": {
        "label": {"id": "Bukti belum tercatat", "en": "No evidence recorded"},
        "explanation": {
            "id": "Nilai ada dalam dataset aktif, tetapi dokumen sumbernya belum tercatat di sistem ini.",
            "en": "The value is in the active dataset, but its source documents are not recorded in this system yet.",
        },
    },
    "provider_published": {
        "label": {"id": "Dipublikasikan penyedia data", "en": "Published by the data provider"},
        "explanation": {
            "id": "Rata-rata bulanan dari data yang dipublikasikan penyedia (EIA, Bank Dunia, atau Bank Indonesia) seperti yang diambil; tidak diverifikasi silang dengan dokumen lain.",
            "en": "Monthly average of data published by the provider (EIA, World Bank or Bank Indonesia) as retrieved; not cross-checked against other documents.",
        },
    },
}

# Bases that count as confirmed by an official document of the month itself.
CONFIRMED = frozenset({"two_documents", "decree_text"})
NOT_LIVE = frozenset({"rejected", "dissent"})

KIND_LABEL: dict[str, dict[str, str]] = {
    "decree_text": {"id": "Teks keputusan menteri", "en": "Ministerial decree text"},
    "decree_ocr": {"id": "Keputusan menteri (dibaca OCR)", "en": "Ministerial decree (OCR)"},
    "announcement": {"id": "Pengumuman ESDM bulan itu", "en": "ESDM announcement for the month"},
    "announcement_backref": {
        "id": "Rujukan di pengumuman bulan berikutnya",
        "en": "Reference in the following month's announcement",
    },
    "legacy_dataset": {"id": "Arsip data lama", "en": "Legacy data archive"},
    "legacy_csv": {"id": "Arsip data lama", "en": "Legacy data archive"},
}
_OUTVOTED = re.compile(r"outvoted by (\d+)")


def _fmt(v: float, lang: str) -> str:
    return f"{v:.2f}".replace(".", ",") if lang == "id" else f"{v:.2f}"


def _reason(row: dict[str, Any]) -> dict[str, str]:
    if row.get("verification_status") == "rejected":
        return {
            "id": "Nilai lama di arsip data diganti dengan nilai dari dokumen resmi ESDM saat rekonsiliasi.",
            "en": "The older archive value was replaced by the value from official ESDM documents during reconciliation.",
        }
    m = _OUTVOTED.search(str(row.get("note") or ""))
    if m:
        n = m.group(1)
        return {
            "id": f"Bacaan ini berbeda dari nilai yang disepakati {n} sumber lain; nilai yang disepakati yang dipakai.",
            "en": f"This reading differs from the value {n} other sources agree on; the agreed value is used.",
        }
    return {
        "id": "Bacaan ini berbeda dari nilai yang didukung dokumen lain untuk bulan ini; bacaan ini tidak dipakai.",
        "en": "This reading differs from the value supported by other documents for this month; it is not used.",
    }


def dissent_view(value: float | None, rows: list[dict[str, Any]], include_urls: bool) -> dict[str, Any]:
    """Readings that differ from the value in use (outvoted or superseded), with a plain reason.

    Customers see the reading, its kind, reference and reason; operators also get the source URL."""
    seen: set[tuple[str, float]] = set()
    out: list[dict[str, Any]] = []
    for r in rows:
        # outvoted/superseded rows, and readings stored before v6 with the month's status although
        # their value differs from the value in use
        if r.get("value") is None or r.get("verification_status") in ("legacy_unverified", "manual"):
            continue
        v = round(float(r["value"]), 4)
        if value is None or abs(v - value) <= TOLERANCE:
            continue
        key = (str(r["kind"]), v)
        if key in seen:
            continue
        seen.add(key)
        kind = str(r["kind"])
        out.append(
            {
                "value": v,
                "kind": kind,
                "kind_label": KIND_LABEL.get(kind, {"id": kind, "en": kind}),
                "reference": r.get("reference"),
                "reason": _reason(r),
                "source_url": r.get("source_url") if include_urls else None,
            }
        )
    if not out:
        return {"dissent": [], "dissent_label": None}
    label = {lang: ", ".join(_fmt(d["value"], lang) for d in out) for lang in ("id", "en")}
    readings = "reading" if len(out) == 1 else "readings"
    return {
        "dissent": out,
        "dissent_label": {
            "id": f"Sumber berbeda: {len(out)} bacaan lain ({label['id']}) tidak dipakai",
            "en": f"Sources differ: {len(out)} other {readings} ({label['en']}) not used",
        },
    }


def doc_key(e: dict[str, Any]) -> str:
    """One official document (repeated retrievals of the same file share the key)."""
    return str(e.get("document_sha256") or e.get("source_url") or "")


def classify_icp_month(value: float | None, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Status/basis of one ICP month from its evidence rows (superseded ``rejected`` rows ignored)."""
    # superseded (``rejected``) and outvoted (``dissent``) readings never decide a month's status;
    # they are shown separately by ``dissent_view``
    live = [r for r in rows if r.get("verification_status") not in NOT_LIVE]
    kinds = sorted({str(r["kind"]) for r in live})
    refs = sorted({str(r["reference"]) for r in live if r.get("reference")})
    if not live or all(r.get("verification_status") in ("legacy_unverified", "manual") for r in live):
        return {"status": "no_evidence", "basis": "no_evidence", "documents": 0, "kinds": kinds, "refs": refs}
    statuses = {str(r["verification_status"]) for r in live}
    agreeing = [
        r
        for r in live
        if value is not None and r.get("value") is not None and abs(float(r["value"]) - value) <= TOLERANCE
    ]
    docs = {doc_key(r) for r in agreeing if r["kind"] in OFFICIAL_KINDS}
    n_docs = len(docs)
    # the most recent reconciliation decides the status (older rows may predate a re-run)
    latest = max(live, key=lambda r: str(r.get("created_at") or ""))
    status = str(latest["verification_status"])
    if status not in statuses:  # defensive; cannot happen
        status = sorted(statuses)[0]
    if status == "conflict" or value is None:
        basis = "conflict"
        status = "conflict"
    elif status == "machine_extracted":
        basis = "ocr_single"
    elif status == "verified":
        if n_docs >= 2:
            basis = "two_documents"
        elif any(r["kind"] == "decree_text" for r in agreeing):
            basis = "decree_text"
        elif any(r["kind"] not in OFFICIAL_KINDS for r in agreeing):
            basis = "document_plus_legacy"  # corroborated only by a non-ESDM (legacy) record
        else:
            basis = "single_document"
    elif status == "single_source":
        basis = "single_document"
    else:
        basis, status = "no_evidence", "no_evidence"
    return {"status": status, "basis": basis, "documents": n_docs, "kinds": kinds, "refs": refs}


class VerificationService:
    def __init__(self, repo: MetadataRepository, datasets: DatasetService):
        self.repo = repo
        self.datasets = datasets

    def months(
        self, dataset: str, include_urls: bool = False, evidence: list[dict[str, Any]] | None = None
    ) -> list[dict[str, Any]]:
        """Every month of the active version with its status, basis and labels (unclipped).

        ``evidence`` lets a caller that already read ``repo.all_evidence(dataset)`` reuse those rows."""
        active = self.datasets.optional(dataset)
        if active is None:
            return []
        series = active.series
        by_period: dict[str, list[dict[str, Any]]] = {}
        for r in self.repo.all_evidence(dataset) if evidence is None else evidence:
            by_period.setdefault(str(r["period"]), []).append(r)
        out: list[dict[str, Any]] = []
        for p, v in series.items():
            period = str(p)
            value = None if v is None or v != v else round(float(v), 4)  # NaN check
            rows = by_period.get(period, [])
            if dataset == "icp":
                c = classify_icp_month(value, rows)
                dissent = dissent_view(value, rows, include_urls)
            else:
                c = {
                    "status": "provider_published",
                    "basis": "provider_published",
                    "documents": 0,
                    "kinds": sorted({str(r["kind"]) for r in rows}),
                    "refs": sorted({str(r["reference"]) for r in rows if r.get("reference")}),
                }
                dissent = {"dissent": [], "dissent_label": None}
            out.append(
                {
                    "period": period,
                    "value": value,
                    "status": c["status"],
                    "basis": c["basis"],
                    "documents": c["documents"],
                    "kinds": c["kinds"],
                    "reference": "; ".join(c["refs"]) or None,
                    "label": LEGEND[c["basis"]]["label"],
                    "explanation": LEGEND[c["basis"]]["explanation"],
                    **dissent,
                }
            )
        return out

    def by_period(self, dataset: str) -> dict[str, dict[str, Any]]:
        return {m["period"]: m for m in self.months(dataset)}

    def summary(
        self, dataset: str, floor: str | None, history_years: int | None, include_urls: bool = False
    ) -> dict[str, Any]:
        months = [m for m in self.months(dataset, include_urls) if floor is None or m["period"] >= floor]
        counts = dict.fromkeys(BASES, 0)
        for m in months:
            counts[m["basis"]] += 1
        return {
            "dataset": dataset,
            "history_years": history_years,
            "earliest_period": floor,
            "counts": counts,
            "dissent_months": sum(1 for m in months if m["dissent"]),
            "months": months,
            "legend": LEGEND,
        }
