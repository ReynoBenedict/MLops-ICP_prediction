"""Dataset versions: seeding, connector ingestion, validated imports, activation, provenance.

Rules
* Every dataset change creates a new immutable version (content-addressed CSV in the object
  store + a metadata row). Nothing is ever overwritten or deleted.
* Connector ingestion only stores *complete* months (partial current months never enter a
  dataset); upstream revisions are recorded in the version diff and in source evidence.
* Manual imports are previewed first. A commit that changes or removes existing observations is
  refused unless ``confirm_overwrite`` is set; the diff is stored with the version and audited.
* Data that fails validation (schema, duplicates, nulls, impossible values) is never activated.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Any, Literal

import pandas as pd

from icp_api.errors import ApiError
from icp_api.storage.objects import ObjectExists, ObjectNotFound, ObjectStore
from icp_api.storage.repository import MetadataRepository, Page, new_id, utcnow
from icp_api.storage.sql import IntegrityError
from icp_ml.features import build_panel
from icp_ml.schema import (
    DATASET_SPECS,
    DATASETS,
    QualityReport,
    diff_series,
    freshness,
    parse_csv_bytes,
    series_to_csv_bytes,
    sha256_bytes,
    spec,
)
from icp_ml.sources.base import SourceFetch

logger = logging.getLogger(__name__)

ImportMode = Literal["merge", "replace"]
BENCHMARKS = ("wti", "brent", "dubai")


@dataclass
class ActiveData:
    series: pd.Series
    version: dict[str, Any]


@dataclass
class PanelData:
    """Aligned monthly panel (ICP + benchmarks + FX) and the dataset version behind each column."""

    panel: pd.DataFrame
    versions: dict[str, dict[str, Any]]

    @property
    def version_ids(self) -> dict[str, str]:
        return {k: v["id"] for k, v in self.versions.items()}


def _slug(provider: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in provider.lower()).strip("_")[:40]


class DatasetService:
    def __init__(self, repo: MetadataRepository, objects: ObjectStore):
        self.repo = repo
        self.objects = objects
        self._cache: dict[str, tuple[str, pd.Series]] = {}
        self._lock = threading.Lock()

    # -- reading ------------------------------------------------------------------------------
    def active(self, dataset: str) -> ActiveData:
        sp = spec(dataset)
        row = self.repo.get_active_dataset(dataset)
        if row is None:
            raise ApiError(
                503, "DATASET_UNAVAILABLE", f"No active {sp.label_en} dataset version is configured."
            )
        with self._lock:
            cached = self._cache.get(dataset)
            if cached and cached[0] == row["id"]:
                return ActiveData(cached[1], row)
        try:
            data = self.objects.get(row["object_key"])
        except ObjectNotFound as exc:
            raise ApiError(
                503,
                "DATASET_OBJECT_MISSING",
                f"The active {sp.label_en} dataset file is missing from object storage.",
            ) from exc
        if sha256_bytes(data) != row["sha256"]:
            raise ApiError(
                503,
                "DATASET_INTEGRITY_ERROR",
                f"The active {sp.label_en} dataset failed its checksum verification.",
            )
        series, report = parse_csv_bytes(data, dataset)
        if series is None:
            raise ApiError(
                503,
                "DATASET_INVALID",
                f"The active {sp.label_en} dataset no longer validates.",
                {"errors": [e.message for e in report.errors]},
            )
        with self._lock:
            self._cache[dataset] = (row["id"], series)
        return ActiveData(series, row)

    def optional(self, dataset: str) -> ActiveData | None:
        try:
            return self.active(dataset)
        except ApiError:
            return None

    def panel(self) -> PanelData:
        """ICP is required; WTI/Brent/Dubai/USD-IDR are included when an active version exists."""
        icp = self.active("icp")
        versions = {"icp": icp.version}
        bench: dict[str, pd.Series] = {}
        for ds in BENCHMARKS:
            a = self.optional(ds)
            if a is not None:
                bench[ds] = a.series
                versions[ds] = a.version
        fx = self.optional("usd_idr")
        if fx is not None:
            versions["usd_idr"] = fx.version
        return PanelData(build_panel(icp.series, bench, fx.series if fx is not None else None), versions)

    def get_version(self, version_id: str) -> dict[str, Any]:
        row = self.repo.get_dataset_version(version_id)
        if row is None:
            raise ApiError(404, "NOT_FOUND", "Dataset version not found.")
        return row

    def download(self, version_id: str) -> tuple[dict[str, Any], bytes]:
        row = self.get_version(version_id)
        try:
            return row, self.objects.get(row["object_key"])
        except ObjectNotFound as exc:
            raise ApiError(
                404, "DATASET_OBJECT_MISSING", "Dataset file is missing from object storage."
            ) from exc

    def list_versions(self, dataset: str | None, limit: int, cursor: str | None) -> Page:
        if dataset is not None:
            spec(dataset)
        return self.repo.list_dataset_versions(dataset, limit, cursor)

    def provenance(self, dataset: str) -> dict[str, Any]:
        sp = spec(dataset)
        row = self.repo.get_active_dataset(dataset)
        last_run = self.repo.latest_ingestion_runs().get(dataset)
        base = {
            "dataset": dataset,
            "label_id": sp.label_id,
            "label_en": sp.label_en,
            "unit": sp.unit,
            "provider": sp.provider,
            "definition": sp.definition,
            "definition_id": sp.definition_id,
            "publication_lag_months": sp.publication_lag_months,
            "last_ingestion": last_run,
        }
        if row is None:
            return {**base, "active_version": None, "freshness": freshness(None)}
        return {
            **base,
            "active_version": row,
            "freshness": freshness(row["period_end"], expected_lag_months=sp.publication_lag_months),
        }

    def all_provenance(self) -> dict[str, dict[str, Any]]:
        return {ds: self.provenance(ds) for ds in DATASETS}

    def quality(self, dataset: str) -> dict[str, Any]:
        active = self.active(dataset)
        _, report = parse_csv_bytes(self.objects.get(active.version["object_key"]), dataset)
        return {"dataset": dataset, "version_id": active.version["id"], "report": report.to_dict()}

    def evidence(self, dataset: str, period: str | None = None) -> list[dict[str, Any]]:
        """Evidence rows. Without ``period`` only months of the active version are listed (rows for
        months later bounded out of the dataset stay queryable per period, for audit)."""
        spec(dataset)
        items = self.repo.list_evidence(dataset, period)
        if period is not None:
            return items
        active = self.optional(dataset)
        if active is None:
            return items
        periods = {str(p) for p in active.series.index}
        return [e for e in items if e.get("period") in periods]

    def verification_by_period(self, dataset: str) -> dict[str, dict[str, Any]]:
        """Verification status per month, restricted to months present in the active version
        (evidence for months that were later bounded out of the dataset is kept for audit only)."""
        status = self.repo.evidence_status_by_period(dataset)
        active = self.optional(dataset)
        if active is None:
            return status
        periods = {str(p) for p in active.series.index}
        return {p: v for p, v in status.items() if p in periods}

    def history(
        self, start: str | None = None, end: str | None = None, datasets: tuple[str, ...] = DATASETS
    ) -> list[dict[str, Any]]:
        """Monthly table across datasets; gaps are ``None``. Adds ``icp_idr`` (ICP x monthly-average JISDOR)."""
        series = {ds: a.series for ds in datasets if (a := self.optional(ds)) is not None}
        if "icp" not in series:
            raise ApiError(503, "DATASET_UNAVAILABLE", "No active ICP dataset version is configured.")
        icp = series["icp"]
        lo = pd.Period(start, freq="M") if start else icp.index.min()
        hi = pd.Period(end, freq="M") if end else max(s.index.max() for s in series.values())
        if lo > hi:
            raise ApiError(422, "VALIDATION_ERROR", "start must not be after end")
        idx = pd.period_range(lo, hi, freq="M")
        aligned = {ds: s.reindex(idx) for ds, s in series.items()}
        out = []
        for p in idx:
            row: dict[str, Any] = {"period": str(p)}
            for ds in datasets:
                v = aligned[ds][p] if ds in aligned else None
                row[DATASET_SPECS[ds].value_column] = None if v is None or pd.isna(v) else round(float(v), 4)
            icp_v, fx_v = row.get("icp_price"), row.get("usd_idr")
            row["icp_idr"] = round(icp_v * fx_v, 0) if icp_v is not None and fx_v is not None else None
            out.append(row)
        return out

    def summary(self) -> dict[str, Any]:
        datasets: dict[str, Any] = {}
        for ds in DATASETS:
            sp = DATASET_SPECS[ds]
            active = self.optional(ds)
            if active is None:
                datasets[ds] = {"available": False, "label_id": sp.label_id, "unit": sp.unit}
                continue
            s = active.series
            q = active.version["quality"] or {}
            prev = s.iloc[-2] if len(s) > 1 and (s.index[-1] - s.index[-2]).n == 1 else None
            datasets[ds] = {
                "available": True,
                "label_id": sp.label_id,
                "unit": sp.unit,
                "provider": sp.provider,
                "version_id": active.version["id"],
                "version": active.version["version"],
                "observations": len(s),
                "period_start": str(s.index.min()),
                "period_end": str(s.index.max()),
                "latest_value": round(float(s.iloc[-1]), 4),
                "previous_value": round(float(prev), 4) if prev is not None else None,
                "mom_change_pct": round(float((s.iloc[-1] - prev) / prev * 100), 2) if prev else None,
                "min": round(float(s.min()), 4),
                "max": round(float(s.max()), 4),
                "mean": round(float(s.mean()), 4),
                "missing_periods": q.get("missing_periods", []),
                "warning_count": len(q.get("warnings", [])),
                "freshness": freshness(str(s.index.max()), expected_lag_months=sp.publication_lag_months),
                "source_detail": active.version.get("source_detail"),
                "updated_at": active.version.get("created_at"),
            }
        out: dict[str, Any] = {"datasets": datasets, "relationships": {}, "icp_idr": None}
        icp_active = self.optional("icp")
        if icp_active is not None:
            icp = icp_active.series
            for ds in BENCHMARKS:
                b = self.optional(ds)
                if b is None:
                    continue
                common = icp.index.intersection(b.series.index)
                if len(common) < 3:
                    continue
                spread = icp[common] - b.series[common]
                out["relationships"][ds] = {
                    "correlation": round(float(icp[common].corr(b.series[common])), 4),
                    "spread_mean": round(float(spread.mean()), 4),
                    "spread_latest": round(float(spread.iloc[-1]), 4),
                    "spread_latest_period": str(common.max()),
                    "months": len(common),
                }
            fx = self.optional("usd_idr")
            if fx is not None:
                last = icp.index.max()
                rate = fx.series.get(last)
                if rate is not None and not pd.isna(rate):
                    out["icp_idr"] = {
                        "period": str(last),
                        "icp_usd": round(float(icp[last]), 2),
                        "usd_idr": round(float(rate), 2),
                        "icp_idr": round(float(icp[last]) * float(rate), 0),
                        "fx_definition": DATASET_SPECS["usd_idr"].definition,
                        "note": "Nilai setara rupiah dihitung dari ICP resmi (USD) dikalikan rata-rata JISDOR bulan yang sama; ICP resmi ditetapkan dalam USD.",
                    }
        return out

    # -- writing ------------------------------------------------------------------------------
    def _store(
        self,
        dataset: str,
        series: pd.Series,
        report: QualityReport,
        source: str,
        source_detail: str | None,
        actor: str,
        diff: dict[str, Any] | None,
        parent_id: str | None,
        provenance: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        data = series_to_csv_bytes(series, dataset)
        digest = sha256_bytes(data)
        existing = self.repo.find_dataset_by_sha(dataset, digest)
        if existing is not None:
            return existing, False
        key = f"datasets/{dataset}/{digest}.csv"
        try:
            self.objects.put(key, data, "text/csv", overwrite=False)
        except ObjectExists:
            pass  # content-addressed: an identical object is already stored
        _, canonical_report = parse_csv_bytes(data, dataset)
        row = {
            "id": new_id("dsv"),
            "dataset": dataset,
            "object_key": key,
            "sha256": digest,
            "size_bytes": len(data),
            "row_count": len(series),
            "period_start": str(series.index.min()),
            "period_end": str(series.index.max()),
            "source": source,
            "source_detail": source_detail,
            "quality": canonical_report.to_dict(),
            "diff": diff,
            "parent_version_id": parent_id,
            "created_at": utcnow(),
            "created_by": actor,
            "provenance": provenance,
        }
        try:
            return self.repo.insert_dataset_version(row), True
        except IntegrityError:
            again = self.repo.find_dataset_by_sha(dataset, digest)
            if again is None:
                raise
            return again, False

    def seed_from_bytes(self, dataset: str, data: bytes, source_detail: str) -> dict[str, Any] | None:
        """Create and activate the first version when no version is active yet (idempotent)."""
        if self.repo.get_active_dataset(dataset) is not None:
            return None
        series, report = parse_csv_bytes(data, dataset)
        if series is None:
            raise ApiError(
                422,
                "DATASET_INVALID",
                f"Seed {dataset} dataset is invalid.",
                {"errors": [e.message for e in report.errors]},
            )
        row, _ = self._store(
            dataset,
            series,
            report,
            "repository_seed",
            source_detail,
            "system:seed",
            None,
            None,
            {
                "note": "Seeded from repository file; values carry no machine-verified provenance until an official ingestion runs."
            },
        )
        self.repo.activate_dataset_version(dataset, row["id"])
        self._cache.pop(dataset, None)
        return self.get_version(row["id"])

    def _activate_new(self, dataset: str, row: dict[str, Any]) -> None:
        self.repo.activate_dataset_version(dataset, row["id"])
        self._cache.pop(dataset, None)

    def ingest_series(
        self,
        dataset: str,
        new: pd.Series,
        *,
        source: str,
        source_detail: str,
        actor: str,
        job_id: str | None,
        connector: str,
        provenance: dict[str, Any],
        evidence: list[dict[str, Any]] | None = None,
        documents: list[dict[str, Any]] | None = None,
        allow_revisions: bool = True,
        started_at: str | None = None,
        allow_removals: bool = False,
    ) -> dict[str, Any]:
        """Merge connector output into the active dataset as a new version, recording provenance.

        * New periods are added. Existing periods are updated only when ``allow_revisions``
          (upstream revisions are legitimate for benchmarks; for ICP a corrected value must come
          from the reconciliation workflow which calls this with ``allow_revisions=True``).
        * Periods present in the active version but absent upstream are kept unless ``allow_removals``.
        """
        started_at = started_at or utcnow()
        sp = spec(dataset)
        current_row = self.repo.get_active_dataset(dataset)
        current = self.active(dataset).series if current_row else None
        if new.empty and current is None:
            raise ApiError(
                422, "DATASET_INVALID", f"Connector returned no complete months for {sp.label_en}."
            )
        if current is not None:
            merged = new.combine_first(current) if allow_revisions else current.combine_first(new)
            if allow_removals:
                merged = merged[merged.index.isin(new.index)]
        else:
            merged = new
        merged = merged.sort_index()
        merged_series, report = parse_csv_bytes(series_to_csv_bytes(merged, dataset), dataset)
        diff = diff_series(current, merged)
        summary: dict[str, Any] = {
            "added": diff["added"],
            "changed": [c["period"] for c in diff["changed"]],
            "removed": diff["removed"],
            "unchanged": diff["unchanged"],
            "upstream_months": len(new),
            "partial_months_excluded": provenance.get("partial_months", []),
            "warnings": provenance.get("warnings", []),
        }
        for d in documents or []:
            self.repo.upsert_source_document({**d, "provider": provenance.get("provider", source)})
        run_id = new_id("ing")
        if merged_series is None:
            self.repo.insert_ingestion_run(
                {
                    "id": run_id,
                    "dataset": dataset,
                    "connector": connector,
                    "status": "failed",
                    "started_at": started_at,
                    "finished_at": utcnow(),
                    "job_id": job_id,
                    "requested_by": actor,
                    "summary": {**summary, "errors": [e.message for e in report.errors]},
                }
            )
            raise ApiError(
                422,
                "DATASET_INVALID",
                f"Merged {sp.label_en} dataset failed validation.",
                {"errors": [e.message for e in report.errors]},
            )
        if not diff["added"] and not diff["changed"] and not diff["removed"]:
            self.repo.insert_ingestion_run(
                {
                    "id": run_id,
                    "dataset": dataset,
                    "connector": connector,
                    "status": "no_change",
                    "started_at": started_at,
                    "finished_at": utcnow(),
                    "job_id": job_id,
                    "requested_by": actor,
                    "summary": summary,
                    "version_id": current_row["id"] if current_row else None,
                }
            )
            return {"committed": False, "version": current_row, "run_id": run_id, **summary}
        row, created = self._store(
            dataset,
            merged_series,
            report,
            source,
            source_detail,
            actor,
            diff,
            current_row["id"] if current_row else None,
            provenance,
        )
        self._activate_new(dataset, row)
        touched = set(diff["added"]) | {c["period"] for c in diff["changed"]}
        ev_rows = [
            {**e, "dataset": dataset, "dataset_version_id": row["id"]}
            for e in (evidence or [])
            if e["period"] in touched
        ]
        if ev_rows:
            self.repo.insert_evidence(ev_rows)
        self.repo.insert_ingestion_run(
            {
                "id": run_id,
                "dataset": dataset,
                "connector": connector,
                "status": "succeeded",
                "started_at": started_at,
                "finished_at": utcnow(),
                "job_id": job_id,
                "requested_by": actor,
                "summary": summary,
                "version_id": row["id"],
            }
        )
        return {
            "committed": True,
            "created": created,
            "version": self.get_version(row["id"]),
            "run_id": run_id,
            **summary,
        }

    def ingest_fetch(
        self, fetch: SourceFetch, actor: str, job_id: str | None = None, allow_revisions: bool = True
    ) -> dict[str, Any]:
        """Store a connector ``SourceFetch`` (complete months only) with documents and evidence."""
        started = utcnow()
        series = fetch.complete_series()
        provenance = {
            "provider": fetch.provider,
            "series_id": fetch.series_id,
            "unit": fetch.unit,
            "frequency": fetch.frequency,
            "definition": fetch.definition,
            "license_note": fetch.license_note,
            "retrieved_at": fetch.retrieved_at,
            "documents": [d.to_dict() for d in fetch.documents][:12],
            "partial_months": fetch.partial_months(),
            "warnings": fetch.warnings,
        }
        kind = {
            "monthly": "xlsx",
            "daily->monthly": "web-service" if "Bank Indonesia" in fetch.provider else "api",
        }.get(fetch.frequency, "api")
        evidence = (
            [
                {
                    "period": e.period,
                    "value": e.value,
                    "kind": e.extraction_method,
                    "verification_status": e.verification_status,
                    "source_url": e.source_url,
                    "document_sha256": e.document_sha256,
                    "reference": e.reference,
                    "retrieved_at": e.retrieved_at,
                    "note": e.notes,
                }
                for e in fetch.evidence
            ]
            if fetch.evidence
            else [
                {
                    "period": str(p),
                    "value": float(v),
                    "kind": kind,
                    "verification_status": "verified",
                    "source_url": fetch.documents[0].url if fetch.documents else fetch.provider,
                    "document_sha256": fetch.documents[0].sha256 if len(fetch.documents) == 1 else None,
                    "retrieved_at": fetch.retrieved_at,
                    "reference": fetch.series_id,
                }
                for p, v in series.items()
            ]
        )
        docs = [
            {
                "sha256": d.sha256,
                "url": d.url,
                "content_type": d.content_type,
                "bytes": d.bytes,
                "retrieved_at": d.retrieved_at,
            }
            for d in fetch.documents
        ]
        return self.ingest_series(
            fetch.dataset,
            series,
            source=_slug(fetch.provider),
            source_detail=fetch.definition,
            actor=actor,
            job_id=job_id,
            connector=fetch.provider,
            provenance=provenance,
            evidence=evidence,
            documents=docs,
            allow_revisions=allow_revisions,
            started_at=started,
        )

    def import_csv(
        self,
        dataset: str,
        data: bytes,
        mode: ImportMode,
        commit: bool,
        confirm_overwrite: bool,
        actor: str,
        source: str = "upload",
        source_detail: str | None = None,
    ) -> dict[str, Any]:
        spec(dataset)
        upload, report = parse_csv_bytes(data, dataset)
        result: dict[str, Any] = {
            "dataset": dataset,
            "mode": mode,
            "committed": False,
            "upload_report": report.to_dict(),
        }
        if upload is None:
            result.update(valid=False, diff=None, requires_confirmation=False, version=None)
            if commit:
                raise ApiError(422, "DATASET_INVALID", "Uploaded data failed validation.", result)
            return result
        current_row = self.repo.get_active_dataset(dataset)
        current = self.active(dataset).series if current_row else None
        new = (
            upload.combine_first(current).sort_index() if mode == "merge" and current is not None else upload
        )
        new_series, new_report = parse_csv_bytes(series_to_csv_bytes(new, dataset), dataset)
        diff = diff_series(current, new)
        destructive = bool(diff["changed"] or diff["removed"])
        result.update(
            valid=new_series is not None,
            diff=diff,
            requires_confirmation=destructive,
            resulting_report=new_report.to_dict(),
            version=None,
        )
        if not commit:
            return result
        if new_series is None:
            raise ApiError(422, "DATASET_INVALID", "The resulting dataset would fail validation.", result)
        if destructive and not confirm_overwrite:
            raise ApiError(
                409,
                "OVERWRITE_CONFIRMATION_REQUIRED",
                "This import changes or removes existing observations. Review the diff and resubmit with confirm_overwrite=true.",
                {"diff": diff},
            )
        if not diff["added"] and not destructive:
            result.update(
                committed=False, version=current_row, note="No changes compared with the active version."
            )
            return result
        row, created = self._store(
            dataset,
            new_series,
            new_report,
            source,
            source_detail,
            actor,
            diff,
            current_row["id"] if current_row else None,
            {"provider": "manual import", "note": source_detail},
        )
        self._activate_new(dataset, row)
        touched = set(diff["added"]) | {c["period"] for c in diff["changed"]}
        self.repo.insert_evidence(
            [
                {
                    "dataset": dataset,
                    "period": p,
                    "value": float(new_series[pd.Period(p, freq="M")]),
                    "kind": "manual",
                    "verification_status": "manual",
                    "source_url": f"upload by {actor}",
                    "reference": source_detail,
                    "retrieved_at": utcnow(),
                    "dataset_version_id": row["id"],
                }
                for p in sorted(touched)
            ]
        )
        result.update(committed=True, created=created, version=self.get_version(row["id"]))
        return result

    def activate(self, version_id: str) -> dict[str, Any]:
        row = self.get_version(version_id)
        if not (row["quality"] or {}).get("ok", False):
            raise ApiError(409, "DATASET_INVALID", "Only versions that passed validation can be activated.")
        self.repo.activate_dataset_version(row["dataset"], version_id)
        self._cache.pop(row["dataset"], None)
        return self.get_version(version_id)
