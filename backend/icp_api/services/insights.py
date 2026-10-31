"""Insight service facade (the engine lives in ``icp_api.insights``) plus Indonesian number
formatting helpers shared by reports and alerts.

Every insight is traceable to dataset versions, forecast ids and model versions; no language model
is used and no causal claims are made.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Any

import pandas as pd

from icp_api.insights import ENGINE_VERSION, build_insights
from icp_api.insights.context import BENCHMARKS, InsightContext, clean
from icp_api.insights.narrative import TEMPLATES, Lang
from icp_api.services.datasets import DatasetService
from icp_api.storage.product import ProductRepository
from icp_api.storage.repository import utcnow
from icp_ml.schema import DATASET_SPECS
from icp_ml.sources.base import jakarta_today

if TYPE_CHECKING:
    from icp_api.services.models import ModelService
    from icp_api.services.verification import VerificationService

MONTHS_ID = [
    "Januari",
    "Februari",
    "Maret",
    "April",
    "Mei",
    "Juni",
    "Juli",
    "Agustus",
    "September",
    "Oktober",
    "November",
    "Desember",
]


def fmt_period(p: str) -> str:
    y, m = p.split("-")
    return f"{MONTHS_ID[int(m) - 1]} {y}"


def fmt_usd(v: float) -> str:
    return f"USD {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_pct(v: float) -> str:
    return f"{v:+.1f}%".replace(".", ",")


def fmt_dec(v: float, digits: int = 1) -> str:
    """Plain decimal in Indonesian notation (comma decimal separator), e.g. 1,9."""
    return f"{v:.{digits}f}".replace(".", ",")


def fmt_idr(v: float) -> str:
    return f"Rp {v:,.0f}".replace(",", ".")


class InsightService:
    """Loads an ``InsightContext`` and runs the deterministic engine (backend/icp_api/insights)."""

    def __init__(
        self,
        repo: ProductRepository,
        datasets: DatasetService,
        verification: VerificationService | None = None,
        models: ModelService | None = None,
    ):
        self.repo = repo
        self.datasets = datasets
        self.verification = verification
        self.models = models

    def context(self, today: date | None = None) -> InsightContext | None:
        icp_a = self.datasets.optional("icp")
        if icp_a is None:
            return None
        versions: dict[str, dict[str, Any]] = {}
        series: dict[str, pd.Series] = {}
        for ds in ("icp", "brent", "dubai", "wti", "usd_idr"):
            a = icp_a if ds == "icp" else self.datasets.optional(ds)
            if a is None:
                continue
            versions[ds] = {"id": a.version["id"], "period_end": a.version["period_end"]}
            series[ds] = clean(a.series)
        freshness = {ds: self.datasets.provenance(ds)["freshness"] for ds in versions}
        system = self.repo.system_forecasts(2)
        latest_fc = system[0] if system else None
        prev_fc = system[1] if len(system) > 1 else None
        evals: dict[str, dict[str, Any]] = {}
        production = self.models.production_version_or_none() if self.models else None
        if self.models:
            for v in {production, *(fc.get("model_version") for fc in system)}:
                if v and (facts := self.models.evaluation_facts(str(v))) is not None:
                    evals[str(v)] = facts
        failed = [r for r in self.repo.latest_ingestion_runs().values() if r.get("status") == "failed"]
        return InsightContext(
            today=today or jakarta_today(),
            icp=series["icp"],
            benchmarks={b: series[b] for b in BENCHMARKS if b in series},
            fx=series.get("usd_idr"),
            versions=versions,
            freshness=freshness,
            verification=self.verification.months("icp") if self.verification else [],
            latest_fc=latest_fc,
            prev_fc=prev_fc,
            evals=evals,
            production_version=str(production) if production else None,
            failed_runs=sorted(failed, key=lambda r: str(r.get("dataset"))),
        )

    def generate(self, lang: Lang = "id", audience: str = "customer") -> dict[str, Any]:
        ctx = self.context()
        if ctx is None:
            return {
                "generated_at": utcnow(),
                "lang": lang,
                "engine_version": ENGINE_VERSION,
                "basis": {"icp_period": None, "datasets": {}},
                "insights": [],
                "disclaimer": TEMPLATES[lang]["disclaimer"],
            }
        labels = {
            ds: {"id": sp.label_id, "en": sp.label_en, "provider": sp.provider}
            for ds, sp in DATASET_SPECS.items()
        }
        out = build_insights(ctx, lang, audience, labels)
        return {
            "generated_at": utcnow(),
            "basis": {
                "icp_period": str(ctx.last) if ctx.last is not None else None,
                "datasets": {
                    ds: {"version_id": v["id"], "period_end": v["period_end"]}
                    for ds, v in ctx.versions.items()
                },
            },
            **out,
        }
