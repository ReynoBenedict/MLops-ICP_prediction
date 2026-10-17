"""Branded, downloadable reports (PDF / Excel / CSV) built only from stored data and forecast records.

Report kinds
* ``market_monthly``       - monthly market report: official ICP, benchmarks, FX, rupiah equivalent,
                             36-month chart, 12-month table, generated insights, latest system forecast,
                             sources & versions, methodology, limitations.
* ``forecast``             - one forecast run: assumptions per month, intervals, method, baselines.
* ``scenario_comparison``  - baseline vs saved scenarios side by side.
* ``data_provenance``      - dataset coverage, versions, verification status of every ICP month.

Every report states the issue date, the forecast origin, dataset versions, sources and the method,
and labels each value as observed / derived / estimate / forecast. Files are immutable objects.
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import date
from typing import Any

import pandas as pd

from icp_api.errors import ApiError
from icp_api.services.datasets import DatasetService
from icp_api.services.forecasts import ForecastService
from icp_api.services.insights import InsightService, fmt_idr, fmt_period, fmt_usd
from icp_api.services.models import ModelService
from icp_api.services.scenarios import ScenarioService
from icp_api.storage.objects import ObjectStore
from icp_api.storage.product import ProductRepository
from icp_api.storage.repository import new_id, utcnow
from icp_ml.schema import DATASET_SPECS

logger = logging.getLogger(__name__)

KINDS = ("market_monthly", "forecast", "scenario_comparison", "data_provenance")
KIND_LABELS = {
    "market_monthly": "Laporan Pasar Bulanan",
    "forecast": "Laporan Proyeksi ICP",
    "scenario_comparison": "Perbandingan Skenario",
    "data_provenance": "Laporan Sumber & Provenans Data",
}
BRAND = {
    "name": "ICP Intelligence",
    "tagline": "Intelijen harga minyak mentah Indonesia",
    "navy": (0.07, 0.16, 0.30),
    "teal": (0.0, 0.55, 0.55),
    "amber": (0.93, 0.60, 0.13),
    "ink": (0.12, 0.14, 0.18),
    "muted": (0.42, 0.46, 0.53),
    "line": (0.86, 0.88, 0.91),
}
KIND_LEGEND = {
    "observed": "Teramati (resmi)",
    "derived": "Turunan",
    "estimate_awaiting_publication": "Estimasi (menunggu publikasi)",
    "future_forecast": "Proyeksi (bulan mendatang)",
    "scenario": "Asumsi skenario",
    "assumed_hold_last": "Asumsi: nilai terakhir dipertahankan",
}
METHOD_PLAIN = {
    "spread_rule": "Harga acuan bulan target ditambah rata-rata selisih ICP terhadap acuan tersebut pada beberapa bulan terakhir.",
    "naive": "ICP bulan terakhir dipertahankan (acuan pembanding paling sederhana).",
    "blend": "Kombinasi tertimbang Brent, Dubai dan WTI yang dikalibrasi pada riwayat ICP.",
    "ridge": "Regresi terregularisasi atas harga acuan bulan target dan ICP bulan sebelumnya.",
    "gradient_boosting": "Ensambel pohon keputusan atas harga acuan dan ICP bulan sebelumnya.",
    "random_forest": "Ensambel pohon keputusan atas harga acuan dan ICP bulan sebelumnya.",
    "linear": "Regresi linear atas harga acuan bulan target dan ICP bulan sebelumnya.",
}


def _chart_png(history: list[dict[str, Any]], forecast_points: list[dict[str, Any]] | None = None) -> bytes:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    df = pd.DataFrame(history)
    fig, ax = plt.subplots(figsize=(7.2, 3.0), dpi=160)
    x = pd.PeriodIndex(df["period"], freq="M").to_timestamp()
    if "brent_price" in df:
        ax.plot(x, df["brent_price"], color="#7a8699", lw=1.2, label="Brent (rata-rata bulanan)")
    ax.plot(x, df["icp_price"], color="#123a5c", lw=2.0, label="ICP resmi")
    if forecast_points:
        fx = pd.PeriodIndex([p["period"] for p in forecast_points], freq="M").to_timestamp()
        fy = [p["prediction"] for p in forecast_points]
        ax.plot(fx, fy, color="#d99a2b", lw=2.0, ls="--", marker="o", ms=3.5, label="Proyeksi / estimasi")
        lo = [p["intervals"].get("0.8", [None, None])[0] for p in forecast_points]
        hi = [p["intervals"].get("0.8", [None, None])[1] for p in forecast_points]
        if all(v is not None for v in lo + hi):
            ax.fill_between(fx, lo, hi, color="#d99a2b", alpha=0.18, lw=0, label="Rentang 80%")
    ax.grid(True, color="#e6e9ee", lw=0.8)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.set_ylabel("USD per barel", fontsize=8, color="#5c6675")
    ax.tick_params(labelsize=7.5, colors="#5c6675")
    ax.legend(fontsize=7.5, frameon=False, ncol=4, loc="upper left")
    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return buf.getvalue()


class ReportService:
    def __init__(
        self,
        repo: ProductRepository,
        datasets: DatasetService,
        forecasts: ForecastService,
        models: ModelService,
        insights: InsightService,
        scenarios: ScenarioService,
        objects: ObjectStore,
    ):
        self.repo = repo
        self.datasets = datasets
        self.forecasts = forecasts
        self.models = models
        self.insights = insights
        self.scenarios = scenarios
        self.objects = objects

    # -- public API --------------------------------------------------------------------------
    def generate(
        self,
        kind: str,
        params: dict[str, Any],
        actor: str,
        org_id: str | None,
        request_id: str | None,
        formats: tuple[str, ...] = ("pdf", "xlsx", "csv"),
    ) -> dict[str, Any]:
        if kind not in KINDS:
            raise ApiError(422, "VALIDATION_ERROR", f"Jenis laporan tidak dikenal: {kind}")
        rid = new_id("rpt")
        created = utcnow()
        row: dict[str, Any] = {
            "id": rid,
            "org_id": org_id,
            "created_by": actor,
            "kind": kind,
            "params": params,
            "status": "failed",
            "created_at": created,
        }
        try:
            content = self._build(kind, params, actor, org_id, request_id)
            prefix = f"exports/reports/{created[:4]}/{created[5:7]}/{rid}"
            if "pdf" in formats:
                row["object_key_pdf"] = f"{prefix}.pdf"
                self.objects.put(
                    row["object_key_pdf"], self._pdf(content), "application/pdf", overwrite=False
                )
            if "xlsx" in formats:
                row["object_key_xlsx"] = f"{prefix}.xlsx"
                self.objects.put(
                    row["object_key_xlsx"],
                    self._xlsx(content),
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    overwrite=False,
                )
            if "csv" in formats:
                row["object_key_csv"] = f"{prefix}.csv"
                self.objects.put(row["object_key_csv"], self._csv(content), "text/csv", overwrite=False)
            row.update(status="succeeded", finished_at=utcnow(), summary=content["summary"])
        except ApiError as exc:
            row.update(error_message=f"{exc.code}: {exc.message}"[:500], finished_at=utcnow())
            self.repo.insert_report(row)
            raise
        except Exception as exc:
            logger.exception("report generation failed")
            row.update(error_message=f"{type(exc).__name__}: {exc}"[:500], finished_at=utcnow())
            self.repo.insert_report(row)
            raise ApiError(500, "REPORT_FAILED", "Pembuatan laporan gagal.") from exc
        self.repo.insert_report(row)
        if org_id:
            self.repo.record_usage(org_id, None, None, "report.generate", request_id=request_id)
        return self.get(rid, org_id, operator=org_id is None)

    def get(self, report_id: str, org_id: str | None, operator: bool = False) -> dict[str, Any]:
        row = self.repo.get_report(report_id)
        if row is None or (not operator and row.get("org_id") not in (org_id, None)):
            raise ApiError(404, "NOT_FOUND", "Laporan tidak ditemukan.")
        return {
            **row,
            "label": KIND_LABELS[row["kind"]],
            "formats": [f for f in ("pdf", "xlsx", "csv") if row.get(f"object_key_{f}")],
        }

    def list_reports(self, org_id: str | None, limit: int, cursor: str | None) -> dict[str, Any]:
        page = self.repo.list_reports(org_id, limit, cursor)
        return {
            "items": [
                {
                    **r,
                    "label": KIND_LABELS[r["kind"]],
                    "formats": [f for f in ("pdf", "xlsx", "csv") if r.get(f"object_key_{f}")],
                }
                for r in page.items
            ],
            "next_cursor": page.next_cursor,
        }

    def download(
        self, report_id: str, fmt: str, org_id: str | None, operator: bool = False
    ) -> tuple[bytes, str, str]:
        row = self.get(report_id, org_id, operator)
        key = row.get(f"object_key_{fmt}")
        if not key:
            raise ApiError(404, "NOT_FOUND", f"Format {fmt} tidak tersedia untuk laporan ini.")
        media = {
            "pdf": "application/pdf",
            "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "csv": "text/csv",
        }[fmt]
        return self.objects.get(key), media, f"icp-intelligence_{row['kind']}_{report_id}.{fmt}"

    # -- content assembly --------------------------------------------------------------------
    def _common(self) -> dict[str, Any]:
        summary = self.datasets.summary()
        prov = self.datasets.all_provenance()
        return {
            "market": summary,  # datasets summary; "summary" is the report's own result summary
            "provenance": prov,
            "issued_at": utcnow(),
            "issue_date": date.today().isoformat(),
        }

    def _build(
        self, kind: str, params: dict[str, Any], actor: str, org_id: str | None, request_id: str | None
    ) -> dict[str, Any]:
        base = self._common()
        icp = base["market"]["datasets"].get("icp") or {}
        if not icp.get("available"):
            raise ApiError(503, "DATASET_UNAVAILABLE", "Riwayat ICP belum tersedia.")
        content: dict[str, Any] = {
            "kind": kind,
            "title": KIND_LABELS[kind],
            **base,
            "sections": [],
            "tables": {},
            "chart": None,
            "summary": {},
        }
        if kind == "market_monthly":
            self._market(content, params)
        elif kind == "forecast":
            self._forecast(content, params, actor, org_id, request_id)
        elif kind == "scenario_comparison":
            self._scenarios(content, params, actor, org_id, request_id)
        else:
            self._provenance(content)
        content["sources"] = self._sources_rows(content["provenance"])
        return content

    def _sources_rows(self, prov: dict[str, Any]) -> list[dict[str, Any]]:
        rows = []
        for ds, p in prov.items():
            v = p.get("active_version") or {}
            rows.append(
                {
                    "dataset": DATASET_SPECS[ds].label_id,
                    "provider": p["provider"],
                    "definition": p["definition"],
                    "version": v.get("id") or "-",
                    "coverage": f"{v.get('period_start', '-')} s.d. {v.get('period_end', '-')}"
                    if v
                    else "tidak tersedia",
                    "freshness": (p.get("freshness") or {}).get("status", "-"),
                }
            )
        return rows

    def _history_table(self, months: int) -> list[dict[str, Any]]:
        hist = self.datasets.history()
        return hist[-months:]

    def _market(self, content: dict[str, Any], params: dict[str, Any]) -> None:
        s = content["market"]
        icp, fx = s["datasets"]["icp"], s["datasets"].get("usd_idr") or {}
        period = icp["period_end"]
        hist36 = self._history_table(36)
        hist12 = hist36[-12:]
        latest_fc = self.repo.latest_system_forecast()
        fc_points = latest_fc["result"]["points"] if latest_fc and latest_fc.get("result") else []
        content["chart"] = _chart_png(hist36, fc_points)
        content["title"] = f"Laporan Pasar ICP - {fmt_period(period)}"
        yoy = next(
            (
                h
                for h in self.datasets.history(
                    start=str(pd.Period(period, freq="M") - 12), end=str(pd.Period(period, freq="M") - 12)
                )
            ),
            None,
        )
        kpis = [
            (
                "ICP resmi terbaru",
                f"{fmt_usd(icp['latest_value'])} / bbl",
                f"{fmt_period(period)} (teramati)",
            ),
            (
                "Perubahan bulanan",
                f"{icp['mom_change_pct']:+.1f}%" if icp.get("mom_change_pct") is not None else "-",
                f"dari {fmt_usd(icp['previous_value'])}"
                if icp.get("previous_value")
                else "bulan sebelumnya tidak tersedia",
            ),
            (
                "Perubahan tahunan",
                f"{(icp['latest_value'] - yoy['icp_price']) / yoy['icp_price'] * 100:+.1f}%"
                if yoy and yoy.get("icp_price")
                else "-",
                f"vs {fmt_period(str(pd.Period(period, freq='M') - 12))}",
            ),
        ]
        for ds, label in (("brent", "Brent"), ("dubai", "Dubai"), ("wti", "WTI")):
            d = s["datasets"].get(ds)
            if d and d.get("available"):
                kpis.append(
                    (
                        f"{label} (rata-rata bulanan)",
                        f"{fmt_usd(d['latest_value'])} / bbl",
                        f"{fmt_period(d['period_end'])} (teramati)",
                    )
                )
        if s.get("icp_idr"):
            kpis.append(
                (
                    "Setara rupiah",
                    f"{fmt_idr(s['icp_idr']['icp_idr'])} / bbl",
                    f"ICP x JISDOR rata-rata {fmt_period(s['icp_idr']['period'])} (turunan)",
                )
            )
        if fx.get("available"):
            kpis.append(
                (
                    "Kurs USD/IDR (JISDOR)",
                    fmt_idr(fx["latest_value"]),
                    f"rata-rata {fmt_period(fx['period_end'])} (teramati)",
                )
            )
        content["kpis"] = kpis
        ins = self.insights.generate()
        content["insights"] = ins["insights"][:6]
        content["forecast"] = latest_fc
        content["model"] = self._model_plain()
        content["tables"]["Riwayat 12 bulan"] = hist12
        content["tables"]["Riwayat 36 bulan"] = hist36
        if fc_points:
            content["tables"]["Proyeksi"] = self._forecast_rows(fc_points)
        content["summary"] = {
            "period": period,
            "icp": icp["latest_value"],
            "mom_pct": icp.get("mom_change_pct"),
            "forecast_id": latest_fc["id"] if latest_fc else None,
        }
        content["sections"] = [
            (
                "Ringkasan",
                f"ICP resmi {fmt_period(period)} ditetapkan {fmt_usd(icp['latest_value'])} per barel"
                + (
                    f", {icp['mom_change_pct']:+.1f}% dibanding bulan sebelumnya."
                    if icp.get("mom_change_pct") is not None
                    else "."
                )
                + " Nilai acuan dan kurs di bawah ini adalah rata-rata bulanan dari sumber yang disebutkan pada bagian Sumber Data.",
            ),
        ]

    def _forecast_rows(self, points: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = []
        for p in points:
            iv80 = p["intervals"].get("0.8")
            b = p["benchmarks"]
            used = [k for k in ("brent", "dubai", "wti") if b[k]["used_by_model"]]
            rows.append(
                {
                    "Bulan": fmt_period(p["period"]),
                    "Jenis": KIND_LEGEND.get(p["kind"], p["kind"]),
                    "Proyeksi (USD/bbl)": round(p["prediction"], 2),
                    "Rentang 80% bawah": round(iv80[0], 2) if iv80 else None,
                    "Rentang 80% atas": round(iv80[1], 2) if iv80 else None,
                    "Input acuan": "; ".join(
                        f"{k.upper() if k == 'wti' else k.capitalize()} {b[k]['value']:.2f} ({KIND_LEGEND.get(b[k]['source'], b[k]['source'])})"
                        for k in used
                        if b[k]["value"] is not None
                    ),
                    "Setara rupiah": round(p["prediction_idr"]) if p.get("prediction_idr") else None,
                    "Kurs": p.get("fx_rate"),
                    "Sumber kurs": KIND_LEGEND.get(p.get("fx_source") or "", p.get("fx_source")),
                }
            )
        return rows

    def _model_plain(self) -> dict[str, Any]:
        try:
            d = self.models.describe_production()
        except ApiError as exc:
            return {"available": False, "message": exc.message}
        m = d["metrics"]
        return {
            "available": True,
            "version": d["version"],
            "method": d["method"],
            "family": d["method_family"],
            "plain": METHOD_PLAIN.get(d["method_family"], d["description"]),
            "mae": m.get("mae"),
            "naive_mae": m.get("baseline_naive_last_mae"),
            "hold_last_mae": m.get("hold_last_h1_mae"),
            "naive_hold_last_mae": m.get("baseline_naive_hold_last_h1_mae"),
        }

    def _forecast(
        self,
        content: dict[str, Any],
        params: dict[str, Any],
        actor: str,
        org_id: str | None,
        request_id: str | None,
    ) -> None:
        fid = params.get("forecast_id")
        if fid:
            run = self.forecasts.get(fid)
            if run.get("org_id") not in (None, org_id) and org_id is not None:
                raise ApiError(404, "NOT_FOUND", "Proyeksi tidak ditemukan.")
        else:
            run = self.forecasts.run(
                int(params.get("horizon", 3)),
                params.get("scenario") or {},
                actor,
                request_id,
                workflow="report",
                org_id=org_id,
            )
        if run["status"] != "succeeded":
            raise ApiError(409, "FORECAST_FAILED", "Proyeksi yang dipilih gagal dan tidak dapat dilaporkan.")
        res = run["result"]
        pts = res["points"]
        hist = self._history_table(24)
        content["chart"] = _chart_png(hist, pts)
        content["title"] = f"Laporan Proyeksi ICP - terbit {res['issue_date']}"
        content["forecast"] = run
        content["model"] = self._model_plain()
        content["kpis"] = [
            (
                "Titik awal (ICP resmi terakhir)",
                fmt_period(res["forecast_origin"]),
                "bulan resmi terakhir yang digunakan",
            ),
            ("Tanggal terbit", res["issue_date"], "informasi yang tersedia pada tanggal ini"),
            (
                "Horizon",
                f"{res['horizon']} bulan",
                f"{fmt_period(pts[0]['period'])} s.d. {fmt_period(pts[-1]['period'])}",
            ),
            ("Metode", res["method"], "lihat bagian Metodologi"),
        ]
        content["tables"]["Proyeksi"] = self._forecast_rows(pts)
        content["tables"]["Riwayat 24 bulan"] = hist
        content["summary"] = {
            "forecast_id": run["id"],
            "forecast_origin": res["forecast_origin"],
            "issue_date": res["issue_date"],
        }
        content["sections"] = [
            (
                "Ringkasan",
                f"Proyeksi diterbitkan {res['issue_date']} dengan titik awal ICP resmi {fmt_period(res['forecast_origin'])}. "
                "Bulan yang telah berakhir tetapi belum ditetapkan ESDM ditampilkan sebagai estimasi menunggu publikasi; bulan mendatang sebagai proyeksi "
                "bersyarat pada asumsi harga acuan yang tercantum.",
            )
        ]
        if res.get("warnings"):
            content["sections"].append(("Catatan", " ".join(res["warnings"])))

    def _scenarios(
        self,
        content: dict[str, Any],
        params: dict[str, Any],
        actor: str,
        org_id: str | None,
        request_id: str | None,
    ) -> None:
        if not org_id:
            raise ApiError(422, "VALIDATION_ERROR", "Perbandingan skenario memerlukan organisasi.")
        ids = list(params.get("scenario_ids") or [])
        if not ids:
            raise ApiError(422, "VALIDATION_ERROR", "Pilih minimal satu skenario.")
        cmp = self.scenarios.compare(org_id, actor, ids, request_id, params.get("horizon"))
        content["title"] = f"Perbandingan Skenario ICP - terbit {cmp['issue_date']}"
        names = {s["id"]: s["name"] for s in cmp["scenarios"]}
        rows = []
        for r in cmp["rows"]:
            row = {
                "Bulan": fmt_period(r["period"]),
                "Jenis": KIND_LEGEND.get(r["kind"], r["kind"]),
                "Baseline (USD/bbl)": round(r["baseline"], 2),
            }
            for sid, v in r["scenarios"].items():
                row[f"{names[sid]} (USD/bbl)"] = round(v["prediction"], 2)
                row[f"{names[sid]} vs baseline"] = round(v["delta_vs_baseline"], 2)
            rows.append(row)
        content["tables"]["Perbandingan"] = rows
        content["tables"]["Asumsi skenario"] = [
            {"Skenario": s["name"], "Input": k, "Bulan": fmt_period(p), "Nilai": v}
            for s in cmp["scenarios"]
            for k, months in s["inputs"].items()
            for p, v in months.items()
        ]
        hist = self._history_table(24)
        content["chart"] = _chart_png(
            hist, [{"period": r["period"], "prediction": r["baseline"], "intervals": {}} for r in cmp["rows"]]
        )
        content["model"] = self._model_plain()
        content["comparison"] = cmp
        content["kpis"] = [
            ("Titik awal", fmt_period(cmp["forecast_origin"]), "ICP resmi terakhir"),
            ("Tanggal terbit", cmp["issue_date"], ""),
            ("Horizon", f"{cmp['horizon']} bulan", ""),
            ("Skenario dibandingkan", str(len(cmp["scenarios"])), "+ baseline"),
        ]
        content["summary"] = {
            "baseline_forecast_id": cmp["baseline_forecast_id"],
            "scenarios": [s["id"] for s in cmp["scenarios"]],
        }
        content["sections"] = [("Ringkasan", cmp["note"])]

    def _provenance(self, content: dict[str, Any]) -> None:
        content["title"] = "Laporan Sumber & Provenans Data"
        status = self.datasets.verification_by_period("icp")
        icp = self.datasets.active("icp").series
        rows = [
            {
                "Bulan": fmt_period(str(p)),
                "ICP (USD/bbl)": round(float(v), 2),
                "Status verifikasi": (status.get(str(p)) or {}).get("status", "legacy_unverified"),
                "Referensi": (status.get(str(p)) or {}).get("reference") or "",
            }
            for p, v in icp.items()
        ]
        content["tables"]["Verifikasi ICP per bulan"] = rows
        content["kpis"] = [
            ("Bulan ICP tersedia", str(len(icp)), f"{icp.index.min()} s.d. {icp.index.max()}"),
            (
                "Terverifikasi ganda",
                str(sum(1 for r in rows if r["Status verifikasi"] == "verified")),
                "dua sumber resmi sepakat",
            ),
            (
                "Menunggu verifikasi",
                str(sum(1 for r in rows if r["Status verifikasi"] not in ("verified",))),
                "satu sumber / OCR / warisan",
            ),
        ]
        content["summary"] = {"months": len(icp)}
        content["sections"] = [
            (
                "Ringkasan",
                "Setiap nilai ICP dilacak ke dokumen sumbernya (keputusan menteri atau pengumuman resmi ESDM). "
                "Status 'verified' berarti dua dokumen resmi independen menyebutkan nilai yang sama.",
            )
        ]

    # -- renderers ---------------------------------------------------------------------------
    def _pdf(self, c: dict[str, Any]) -> bytes:
        from reportlab.lib import colors
        from reportlab.lib.enums import TA_LEFT
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import mm
        from reportlab.platypus import (
            Image,
            PageBreak,
            Paragraph,
            SimpleDocTemplate,
            Spacer,
            Table,
            TableStyle,
        )

        navy, teal, amber, ink, muted, line = (
            colors.Color(*BRAND[k]) for k in ("navy", "teal", "amber", "ink", "muted", "line")
        )
        styles = getSampleStyleSheet()
        h1 = ParagraphStyle(
            "h1",
            parent=styles["Title"],
            fontName="Helvetica-Bold",
            fontSize=22,
            leading=26,
            textColor=colors.white,
            alignment=TA_LEFT,
        )
        h2 = ParagraphStyle(
            "h2",
            parent=styles["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=12.5,
            textColor=navy,
            spaceBefore=10,
            spaceAfter=4,
        )
        body = ParagraphStyle(
            "body", parent=styles["BodyText"], fontName="Helvetica", fontSize=9.2, leading=13, textColor=ink
        )
        small = ParagraphStyle("small", parent=body, fontSize=7.8, leading=10.5, textColor=muted)
        kpi_v = ParagraphStyle(
            "kpiv", parent=body, fontName="Helvetica-Bold", fontSize=12.5, leading=15, textColor=navy
        )
        kpi_l = ParagraphStyle("kpil", parent=small, textColor=muted)
        buf = io.BytesIO()

        def on_page(canvas: Any, doc: Any) -> None:
            w, h = A4
            canvas.saveState()
            if doc.page == 1:
                canvas.setFillColor(navy)
                canvas.rect(0, h - 62 * mm, w, 62 * mm, stroke=0, fill=1)
                canvas.setFillColor(teal)
                canvas.rect(0, h - 62 * mm, w, 2.2 * mm, stroke=0, fill=1)
                # brand mark: three rising bars + arrow, drawn with primitives (same geometry as the SVG logo)
                x0, y0 = 18 * mm, h - 24 * mm
                canvas.setFillColor(colors.white)
                for i, bh in enumerate((4, 7, 10)):
                    canvas.roundRect(x0 + i * 4.2 * mm, y0, 3 * mm, bh * mm, 0.8 * mm, stroke=0, fill=1)
                canvas.setStrokeColor(amber)
                canvas.setLineWidth(1.6)
                canvas.line(x0 - 1 * mm, y0 + 1 * mm, x0 + 14.5 * mm, y0 + 12 * mm)
                canvas.setFont("Helvetica-Bold", 13)
                canvas.drawString(x0 + 18 * mm, y0 + 5 * mm, BRAND["name"])
                canvas.setFont("Helvetica", 8.5)
                canvas.drawString(x0 + 18 * mm, y0 + 0.5 * mm, BRAND["tagline"])
            canvas.setFont("Helvetica", 7.5)
            canvas.setFillColor(muted)
            canvas.drawString(
                18 * mm,
                10 * mm,
                f"{BRAND['name']} - {c['title']} - diterbitkan {c['issue_date']} (UTC). Bukan nasihat investasi.",
            )
            canvas.drawRightString(w - 18 * mm, 10 * mm, f"Halaman {doc.page}")
            canvas.restoreState()

        doc = SimpleDocTemplate(
            buf,
            pagesize=A4,
            leftMargin=18 * mm,
            rightMargin=18 * mm,
            topMargin=20 * mm,
            bottomMargin=18 * mm,
            title=c["title"],
            author=BRAND["name"],
            subject=KIND_LABELS[c["kind"]],
        )
        story: list[Any] = [
            Spacer(1, 14 * mm),
            Paragraph(c["title"], h1),
            Paragraph(
                f"Diterbitkan {c['issue_date']} - data per {c['summary'] and c.get('provenance', {}).get('icp', {}).get('active_version', {}).get('period_end', '-')}",
                ParagraphStyle("sub", parent=body, textColor=colors.white, fontSize=10),
            ),
            Spacer(1, 22 * mm),
        ]
        kpis = c.get("kpis") or []
        if kpis:
            cells = [
                [
                    Paragraph(v, kpi_v),
                ]
                for _, v, _ in kpis
            ]
            data = []
            for i in range(0, len(kpis), 3):
                chunk = kpis[i : i + 3]
                data.append(
                    [
                        [Paragraph(label, kpi_l), Paragraph(value, kpi_v), Paragraph(note, small)]
                        for label, value, note in chunk
                    ]
                    + [""] * (3 - len(chunk))
                )
            t = Table(data, colWidths=[58 * mm] * 3)
            t.setStyle(
                TableStyle(
                    [
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ("BOX", (0, 0), (-1, -1), 0.5, line),
                        ("INNERGRID", (0, 0), (-1, -1), 0.5, line),
                        ("TOPPADDING", (0, 0), (-1, -1), 6),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                    ]
                )
            )
            story += [t, Spacer(1, 6)]
            del cells
        for heading, text in c.get("sections", []):
            story += [Paragraph(heading, h2), Paragraph(text, body)]
        if c.get("chart"):
            story += [
                Paragraph("Grafik", h2),
                Image(io.BytesIO(c["chart"]), width=174 * mm, height=72.5 * mm),
                Paragraph(
                    "Garis putus-putus dan area berwarna adalah estimasi/proyeksi dengan rentang 80% dari kesalahan historis; garis penuh adalah nilai resmi/teramati.",
                    small,
                ),
            ]
        for name, rows in c["tables"].items():
            if not rows:
                continue
            if name.startswith("Riwayat 36"):
                continue  # in Excel/CSV only
            story += [Paragraph(name, h2), self._pdf_table(rows, body, small, navy, line)]
        model = c.get("model") or {}
        if model.get("available"):
            story += [
                Paragraph("Metodologi", h2),
                Paragraph(
                    f"Metode aktif: <b>{model['method']}</b>. {model['plain']} Pemilihan metode dilakukan dengan uji mundur bergulir (rolling-origin) "
                    "terhadap seluruh kandidat termasuk acuan paling sederhana; metode yang lebih rumit hanya dipilih bila terbukti lebih akurat.",
                    body,
                ),
                Paragraph(self._accuracy_plain(model), body),
            ]
        story += [
            Paragraph("Sumber data dan versi", h2),
            self._pdf_table(c["sources"], body, small, navy, line, widths=[30, 28, 62, 26, 28]),
            Paragraph("Keterbatasan", h2),
            Paragraph(
                "ICP resmi ditetapkan oleh Kementerian ESDM dan diterbitkan awal bulan berikutnya; bulan yang telah berakhir tetapi belum ditetapkan ditampilkan sebagai estimasi. "
                "Proyeksi bulan mendatang bergantung pada asumsi harga acuan yang tercantum dan tidak memperhitungkan kejadian yang belum terjadi. Rentang ketidakpastian dihitung dari "
                "kesalahan historis dan dapat meremehkan risiko pada periode bergejolak. Nilai rupiah adalah turunan (ICP x kurs JISDOR) dan bukan harga resmi dalam rupiah.",
                body,
            ),
            Spacer(1, 4),
            Paragraph("Legenda jenis nilai: " + "; ".join(f"{v}" for v in KIND_LEGEND.values()) + ".", small),
        ]
        if c.get("insights"):
            story += [
                PageBreak(),
                Paragraph("Insight pasar (dihasilkan otomatis dari data terverifikasi)", h2),
            ]
            for ins in c["insights"]:
                story += [
                    Paragraph(f"<b>{ins['title']}</b>", body),
                    Paragraph(ins["body"], body),
                    Spacer(1, 4),
                ]
        doc.build(story, onFirstPage=on_page, onLaterPages=on_page)
        return buf.getvalue()

    def _accuracy_plain(self, model: dict[str, Any]) -> str:
        if model.get("mae") is None:
            return "Statistik akurasi belum tersedia."
        s = (
            f"Pada uji mundur, rata-rata selisih absolut (MAE) estimasi 1 bulan ke depan adalah USD {model['mae']:.2f} per barel ketika harga acuan bulan target sudah diketahui "
            f"(bulan yang telah berakhir, menunggu publikasi), dibanding USD {model['naive_mae']:.2f} bila hanya mengulang ICP bulan lalu. "
            "Artinya, untuk bulan yang sudah berakhir, estimasi biasanya meleset sekitar nilai tersebut dari angka resmi yang kemudian terbit."
        )
        if model.get("hold_last_mae") is not None:
            s += (
                f" Untuk bulan yang belum berakhir tanpa skenario harga acuan, kesalahan rata-rata naik menjadi USD {model['hold_last_mae']:.2f} per barel "
                f"(acuan sederhana: USD {model['naive_hold_last_mae']:.2f}): tanpa pandangan tentang harga minyak dunia, tidak ada metode yang jauh lebih baik dari 'sama seperti bulan lalu'."
            )
        return s

    def _pdf_table(
        self,
        rows: list[dict[str, Any]],
        body: Any,
        small: Any,
        navy: Any,
        line: Any,
        widths: list[int] | None = None,
    ) -> Any:
        from reportlab.lib import colors
        from reportlab.lib.units import mm
        from reportlab.platypus import Paragraph, Table, TableStyle

        cols = list(rows[0].keys())
        pretty = {
            "period": "Bulan",
            "icp_price": "ICP",
            "wti_price": "WTI",
            "brent_price": "Brent",
            "dubai_price": "Dubai",
            "usd_idr": "USD/IDR",
            "usd_idr_eom": "USD/IDR akhir bulan",
            "icp_idr": "ICP (Rp/bbl)",
            "dataset": "Dataset",
            "provider": "Penyedia",
            "definition": "Definisi",
            "version": "Versi",
            "coverage": "Cakupan",
            "freshness": "Kesegaran",
        }

        def cell(v: Any) -> Any:
            if v is None:
                return Paragraph("-", small)
            if isinstance(v, float):
                return Paragraph(f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."), small)
            return Paragraph(str(v), small)

        data = [[Paragraph(f"<b>{pretty.get(col, col)}</b>", small) for col in cols]] + [
            [cell(r.get(col)) for col in cols] for r in rows[:60]
        ]
        total = 174
        cw = [w * mm for w in widths] if widths else [total / len(cols) * mm] * len(cols)
        t = Table(data, colWidths=cw, repeatRows=1)
        t.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.Color(0.93, 0.95, 0.97)),
                    ("LINEBELOW", (0, 0), (-1, 0), 0.8, navy),
                    ("INNERGRID", (0, 1), (-1, -1), 0.25, line),
                    ("BOX", (0, 0), (-1, -1), 0.5, line),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("TOPPADDING", (0, 0), (-1, -1), 3),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                ]
            )
        )
        return t

    def _xlsx(self, c: dict[str, Any]) -> bytes:
        from openpyxl import Workbook
        from openpyxl.styles import Alignment, Font, PatternFill
        from openpyxl.utils import get_column_letter

        wb = Workbook()
        ws = wb.active
        ws.title = "Ringkasan"
        ws["A1"] = c["title"]
        ws["A1"].font = Font(bold=True, size=14, color="123A5C")
        ws["A2"] = (
            f"Diterbitkan {c['issue_date']} oleh {BRAND['name']}. Semua nilai USD per barel kecuali disebutkan lain."
        )
        r = 4
        for label, value, note in c.get("kpis") or []:
            ws.cell(r, 1, label).font = Font(bold=True)
            ws.cell(r, 2, value)
            ws.cell(r, 3, note)
            r += 1
        for heading, text in c.get("sections", []):
            r += 1
            ws.cell(r, 1, heading).font = Font(bold=True)
            ws.cell(r, 2, text).alignment = Alignment(wrap_text=True)
        ws.column_dimensions["A"].width = 34
        ws.column_dimensions["B"].width = 40
        ws.column_dimensions["C"].width = 60
        head_fill = PatternFill("solid", fgColor="E8EEF5")
        for name, rows in c["tables"].items():
            if not rows:
                continue
            sh = wb.create_sheet(name[:31])
            cols = list(rows[0].keys())
            for j, col in enumerate(cols, 1):
                cell = sh.cell(1, j, col)
                cell.font = Font(bold=True)
                cell.fill = head_fill
                sh.column_dimensions[get_column_letter(j)].width = max(12, min(40, len(str(col)) + 4))
            for i, row in enumerate(rows, 2):
                for j, col in enumerate(cols, 1):
                    sh.cell(i, j, row.get(col))
            sh.freeze_panes = "A2"
        src = wb.create_sheet("Sumber data")
        cols = ["dataset", "provider", "definition", "version", "coverage", "freshness"]
        for j, col in enumerate(cols, 1):
            src.cell(1, j, col).font = Font(bold=True)
            src.column_dimensions[get_column_letter(j)].width = 28
        for i, row in enumerate(c["sources"], 2):
            for j, col in enumerate(cols, 1):
                src.cell(i, j, row.get(col))
        model = c.get("model") or {}
        meta = wb.create_sheet("Metodologi")
        meta["A1"] = "Metode aktif"
        meta["B1"] = model.get("method", "-")
        meta["A2"] = "Penjelasan"
        meta["B2"] = model.get("plain", "-")
        meta["A3"] = "Akurasi"
        meta["B3"] = self._accuracy_plain(model) if model.get("available") else "-"
        meta["A4"] = "Keterbatasan"
        meta["B4"] = (
            "Estimasi untuk bulan yang telah berakhir menggunakan harga acuan teramati; proyeksi bulan mendatang bersyarat pada asumsi. "
            "Rentang ketidakpastian berasal dari kesalahan historis."
        )
        meta.column_dimensions["A"].width = 18
        meta.column_dimensions["B"].width = 120
        buf = io.BytesIO()
        wb.save(buf)
        return buf.getvalue()

    def _csv(self, c: dict[str, Any]) -> bytes:
        _, rows = next(((n, r) for n, r in c["tables"].items() if r), ("", []))
        buf = io.StringIO()
        if rows:
            w = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        return buf.getvalue().encode("utf-8-sig")
