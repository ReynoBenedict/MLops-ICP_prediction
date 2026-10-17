"""Rule-based market insights in Indonesian, generated only from validated observations.

Every insight is traceable: it carries the exact figures it is based on, the dataset versions and
the source provider, and it is labelled as automatically generated. No language model is used; the
text is templated from numbers, so nothing is asserted that the data does not contain, and no
causal claims are made.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from icp_api.services.datasets import DatasetService
from icp_api.storage.product import ProductRepository
from icp_api.storage.repository import utcnow
from icp_ml.schema import DATASET_SPECS

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


def fmt_idr(v: float) -> str:
    return f"Rp {v:,.0f}".replace(",", ".")


@dataclass
class Insight:
    id: str
    kind: str
    severity: str  # info | notice | warning
    title: str
    body: str
    period: str | None
    metrics: list[dict[str, Any]] = field(default_factory=list)
    sources: list[dict[str, Any]] = field(default_factory=list)
    next_step: str | None = None
    generated: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class InsightService:
    def __init__(self, repo: ProductRepository, datasets: DatasetService):
        self.repo = repo
        self.datasets = datasets

    def _src(self, ds: str, version: dict[str, Any] | None) -> dict[str, Any]:
        sp = DATASET_SPECS[ds]
        return {
            "dataset": ds,
            "label": sp.label_id,
            "provider": sp.provider,
            "version_id": version["id"] if version else None,
            "period_end": version["period_end"] if version else None,
        }

    def generate(self) -> dict[str, Any]:
        out: list[Insight] = []
        icp_a = self.datasets.optional("icp")
        if icp_a is None:
            return {"generated_at": utcnow(), "basis": {}, "insights": [], "disclaimer": _DISCLAIMER}
        icp = icp_a.series
        last = icp.index.max()
        basis: dict[str, Any] = {"icp_period": str(last), "datasets": {}}
        versions: dict[str, dict[str, Any]] = {"icp": icp_a.version}
        for ds in ("wti", "brent", "dubai", "usd_idr"):
            a = self.datasets.optional(ds)
            if a is not None:
                versions[ds] = a.version
        for ds, v in versions.items():
            basis["datasets"][ds] = {"version_id": v["id"], "period_end": v["period_end"]}

        # 1. Material month-over-month change in official ICP.
        prev = last - 1
        if prev in icp.index:
            chg = float(icp[last] - icp[prev])
            pct = chg / float(icp[prev]) * 100
            sev = "warning" if abs(pct) >= 10 else "notice" if abs(pct) >= 5 else "info"
            direction = "naik" if chg > 0 else "turun" if chg < 0 else "tidak berubah"
            out.append(
                Insight(
                    f"icp_mom_{last}",
                    "material_change",
                    sev,
                    f"ICP {fmt_period(str(last))} {direction} {fmt_pct(pct)} dibanding bulan sebelumnya",
                    f"Harga Minyak Mentah Indonesia (ICP) resmi untuk {fmt_period(str(last))} ditetapkan {fmt_usd(float(icp[last]))} per barel, "
                    f"{direction} {fmt_usd(abs(chg))} ({fmt_pct(pct)}) dari {fmt_usd(float(icp[prev]))} pada {fmt_period(str(prev))}.",
                    str(last),
                    [
                        {"label": "ICP terbaru", "value": round(float(icp[last]), 2), "unit": "USD/bbl"},
                        {
                            "label": "ICP bulan sebelumnya",
                            "value": round(float(icp[prev]), 2),
                            "unit": "USD/bbl",
                        },
                        {"label": "Perubahan", "value": round(pct, 2), "unit": "%"},
                    ],
                    [self._src("icp", icp_a.version)],
                    "Bandingkan dengan pergerakan Brent dan Dubai pada bulan yang sama di Eksplorasi Harga.",
                )
            )
        # 2. Year-over-year context.
        yoy = last - 12
        if yoy in icp.index:
            pct = (float(icp[last]) - float(icp[yoy])) / float(icp[yoy]) * 100
            out.append(
                Insight(
                    f"icp_yoy_{last}",
                    "annual_comparison",
                    "info",
                    f"Dibanding {fmt_period(str(yoy))}, ICP {'lebih tinggi' if pct > 0 else 'lebih rendah'} {fmt_pct(abs(pct)).lstrip('+')}",
                    f"Setahun lalu ICP berada di {fmt_usd(float(icp[yoy]))} per barel; kini {fmt_usd(float(icp[last]))} per barel "
                    f"({fmt_pct(pct)} tahun-ke-tahun).",
                    str(last),
                    [
                        {"label": "ICP tahun lalu", "value": round(float(icp[yoy]), 2), "unit": "USD/bbl"},
                        {"label": "Perubahan YoY", "value": round(pct, 2), "unit": "%"},
                    ],
                    [self._src("icp", icp_a.version)],
                )
            )
        # 3. Spread vs benchmarks: unusual divergence relative to the trailing 12 months.
        for ds, label in (("brent", "Brent"), ("dubai", "Dubai"), ("wti", "WTI")):
            a = self.datasets.optional(ds)
            if a is None or last not in a.series.index:
                continue
            common = icp.index.intersection(a.series.index)
            spread = (icp[common] - a.series[common]).sort_index()
            hist = spread[spread.index < last].tail(12)
            if len(hist) < 6:
                continue
            latest = float(spread[last])
            mean, std = float(hist.mean()), float(hist.std(ddof=0)) or 0.01
            z = (latest - mean) / std
            if abs(z) >= 1.5:
                out.append(
                    Insight(
                        f"spread_{ds}_{last}",
                        "benchmark_divergence",
                        "notice" if abs(z) < 2.5 else "warning",
                        f"Selisih ICP terhadap {label} {'melebar' if latest > mean else 'menyempit'} tidak biasa pada {fmt_period(str(last))}",
                        f"Pada {fmt_period(str(last))} ICP {('di atas' if latest >= 0 else 'di bawah')} {label} sebesar {fmt_usd(abs(latest))} per barel, "
                        f"sedangkan rata-rata 12 bulan sebelumnya {fmt_usd(abs(mean))} ({'di atas' if mean >= 0 else 'di bawah'}). "
                        f"Penyimpangan ini setara {abs(z):.1f} simpangan baku dari pola setahun terakhir.".replace(
                            ".", ",", 0
                        ),
                        str(last),
                        [
                            {
                                "label": f"Selisih ICP-{label} bulan ini",
                                "value": round(latest, 2),
                                "unit": "USD/bbl",
                            },
                            {"label": "Rata-rata 12 bulan", "value": round(mean, 2), "unit": "USD/bbl"},
                            {"label": "Simpangan (z)", "value": round(z, 2), "unit": ""},
                        ],
                        [self._src("icp", icp_a.version), self._src(ds, a.version)],
                        "Metode proyeksi berbasis selisih mengasumsikan selisih kembali ke rata-rata; perubahan selisih yang bertahan akan menggeser proyeksi.",
                    )
                )
        # 4. Largest absolute monthly move in the last 24 months (unusual historical movement).
        recent = icp[icp.index > last - 24]
        diffs = recent.diff().dropna()
        consecutive = [(p, float(v)) for p, v in diffs.items() if (p - 1) in recent.index]
        if consecutive:
            p_max, v_max = max(consecutive, key=lambda t: abs(t[1]))
            if p_max == last and abs(v_max) >= 8:
                out.append(
                    Insight(
                        f"icp_largest_move_{last}",
                        "unusual_movement",
                        "warning",
                        f"Perubahan bulanan {fmt_period(str(last))} adalah yang terbesar dalam 24 bulan terakhir",
                        f"Pergerakan {fmt_usd(abs(v_max))} per barel dalam satu bulan melampaui seluruh perubahan bulanan sejak {fmt_period(str(recent.index.min()))}. "
                        "Perhatikan bahwa rentang ketidakpastian proyeksi dihitung dari kesalahan historis dan dapat meremehkan risiko dalam periode bergejolak.",
                        str(last),
                        [{"label": "Perubahan bulanan", "value": round(v_max, 2), "unit": "USD/bbl"}],
                        [self._src("icp", icp_a.version)],
                    )
                )
        # 5. FX context: rupiah per barrel and the FX contribution.
        fx_a = self.datasets.optional("usd_idr")
        if fx_a is not None and last in fx_a.series.index and prev in fx_a.series.index and prev in icp.index:
            fx_now, fx_prev = float(fx_a.series[last]), float(fx_a.series[prev])
            idr_now, idr_prev = float(icp[last]) * fx_now, float(icp[prev]) * fx_prev
            fx_pct = (fx_now - fx_prev) / fx_prev * 100
            idr_pct = (idr_now - idr_prev) / idr_prev * 100
            out.append(
                Insight(
                    f"fx_context_{last}",
                    "fx_context",
                    "info",
                    f"Dalam rupiah, ICP {fmt_period(str(last))} setara {fmt_idr(idr_now)} per barel ({fmt_pct(idr_pct)})",
                    f"Kurs JISDOR rata-rata {fmt_period(str(last))} adalah {fmt_idr(fx_now)} per USD ({fmt_pct(fx_pct)} dibanding bulan sebelumnya), "
                    f"sehingga nilai rupiah per barel bergerak {fmt_pct(idr_pct)} - gabungan dari perubahan harga USD dan perubahan kurs. "
                    "ICP resmi ditetapkan dalam USD; nilai rupiah adalah turunan.",
                    str(last),
                    [
                        {"label": "Kurs rata-rata", "value": round(fx_now, 2), "unit": "IDR/USD"},
                        {"label": "ICP dalam rupiah", "value": round(idr_now, 0), "unit": "IDR/bbl"},
                        {"label": "Perubahan kurs", "value": round(fx_pct, 2), "unit": "%"},
                    ],
                    [self._src("icp", icp_a.version), self._src("usd_idr", fx_a.version)],
                )
            )
        # 6. Forecast changes between the last two system forecasts for the same target month.
        latest_fc = self.repo.latest_system_forecast()
        if latest_fc and latest_fc.get("result"):
            prev_fc = self._previous_system_forecast(latest_fc["id"])
            if prev_fc and prev_fc.get("result"):
                cur = {p["period"]: p for p in latest_fc["result"]["points"]}
                old = {p["period"]: p for p in prev_fc["result"]["points"]}
                for period in sorted(set(cur) & set(old)):
                    d = cur[period]["prediction"] - old[period]["prediction"]
                    if abs(d) >= 2.0:
                        why = []
                        if latest_fc.get("model_version") != prev_fc.get("model_version"):
                            why.append("model prediksi aktif berubah")
                        if latest_fc.get("forecast_origin") != prev_fc.get("forecast_origin"):
                            why.append(f"ICP resmi {fmt_period(latest_fc['forecast_origin'])} telah terbit")
                        changed_inputs = [
                            b
                            for b in ("brent", "dubai", "wti")
                            if (cur[period]["benchmarks"][b]["value"] or 0)
                            != (old[period]["benchmarks"][b]["value"] or 0)
                            and cur[period]["benchmarks"][b]["used_by_model"]
                        ]
                        if changed_inputs:
                            why.append(
                                "data acuan "
                                + "/".join(
                                    b.upper() if b == "wti" else b.capitalize() for b in changed_inputs
                                )
                                + " diperbarui"
                            )
                        out.append(
                            Insight(
                                f"forecast_change_{period}_{latest_fc['id']}",
                                "forecast_change",
                                "notice",
                                f"Proyeksi ICP {fmt_period(period)} bergeser {fmt_usd(abs(d))} per barel sejak proyeksi sebelumnya",
                                f"Proyeksi terbitan {latest_fc['issue_date']} memperkirakan {fmt_usd(cur[period]['prediction'])}, sedangkan terbitan {prev_fc['issue_date']} "
                                f"memperkirakan {fmt_usd(old[period]['prediction'])}. "
                                + (
                                    "Penyebab yang tercatat: " + "; ".join(why) + "."
                                    if why
                                    else "Tidak ada perubahan model atau data acuan yang tercatat; perbedaan berasal dari pembaruan riwayat ICP."
                                ),
                                period,
                                [
                                    {
                                        "label": "Proyeksi terbaru",
                                        "value": round(cur[period]["prediction"], 2),
                                        "unit": "USD/bbl",
                                    },
                                    {
                                        "label": "Proyeksi sebelumnya",
                                        "value": round(old[period]["prediction"], 2),
                                        "unit": "USD/bbl",
                                    },
                                ],
                                [
                                    {
                                        "dataset": "forecast",
                                        "label": "Riwayat proyeksi",
                                        "provider": "ICP Intelligence",
                                        "version_id": latest_fc["id"],
                                        "period_end": None,
                                    },
                                    {
                                        "dataset": "forecast",
                                        "label": "Proyeksi sebelumnya",
                                        "provider": "ICP Intelligence",
                                        "version_id": prev_fc["id"],
                                        "period_end": None,
                                    },
                                ],
                                "Buka Proyeksi ICP untuk melihat asumsi input kedua terbitan.",
                            )
                        )
                        break
        # 7. Data quality: verification status and freshness.
        status = self.datasets.verification_by_period("icp")
        pending = sorted(
            p for p, s in status.items() if s.get("status") in ("pending", "single_source", "conflict")
        )
        q = icp_a.version.get("quality") or {}
        missing = q.get("missing_periods", [])
        if missing:
            out.append(
                Insight(
                    "icp_missing",
                    "data_quality",
                    "warning",
                    f"{len(missing)} bulan ICP belum tersedia dalam riwayat resmi",
                    "Bulan berikut tidak memiliki nilai ICP terverifikasi: "
                    + ", ".join(fmt_period(m) for m in missing[:12])
                    + (" dan lainnya" if len(missing) > 12 else "")
                    + ". Metode proyeksi tidak menjembatani bulan yang kosong, sehingga rentang evaluasi menjadi lebih pendek.",
                    None,
                    [{"label": "Bulan hilang", "value": len(missing), "unit": "bulan"}],
                    [self._src("icp", icp_a.version)],
                    "Jalankan pembaruan ICP resmi (Operasi) untuk mengambil penetapan yang hilang dari arsip ESDM.",
                )
            )
        if pending:
            out.append(
                Insight(
                    "icp_pending_verification",
                    "data_quality",
                    "notice",
                    f"{len(pending)} bulan ICP masih menunggu verifikasi ganda",
                    "Nilai bulan berikut baru didukung satu sumber atau hasil OCR dan belum dikonfirmasi oleh sumber kedua: "
                    + ", ".join(fmt_period(p) for p in pending[:12])
                    + ". Nilai tetap digunakan tetapi ditandai dalam tabel data.",
                    None,
                    [{"label": "Menunggu verifikasi", "value": len(pending), "unit": "bulan"}],
                    [self._src("icp", icp_a.version)],
                )
            )
        fresh = self.datasets.provenance("icp")["freshness"]
        if fresh.get("status") in ("lagging", "stale"):
            out.append(
                Insight(
                    "icp_freshness",
                    "freshness",
                    "warning" if fresh["status"] == "stale" else "notice",
                    f"ICP resmi terakhir yang tersedia adalah {fmt_period(str(last))}",
                    f"Berdasarkan jadwal publikasi ESDM (awal bulan berikutnya), {(fresh.get('expected_period') and 'bulan ' + fmt_period(fresh['expected_period']) + ' seharusnya sudah terbit') or 'data seharusnya lebih baru'}. "
                    "Bulan yang telah berakhir tetapi belum terbit ditampilkan sebagai estimasi menunggu publikasi, bukan sebagai nilai resmi.",
                    str(last),
                    [{"label": "Status kesegaran", "value": fresh["status"], "unit": ""}],
                    [self._src("icp", icp_a.version)],
                    "Jalankan pembaruan ICP resmi; jika ESDM belum menerbitkan, gunakan estimasi menunggu publikasi.",
                )
            )
        failed = [
            r for r in self.repo.list_ingestion_runs(None, 20, None).items if r.get("status") == "failed"
        ]
        if failed:
            r = failed[0]
            out.append(
                Insight(
                    f"ingestion_failed_{r['id']}",
                    "data_quality",
                    "warning",
                    f"Pembaruan data {(DATASET_SPECS.get(r['dataset'], None) and DATASET_SPECS[r['dataset']].label_id) or r['dataset']} terakhir gagal",
                    f"Percobaan pembaruan pada {r.get('started_at', '')[:16].replace('T', ' ')} UTC gagal: {str(r.get('error_message') or '')[:160]}. Data yang ditampilkan adalah versi terakhir yang berhasil.",
                    None,
                    [],
                    [self._src(r["dataset"], versions.get(r["dataset"]))]
                    if r["dataset"] in DATASET_SPECS
                    else [],
                    "Periksa halaman Operasi untuk detail kegagalan dan jalankan ulang pembaruan.",
                )
            )
        order = {"warning": 0, "notice": 1, "info": 2}
        out.sort(key=lambda i: (order[i.severity], i.kind))
        return {
            "generated_at": utcnow(),
            "basis": basis,
            "insights": [i.to_dict() for i in out],
            "disclaimer": _DISCLAIMER,
        }

    def _previous_system_forecast(self, exclude_id: str) -> dict[str, Any] | None:
        page = self.repo.list_forecasts(10, None, "succeeded")
        for row in page.items:
            if row["id"] != exclude_id and row.get("workflow") == "system":
                return row
        return None


_DISCLAIMER = (
    "Insight dihasilkan otomatis dari data terverifikasi dengan aturan yang tetap (bukan model bahasa). "
    "Setiap angka dapat ditelusuri ke versi dataset yang disebutkan. Insight menjelaskan apa yang terjadi pada data, bukan mengapa pasar bergerak."
)
