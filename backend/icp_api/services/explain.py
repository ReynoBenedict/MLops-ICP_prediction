"""Read-time explanation of forecast records for customers (stored records are never rewritten).

* per point: backtest ``evaluation`` for the same step and input situation, the official ``actual``
  once ESDM has published that month, and ``kind_now``;
* per run: localized method / interval / evaluation ``notes``, scenario ``input_usage`` (inputs
  the active method ignores), and the plan-horizon clip for customers.
"""

from __future__ import annotations

import copy
from typing import Any

from icp_api.insights.context import step_evaluation

METHOD_NOTES: dict[str, dict[str, str]] = {
    "spread_rule": {
        "id": "Harga acuan bulan target ditambah rata-rata selisih ICP terhadap acuan tersebut pada beberapa bulan terakhir.",
        "en": "The benchmark price of the target month plus the average spread of ICP over that benchmark in recent months.",
    },
    "naive": {
        "id": "ICP bulan terakhir dipertahankan (acuan pembanding paling sederhana).",
        "en": "The latest ICP is carried forward (the simplest baseline).",
    },
    "blend": {
        "id": "Kombinasi tertimbang Brent, Dubai dan WTI; bobotnya diestimasi dari riwayat ICP.",
        "en": "A weighted combination of Brent, Dubai and WTI; the weights are estimated from ICP history.",
    },
    "ridge": {
        "id": "Regresi terregularisasi atas harga acuan bulan target dan ICP bulan sebelumnya.",
        "en": "Regularised regression on the target month's benchmark prices and the previous month's ICP.",
    },
    "linear": {
        "id": "Regresi linear atas harga acuan bulan target dan ICP bulan sebelumnya.",
        "en": "Linear regression on the target month's benchmark prices and the previous month's ICP.",
    },
    "gradient_boosting": {
        "id": "Ensambel pohon keputusan atas harga acuan dan ICP bulan sebelumnya.",
        "en": "A decision-tree ensemble on benchmark prices and the previous month's ICP.",
    },
    "random_forest": {
        "id": "Ensambel pohon keputusan atas harga acuan dan ICP bulan sebelumnya.",
        "en": "A decision-tree ensemble on benchmark prices and the previous month's ICP.",
    },
}
INTERVAL_NOTE = {
    "id": "Rentang adalah kuantil empiris dari kesalahan uji mundur bergulir untuk langkah dan situasi input yang sama "
    "(harga acuan teramati atau diasumsikan bertahan). Titik yang memakai skenario bersyarat pada skenario tersebut. "
    "Rentang ini bersifat empiris, bukan jaminan: cakupan yang terukur pada uji mundur ditampilkan per titik dan bisa "
    "lebih rendah dari tingkat nominalnya, terutama pada periode bergejolak.",
    "en": "Ranges are empirical quantiles of rolling backtest errors for the same step and input situation (observed or "
    "held benchmark prices). Scenario-based points are conditional on the scenario. The ranges are empirical, not a "
    "guarantee: the coverage measured in backtests is shown per point and can be below the nominal level, especially "
    "in volatile periods.",
}
EVALUATION_NOTE = {
    "id": "Evaluasi per titik berasal dari uji mundur model yang sama saat dilatih: jumlah kasus (n), selisih absolut "
    "rata-rata, ketepatan arah naik/turun, dan bagian nilai resmi yang jatuh di dalam tiap rentang, beserta periode uji "
    "dan pembanding sederhana (ulang ICP bulan lalu pada kasus yang sama). Uji mundur yang sama dipakai untuk memilih "
    "metode, sehingga angka ini sedikit optimistis. Angka ini mengukur masa lalu dan tidak menjamin hasil di masa "
    "depan.",
    "en": "Per-point evaluation comes from backtests of the same model at training time: number of cases (n), mean "
    "absolute error, up/down direction hit rate, and the share of official values that fell inside each range, with "
    "the test period and a simple baseline (repeat last month's ICP on the same cases). The same backtest was used to "
    "select the method, so these figures are slightly optimistic. They describe the past and do not guarantee future "
    "results.",
}
INPUT_LABEL = {"brent": "Brent", "dubai": "Dubai", "wti": "WTI", "usd_idr": "USD/IDR"}


def _usage_note(inp: str, used: bool) -> dict[str, str]:
    name = INPUT_LABEL.get(inp, inp)
    if inp == "usd_idr":
        return {
            "id": "Kurs hanya memengaruhi nilai setara rupiah, bukan proyeksi ICP dalam USD.",
            "en": "The exchange rate only affects the rupiah equivalent, not the ICP forecast in USD.",
        }
    if used:
        return {
            "id": f"Asumsi {name} dipakai oleh metode proyeksi aktif.",
            "en": f"The {name} assumption is used by the active forecasting method.",
        }
    return {
        "id": f"Metode proyeksi aktif tidak memakai {name}; asumsi ini tidak mengubah proyeksi ICP.",
        "en": f"The active forecasting method does not use {name}; this assumption does not change the ICP forecast.",
    }


def input_usage(inputs: dict[str, dict[str, Any]] | None, used: set[str]) -> list[dict[str, Any]]:
    out = []
    for inp in ("brent", "dubai", "wti", "usd_idr"):
        months = sorted((inputs or {}).get(inp) or {})
        if not months:
            continue
        is_used = inp in used and inp != "usd_idr"
        out.append(
            {
                "input": inp,
                "used_by_method": is_used,
                "scenario_months": months,
                "note": _usage_note(inp, is_used),
                "effect": "rupiah_only" if inp == "usd_idr" else ("used" if is_used else "ignored"),
            }
        )
    return out


def ignored_inputs(usage: list[dict[str, Any]]) -> list[str]:
    """Scenario inputs that cannot change the ICP forecast of the active method (flagged, never silent)."""
    return [u["input"] for u in usage if u["effect"] == "ignored"]


def ignored_note(names: list[str]) -> dict[str, str] | None:
    if not names:
        return None
    label = ", ".join(INPUT_LABEL.get(n, n) for n in names)
    return {
        "id": f"Asumsi {label} tidak dipakai oleh metode proyeksi aktif dan tidak mengubah proyeksi ICP.",
        "en": f"The {label} assumption is not used by the active forecasting method and does not change the ICP forecast.",
    }


def coverage_note(points: list[dict[str, Any]]) -> dict[str, str] | None:
    """Run-level warning when any shown point's range under-covered in backtests (or was checked on few cases)."""
    flagged = [p for p in points if (p.get("evaluation") or {}).get("coverage_warning")]
    if not flagged:
        return None
    under = [p for p in flagged if (p.get("evaluation") or {}).get("under_coverage")]
    months = ", ".join(str(p["period"]) for p in (under or flagged))
    if under:
        return {
            "id": f"Rentang untuk {months} lebih sempit daripada ketidakpastian yang terukur pada uji mundur; "
            "baca rentang sebagai batas bawah ketidakpastian.",
            "en": f"The ranges for {months} were narrower than the uncertainty measured in backtests; read them as a "
            "lower bound on the uncertainty.",
        }
    return {
        "id": f"Cakupan rentang untuk {months} diuji pada sedikit kasus; angka cakupan masih kasar.",
        "en": f"Range coverage for {months} was checked on few cases; treat the coverage figures as rough.",
    }


# --- stored backtest artefacts -> evaluation facts --------------------------------------------------
def evaluation_extras(
    registry: Any, run_id: str | None, summary: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Naive baseline per (mode, horizon) and the coverage-check window, recomputed from the stored
    backtest predictions of the model run, plus the training dataset versions recorded with the model
    (read only; keys are absent when an artefact is missing - nothing is estimated)."""
    from icp_ml.evaluation import baseline_by_step, coverage_window

    out: dict[str, Any] = {"datasets": dict((summary or {}).get("datasets") or {})}
    loader = getattr(registry, "backtest_predictions", None)
    if not run_id or loader is None:
        return out
    try:
        df = loader(run_id)
    except Exception:  # noqa: BLE001 - optional context, never fatal
        return out
    if df is None or df.empty:
        return out
    from icp_ml.evaluation import embargoed_coverage

    try:
        recomputed = embargoed_coverage(df)
    except Exception:  # noqa: BLE001 - optional context, never fatal
        recomputed = {}
    return {
        **out,
        "baselines": baseline_by_step(df),
        "coverage_window": coverage_window(df),
        # ML-01: always recomputed with the embargo; compared with the stored figure at read time
        "coverage_embargoed": recomputed or None,
    }


DISCLOSURE_NOTE = {
    "id": "Semua angka berasal dari uji mundur bergulir: model dilatih ulang pada setiap titik awal hanya dengan data "
    "hingga titik itu. Pembanding = mengulang ICP bulan lalu pada kasus yang sama. Rentang adalah kuantil empiris dari "
    "kesalahan uji mundur; cakupannya diukur pada 40% titik awal terakhir yang tidak dipakai untuk menyusun rentang. "
    "Model yang diekspor sebelum 20-10-2026 menyimpan angka cakupan yang rentangnya dapat memuat hasil bulan yang juga "
    "diproyeksikan kasus uji; perhitungan ulang dengan jeda (coverage_check) ditampilkan di sebelahnya dan peringatan "
    "memakai angka yang lebih ketat. Uji mundur yang sama dipakai untuk memilih metode di antara kandidat, sehingga "
    "angka kesalahan ini sedikit optimistis.",
    "en": "All figures come from rolling-origin backtests: the model is refitted at every origin using only data up to "
    "that origin. Baseline = repeating last month's ICP on the same cases. Ranges are empirical quantiles of backtest "
    "errors; their coverage is measured on the last 40% of origins, which were not used to build the ranges. Models "
    "exported before 2026-10-20 stored a coverage figure whose ranges could include outcomes of months the test cases "
    "forecast; the embargoed recomputation is shown next to it (coverage_check) and warnings use the stricter figure. "
    "The same backtest was used to select the method among the candidates, so these errors are slightly optimistic.",
}


def evaluation_disclosure(facts: dict[str, Any] | None) -> dict[str, Any] | None:
    """Model-card disclosure: every backtest row with its period, n, mode, horizon, baseline and coverage."""
    from icp_api.insights.context import BASELINE_LABEL, _origins, step_evaluation

    if not facts:
        return None
    rows = []
    for r in sorted(facts.get("by_horizon") or [], key=lambda r: (r.get("mode"), int(r.get("horizon", 0)))):
        ev = step_evaluation(facts, r.get("mode"), int(r.get("horizon", 0)))
        if ev is None:
            continue
        rows.append(
            {
                "mode": ev["mode"],
                "horizon": ev["horizon"],
                "n": ev["n"],
                "mae": ev["mae"],
                "baseline_mae": (ev["baseline"] or {}).get("mae"),
                "beats_baseline": ev["beats_baseline"],
                "coverage": ev["coverage"],
                "coverage_n": ev["coverage_n"],
                "coverage_embargoed": ev["coverage_embargoed"],
                "coverage_embargoed_n": ev["coverage_embargoed_n"],
                "coverage_check": ev["coverage_check"],
                "under_coverage": ev["under_coverage"],
                "coverage_warning": ev["coverage_warning"],
            }
        )
    start, end = _origins(facts.get("backtest_origins"))
    return {
        "origin_start": start,
        "origin_end": end,
        "baseline_method": "naive_last",
        "baseline_label": BASELINE_LABEL,
        "calibrated": False,
        "coverage_period": facts.get("coverage_window") or None,
        "rows": rows,
        "note": DISCLOSURE_NOTE,
    }


def explain_run(
    row: dict[str, Any],
    result: dict[str, Any] | None,
    evals: dict[str, Any] | None,
    actuals: dict[str, dict[str, Any]],
    horizon_max: int | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Return (enriched result, plan_clip). ``actuals``: period -> verification month (official
    ICP values now available). ``horizon_max`` None = no clip (operators)."""
    if not result:
        return result, None
    res = copy.deepcopy(result)
    points = res.get("points") or []
    stored = len(points)
    clip = None
    if horizon_max is not None:
        kept = [p for p in points if int(p.get("step", 0)) <= horizon_max]
        clip = {"horizon_max": horizon_max, "stored_horizon": stored, "clipped": len(kept) < stored}
        points = kept
    used: set[str] = set()
    for p in points:
        ev = step_evaluation(evals, p.get("interval_basis"), int(p.get("step", 1)))
        p["evaluation"] = ev
        a = actuals.get(str(p["period"]))
        if a is not None and a.get("value") is not None:
            p["actual"] = {
                "period": a["period"],
                "value": a["value"],
                "basis": a["basis"],
                "label": a["label"],
            }
            p["kind_now"] = "official_published"
        else:
            p["actual"] = None
            p["kind_now"] = p.get("kind")
        used |= {b for b, v in (p.get("benchmarks") or {}).items() if v.get("used_by_model")}
    res["points"] = points
    if clip and clip["clipped"]:
        res["horizon"] = len(points)
    family = (res.get("model") or {}).get("method_family") or (evals or {}).get("method_family")
    res["notes"] = {
        "method": METHOD_NOTES.get(
            str(family),
            {
                "id": str((res.get("model") or {}).get("description") or "-"),
                "en": str(res.get("method") or "-"),
            },
        ),
        "interval": INTERVAL_NOTE,
        "evaluation": EVALUATION_NOTE,
    }
    res["input_usage"] = input_usage(row.get("scenario"), used)
    res["ignored_inputs"] = ignored_inputs(res["input_usage"])
    res["notes"]["ignored_inputs"] = ignored_note(res["ignored_inputs"])
    res["notes"]["coverage_warning"] = coverage_note(points)
    return res, clip
