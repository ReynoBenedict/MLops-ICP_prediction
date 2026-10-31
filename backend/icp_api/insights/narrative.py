"""Compose localized text from findings: number/period formatting per language plus templates."""

from __future__ import annotations

import math
from typing import Any, Literal

from icp_api.insights import templates_en, templates_id
from icp_api.insights.evidence import Finding, Loc, Parts, Value, Word

Lang = Literal["id", "en"]
TEMPLATES: dict[str, dict[str, str]] = {"id": templates_id.TEMPLATES, "en": templates_en.TEMPLATES}
MONTHS = {
    "id": [
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
    ],
    "en": [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ],
}
UNITS = {
    "usd": "USD/bbl",
    "usd_signed": "USD/bbl",
    "pct": "%",
    "pct_signed": "%",
    "idr": "IDR/bbl",
    "rate": "IDR/USD",
    "share": "%",
}


def _num(v: float, digits: int, lang: Lang) -> str:
    s = f"{v:,.{digits}f}"
    if lang == "id":
        s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return s


def fmt_period(p: str | None, lang: Lang) -> str:
    if not p:
        return "-"
    y, m = p.split("-")[:2]
    return f"{MONTHS[lang][int(m) - 1]} {y}"


def fmt_date(d: str | None, lang: Lang) -> str:
    if not d:
        return "-"
    y, m, day = d[:10].split("-")
    return f"{int(day)} {MONTHS[lang][int(m) - 1]} {y}"


def fmt_value(v: Value, lang: Lang) -> str:
    x = v.value
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "-"
    f = v.fmt
    if f == "text":
        return str(x)
    if f == "period":
        return fmt_period(str(x), lang)
    if f == "date":
        return fmt_date(str(x), lang)
    if isinstance(x, str):
        return x
    if f == "usd":
        return (
            f"USD {_num(abs(float(x)), 2, lang)}" if float(x) >= 0 else f"USD -{_num(abs(float(x)), 2, lang)}"
        )
    if f == "usd_signed":
        sign = "+" if float(x) > 0 else "-" if float(x) < 0 else ""
        return f"USD {sign}{_num(abs(float(x)), 2, lang)}"
    if f == "pct":
        return f"{_num(float(x), 1, lang)}%"
    if f == "pct_signed":
        sign = "+" if float(x) > 0 else ""
        return f"{sign}{_num(float(x), 1, lang)}%"
    if f in ("idr", "rate"):
        return ("Rp " if lang == "id" else "IDR ") + _num(float(x), 0, lang)
    if f == "share":
        return f"{_num(float(x) * 100, 0, lang)}%"
    if f == "int":
        return _num(float(x), 0, lang)
    return str(x)


def render_slot(value: Any, lang: Lang) -> str:
    if isinstance(value, Value):
        return fmt_value(value, lang)
    if isinstance(value, Word):
        return TEMPLATES[lang][f"word.{value.key}"]
    if isinstance(value, Loc):
        return value.id if lang == "id" else value.en
    if isinstance(value, Parts):
        return value.sep.join(render(key, slots, lang) for key, slots in value.items)
    if isinstance(value, (list, tuple)):
        return ", ".join(render_slot(v, lang) for v in value)
    if isinstance(value, int) and not isinstance(value, bool):
        return _num(float(value), 0, lang)
    return str(value)


def render(key: str, slots: dict[str, Any], lang: Lang) -> str:
    text = TEMPLATES[lang][key]
    return text.format(**{k: render_slot(v, lang) for k, v in slots.items()})


def has(key: str) -> bool:
    return key in TEMPLATES["id"]


def fact_dict(v: Value, lang: Lang) -> dict[str, Any]:
    label = render(f"fact.{v.key}", v.label_slots, lang)
    value = v.value
    if isinstance(value, float):
        value = round(value, 4)
    return {
        "key": v.key,
        "label": label,
        "value": value,
        "unit": v.unit or UNITS.get(v.fmt, ""),
        "display": fmt_value(v, lang),
    }


def compose(f: Finding, lang: Lang, dataset_labels: dict[str, dict[str, str]]) -> dict[str, Any]:
    """Finding -> API item (InsightItem v2) in one language."""
    title = render(f"{f.template}.title", f.slots, lang)
    body_key = f"{f.template}.body"
    sentences = [render(body_key, f.slots, lang)] if has(body_key) else []
    sentences += [render(k, {**f.slots, **s}, lang) for k, s in f.body_parts]
    next_key = f"{f.template}.next"
    facts = [fact_dict(v, lang) for v in f.facts]
    sev = {"high": "warning", "medium": "notice", "low": "info"}[f.importance]
    datasets = [
        {
            "dataset": d.dataset,
            "label": dataset_labels.get(d.dataset, {}).get(lang, d.dataset),
            "provider": dataset_labels.get(d.dataset, {}).get("provider", ""),
            "version_id": d.version_id,
            "period_end": d.period_end,
        }
        for d in f.datasets
    ]
    return {
        "id": f.id,
        "rule_id": f.rule_id,
        "rule_version": f.rule_version,
        "kind": f.kind,
        "importance": f.importance,
        "severity": sev,
        "audience": f.audience,
        "period": f.period,
        "title": title,
        "body": " ".join(s for s in sentences if s),
        "next_step": render(next_key, f.slots, lang) if has(next_key) else None,
        "facts": facts,
        "evidence": {
            "datasets": datasets,
            "periods": {
                "subject": f.window.get("subject", f.period),
                "window_start": f.window.get("window_start"),
                "window_end": f.window.get("window_end"),
                "n": f.window.get("n"),
            },
            "inputs": [fact_dict(v, lang) for v in f.inputs],
            "outputs": [fact_dict(v, lang) for v in f.outputs],
            "method": {"key": f.method, "text": TEMPLATES[lang][f"method.{f.method}"]},
            "model": f.model,
            "forecast_ids": f.forecast_ids,
        },
        "metrics": [{"label": x["label"], "value": x["value"], "unit": x["unit"]} for x in facts],
        "sources": datasets,
        "generated": True,
    }
