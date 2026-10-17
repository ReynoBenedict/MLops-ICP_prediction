"""Official Indonesian Crude Price (ICP) from the Ministry of Energy (ESDM, Ditjen Migas).

Two official sources are combined, and a monthly value is only *verified* when they agree
(or when a single machine-readable decree text states it unambiguously):

1. **Ministerial decrees (Kepmen) archive** - https://www.migas.esdm.go.id/post/harga-minyak-mentah
   A table (columns = years, rows = months) linking one PDF per month. Each decree contains the
   clause ``"Harga rata-rata Minyak Mentah Indonesia untuk bulan <Bulan> <Tahun> ditetapkan
   sebesar US$ <value>/barrel"``. Newer PDFs have a text layer; 2019-2021 files are scans and need OCR.
2. **Monthly announcements** - news posts on the same site, e.g.
   ``/post/icp-januari-2022-terkerek-jadi-us-85-89-per-barel``, which state the month's value, the
   decree number, the previous month's value, and the Dated Brent / WTI / Brent ICE / OPEC basket
   averages ESDM used for context.

Definitions: the ICP is the official monthly average price (USD per barrel) determined by
ministerial decree for Indonesian crude (the "Harga rata-rata Minyak Mentah Indonesia" value in
the operative clause). Individual crude-grade prices in the decree annex, provisional mid-month
figures, and benchmark prices quoted in announcements are *not* the ICP and are never used as it.

Terms: ESDM publishes these as public information (Informasi Publik). ``robots.txt`` allows
crawling; requests are throttled and cached. Source documents are kept by checksum.
"""

from __future__ import annotations

import html
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from icp_ml.sources.base import HttpClient, SourceDocument, SourceError, ValueEvidence, utcnow_iso

logger = logging.getLogger(__name__)

ARCHIVE_URL = "https://www.migas.esdm.go.id/post/harga-minyak-mentah"
NEWS_INDEX_URL = "https://www.migas.esdm.go.id/post/index?kategori=berita"
BASE_URL = "https://www.migas.esdm.go.id"

MONTHS = {
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
MONTH_ALIASES = {
    **MONTHS,
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "jun": 6,
    "jul": 7,
    "ags": 8,
    "agt": 8,
    "aug": 8,
    "sep": 9,
    "sept": 9,
    "okt": 10,
    "okto": 10,
    "oct": 10,
    "nov": 11,
    "des": 12,
    "dec": 12,
}
_MONTH_RE = "|".join(sorted(MONTH_ALIASES, key=len, reverse=True))

# Decree operative clause. OCR often renders "US$" as "US $", "USS", "US5" or "US8".
_DECREE_ANCHOR = re.compile(
    r"untuk\s+bulan\s+(?P<m>"
    + _MONTH_RE
    + r")\s+(?P<y>20\d\d)\s+ditetapkan\s+sebesar\s+U\s?[S5]\s?[\$S85]?\s*(?P<v>\d{1,3}\s?[.,]\s?\d{2})",
    re.I,
)
_DECREE_NUMBER = re.compile(r"NOMOR\s*:?\s*(?P<n>\d{1,4}\s?\.?\s?K\s?/[A-Z0-9./ ]{3,40}?/20\d\d)", re.I)
# Announcement sentences.
_ARTICLE_MAIN = re.compile(
    r"(?:ICP|minyak\s+mentah\s+Indonesia)[^.]{0,80}?bulan\s+(?P<m>"
    + _MONTH_RE
    + r")\s+(?P<y>20\d\d)[^.]{0,60}?"
    r"(?:sebesar|mencapai|menjadi|di\s+level|pada\s+level|ke\s+angka)\s+US\$\s?(?P<v>\d{1,3}[.,]\d{2})",
    re.I,
)
_ARTICLE_PREV = re.compile(
    r"(?:dibandingkan|dibanding|dari)[^.]{0,40}?(?:bulan\s+)?(?P<m>"
    + _MONTH_RE
    + r")\s+(?P<y>20\d\d)[^.]{0,60}?US\$\s?(?P<v>\d{1,3}[.,]\d{2})",
    re.I,
)
_ARTICLE_TITLE_VALUE = re.compile(r"US-?\$?\s?(?P<v>\d{1,3}[-.,]\d{2})\s*(?:per|-per)?-?Barel", re.I)
_SLUG_MONTH = re.compile(r"icp-(?P<m>" + _MONTH_RE + r")-(?P<y>20\d\d)", re.I)


def _to_float(raw: str) -> float:
    return float(re.sub(r"\s", "", raw).replace(",", "."))


def _period(month_token: str, year: str) -> str | None:
    m = MONTH_ALIASES.get(month_token.lower())
    return f"{int(year)}-{m:02d}" if m else None


@dataclass
class ArchiveEntry:
    period: str
    url: str
    year: int
    month: int
    link_text: str


@dataclass
class DecreeExtraction:
    period: str | None
    value: float | None
    method: str  # text-layer | ocr | none
    decree_number: str | None
    chars: int
    note: str | None = None


@dataclass
class ArticleExtraction:
    url: str
    published: str | None
    period: str | None
    value: float | None
    previous_period: str | None
    previous_value: float | None
    decree_number: str | None
    benchmarks: dict[str, float] = field(default_factory=dict)
    title: str | None = None


# ---------------------------------------------------------------------------------------------
# Archive table
# ---------------------------------------------------------------------------------------------
def parse_archive_table(page_html: str) -> list[ArchiveEntry]:
    m = re.search(r"<table.*?</table>", page_html, re.S)
    if not m:
        raise SourceError("ICP archive page has no table")
    rows = re.findall(r"<tr.*?</tr>", m.group(0), re.S)
    header = [
        re.sub(r"<[^>]+>", "", c).strip() for c in re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", rows[0], re.S)
    ]
    entries: list[ArchiveEntry] = []
    for row in rows[1:]:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.S)
        for ci, cell in enumerate(cells):
            year = header[ci] if ci < len(header) else ""
            if not year.isdigit():
                continue
            for href, text in re.findall(r'href="([^"]+)"[^>]*>(.*?)</a>', cell, re.S | re.I):
                label = re.sub(r"<[^>]+>", "", text).strip().lower()
                month = MONTH_ALIASES.get(label)
                if href.lower().endswith(".pdf") and month:
                    url = html.unescape(href)
                    if url.startswith("/"):
                        url = BASE_URL + url
                    entries.append(ArchiveEntry(f"{int(year)}-{month:02d}", url, int(year), month, label))
    return entries


def fetch_archive(client: HttpClient) -> tuple[list[ArchiveEntry], SourceDocument]:
    body, doc = client.get(ARCHIVE_URL, max_age_s=6 * 3600)
    return parse_archive_table(body.decode("utf-8", "replace")), doc


# ---------------------------------------------------------------------------------------------
# Decree PDFs
# ---------------------------------------------------------------------------------------------
def extract_from_text(text: str) -> tuple[str | None, float | None, str | None]:
    flat = re.sub(r"\s+", " ", text)
    number = None
    nm = _DECREE_NUMBER.search(flat)
    if nm:
        number = re.sub(r"\s+", "", nm.group("n"))
    for m in _DECREE_ANCHOR.finditer(flat):
        period = _period(m.group("m"), m.group("y"))
        value = _to_float(m.group("v"))
        if period and 5 < value < 300:
            return period, value, number
    return None, None, number


def pdf_text_layer(pdf_bytes: bytes, max_pages: int = 4) -> str:
    import fitz  # PyMuPDF

    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        return "\n".join(doc[i].get_text() for i in range(min(len(doc), max_pages)))


# OCR passes tried in order until the operative clause is found (scan quality varies a lot).
OCR_PASSES: tuple[tuple[int, int, bool], ...] = (
    (220, 6, False),
    (300, 6, False),
    (300, 4, True),
    (260, 3, False),
    (340, 6, True),
)


def pdf_ocr(
    pdf_bytes: bytes, max_pages: int = 6, dpi: int = 220, psm: int = 6, binarize: bool = False
) -> str:
    """OCR the first ``max_pages`` pages. Decrees are 4-5 pages and the operative clause
    ("KEDUA: Harga rata-rata ... ditetapkan sebesar US$ ...") is usually on page 3 or 4."""
    import fitz
    import pytesseract
    from PIL import Image, ImageOps

    cmd = Path(os.getenv("TESSERACT_CMD", r"C:\Program Files\Tesseract-OCR\tesseract.exe"))
    if cmd.exists():
        pytesseract.pytesseract.tesseract_cmd = str(cmd)
    parts = []
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        for i in range(min(len(doc), max_pages)):
            pix = doc[i].get_pixmap(dpi=dpi, colorspace=fitz.csGRAY)
            img = Image.frombytes("L", (pix.width, pix.height), pix.samples)
            if binarize:
                img = ImageOps.autocontrast(img).point(lambda v: 255 if v > 160 else 0)
            parts.append(pytesseract.image_to_string(img, lang="eng", config=f"--psm {psm}"))
    return "\n".join(parts)


def extract_decree(pdf_bytes: bytes, allow_ocr: bool = True) -> DecreeExtraction:
    try:
        text = pdf_text_layer(pdf_bytes)
    except Exception as exc:  # noqa: BLE001 - corrupt PDF -> reported, not raised
        return DecreeExtraction(None, None, "none", None, 0, f"unreadable PDF: {type(exc).__name__}")
    if len(text.strip()) > 200:
        period, value, number = extract_from_text(text)
        if value is not None:
            return DecreeExtraction(period, value, "text-layer", number, len(text))
    if not allow_ocr:
        return DecreeExtraction(None, None, "none", None, len(text), "no text layer")
    number = None
    chars = len(text)
    for dpi, psm, binarize in OCR_PASSES:
        try:
            ocr = pdf_ocr(pdf_bytes, dpi=dpi, psm=psm, binarize=binarize)
        except Exception as exc:  # noqa: BLE001
            return DecreeExtraction(None, None, "none", None, chars, f"OCR unavailable: {type(exc).__name__}")
        period, value, num = extract_from_text(ocr)
        number = number or num
        chars = max(chars, len(ocr))
        if value is not None:
            return DecreeExtraction(
                period, value, "ocr", number, chars, f"ocr dpi={dpi} psm={psm}{' bin' if binarize else ''}"
            )
    return DecreeExtraction(None, None, "none", number, chars, "operative clause not found in OCR text")


# ---------------------------------------------------------------------------------------------
# Announcements
# ---------------------------------------------------------------------------------------------
# Announcements phrase benchmarks as "<name> naik sebesar US$X ... dari US$Y menjadi US$Z"; the level is Z.
_LEVEL = r"[^.]{0,160}?(?:menjadi|ke\s+level|pada\s+level|mencapai)\s+US\$\s?(\d{1,3}[.,]\d{2})"
_BENCHMARK_PATTERNS = {
    "dated_brent": r"Dated\s+Brent" + _LEVEL,
    "wti_nymex": r"WTI\s*\(?Nymex\)?" + _LEVEL,
    "brent_ice": r"Brent\s*\(?ICE\)?" + _LEVEL,
    "opec_basket": r"(?:Basket\s+OPEC|OPEC\s+Basket)" + _LEVEL,
}


def article_text(page_html: str) -> tuple[str, str | None, str | None]:
    t = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", page_html, flags=re.S | re.I)
    title = re.search(r"<title>(.*?)</title>", t, re.S)
    body = html.unescape(re.sub(r"<[^>]+>", "\n", t))
    body = re.sub(r"[ \t]+", " ", body)
    date = re.search(r"\b(\d{1,2}\s+(?:" + "|".join(MONTHS) + r")\s+20\d\d)\b", body, re.I)
    return body, (html.unescape(title.group(1)).strip() if title else None), (date.group(1) if date else None)


# A price *level* is "US$ N per barel" (or "menjadi/sebesar US$ N" in a title); a *change* is
# "naik/turun (sebesar/tipis/...) US$ N per barel". Changes are recognised by the words before them.
_LEVEL_RE = re.compile(r"(?:US\s?\$?D?\s?)?(?P<v>\d{1,3}[.,]\d{2})\s*(?:/|per)\s*(?:barr?el|bbl)", re.I)
_TITLE_LEVEL = re.compile(
    r"(?:menjadi|jadi|sebesar|ke\s+angka|ke\s+level|di\s+level|tembus|mencapai|capai|tetapkan|dipatok|patok)"
    r"\s+(?:ICP\s+)?(?:US\s?\$?D?\s?)?(?P<v>\d{1,3}[.,]\d{2})(?!\d)",
    re.I,
)
_CHANGE_WORDS = (
    r"naik|turun|meningkat|menurun|melemah|menguat|kenaikan|penurunan|terkoreksi|melonjak|anjlok|merosot|bertambah|"
    r"berkurang|lebih\s+tinggi|lebih\s+rendah|selisih|perubahan|koreksi|terkerek|terdongkrak|menyusut|melesat|meroket|turunnya|naiknya"
)
# "naik sebesar US$ 2,69" is a change; "naik menjadi US$ 80,09" / "turun ke angka US$ 68,59" is a level.
_CHANGE_BEFORE = re.compile(
    r"(?:" + _CHANGE_WORDS + r")(?:\s+(?!menjadi\b|jadi\b|ke\b|di\b|mencapai\b|tembus\b)[\w()]+){0,2}$", re.I
)
_PREV_BEFORE = re.compile(r"(?:\bdari|dibanding\w*|sebelumnya|\bpada)(?:\s+[\w()]+){0,7}$", re.I)
_TITLE_PERIOD = re.compile(
    r"ICP\)?\s+(?:bulan\s+|minyak\s+mentah\s+(?:indonesia\s+)?(?:bulan\s+)?)?(?P<m>"
    + _MONTH_RE
    + r")(?:\s+(?P<y>20\d\d))?",
    re.I,
)
_MONTH_NAME = {n: name for name, n in MONTHS.items()}


def _levels(text: str, context: re.Pattern[str] | None = None) -> list[float]:
    """Price levels in ``text`` excluding month-on-month changes; ``context`` must match the words before."""
    out = []
    for m in _LEVEL_RE.finditer(text):
        before = text[max(0, m.start() - 45) : m.start()].strip()
        if _CHANGE_BEFORE.search(before):
            continue
        if context is not None and not context.search(before):
            continue
        out.append(_to_float(m.group("v")))
    return out


def _title_levels(title: str) -> list[float]:
    levels = _levels(title)
    if levels:
        return levels
    return [
        _to_float(m.group("v"))
        for m in _TITLE_LEVEL.finditer(title)
        if not _CHANGE_BEFORE.search(title[max(0, m.start() - 45) : m.start()].strip())
    ]


def _month_number(token: str) -> int | None:
    p = _period(token, "2000")
    return int(p[5:]) if p else None


def parse_article(url: str, page_html: str) -> ArticleExtraction:
    """Read the month, the official level, and the previous month's level from an ESDM announcement.

    Levels are only taken from "US$ N per barel" phrases that are not month-on-month changes
    (titles such as "Naik US$ 2,69, ... ICP Juli 2024 sebesar US$ 82,00 per Barel" contain both).
    """
    body, title, published = article_text(page_html)
    flat = re.sub(r"\s+", " ", body)
    title_text = re.sub(r"\s+", " ", title or "")
    slug = url.rsplit("/", 1)[-1].replace("-", " ")
    period = value = prev_period = prev_value = None
    parts = published.split() if published else []
    pub_year = int(parts[2]) if len(parts) == 3 and parts[2].isdigit() else None
    pub_month = _month_number(parts[1]) if len(parts) == 3 else None
    # 1. period: title or slug ("ICP <Month> [<Year>]"); the year falls back to the publication date.
    for text in (title_text, slug):
        tm = _TITLE_PERIOD.search(text)
        if tm:
            month = _month_number(tm.group("m"))
            year = int(tm.group("y")) if tm.group("y") else None
            if year is None and pub_year and month:
                year = pub_year - 1 if pub_month and month > pub_month else pub_year
            if month and year:
                period = f"{year}-{month:02d}"
                break
    # 2. level: title first (one clear level), else the body sentence naming the month.
    levels = _title_levels(title_text) or _title_levels(slug)
    if not levels and period:
        month_name = _MONTH_NAME[int(period[5:])]
        for sentence in re.split(r"(?<=[.!?])\s+", flat):
            if re.search(month_name, sentence, re.I) and re.search(
                r"ICP|minyak mentah Indonesia", sentence, re.I
            ):
                levels = _levels(sentence)
                if levels:
                    break
    if levels:
        value = levels[0]
    # 3. previous month: a sentence mentioning the previous month's name (or "bulan sebelumnya").
    if period:
        prev = pd.Period(period, freq="M") - 1
        prev_name = _MONTH_NAME[prev.month]
        prev_re = prev_name + r"\s+" + str(prev.year) + r"|bulan sebelumnya|sebelumnya"
        icp_sentences = [
            sent
            for sent in re.split(r"(?<=[.!?])\s+", flat)
            if re.search(r"ICP|minyak mentah Indonesia", sent, re.I) and re.search(prev_re, sent, re.I)
        ]
        # Prefer the sentence that also states this month's level ("dari US$ X/bbl menjadi US$ Y/bbl").
        icp_sentences.sort(key=lambda sent: 0 if value is not None and value in _levels(sent) else 1)
        for sentence in icp_sentences:
            # Plausibility: a previous-month level is never a small change-like figure relative to this month.
            cands = [
                v
                for v in _levels(sentence, _PREV_BEFORE)
                if value is None or (abs(v - value) > 0.005 and 0.4 * value <= v <= 2.5 * value)
            ]
            if cands:
                prev_period, prev_value = str(prev), cands[0]
                break
    nm = re.search(r"Nomor\s+(\d{1,4}\.?K/[A-Z0-9./]+/20\d\d)", flat)
    benchmarks = {}
    for key, pat in _BENCHMARK_PATTERNS.items():
        bm = re.search(pat, flat, re.I)
        if bm:
            benchmarks[key] = _to_float(bm.group(1))
    return ArticleExtraction(
        url, published, period, value, prev_period, prev_value, nm.group(1) if nm else None, benchmarks, title
    )


def list_article_links(index_html: str) -> list[str]:
    links = set(re.findall(r'href="(/post/[^"?#]+)"', index_html))
    return sorted(
        BASE_URL + href
        for href in links
        if re.search(r"ICP|Minyak-Mentah-Indonesia", href, re.I) and "harga-minyak-mentah" not in href
    )


# ---------------------------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------------------------
@dataclass
class ReconciledMonth:
    period: str
    value: float | None
    status: str  # verified | single_source | conflict | missing
    sources: list[dict[str, Any]]
    note: str = ""


STRONG_KINDS = {"decree_text", "decree_ocr", "announcement"}
WEAK_KINDS = {"announcement_backref", "legacy_csv"}


def reconcile(
    period_values: dict[str, list[dict[str, Any]]], tolerance: float = 0.005
) -> list[ReconciledMonth]:
    """Combine per-source observations for each month into one status.

    Strong observations: the decree text layer, decree OCR, the month's own announcement. Weak
    observations: the next month's announcement back-reference and the repository's legacy CSV.
    A weak observation can corroborate a single strong one (-> verified) but never creates a
    conflict and is never sufficient on its own.

    * verified: >= 2 observations agree (at least one strong), or a text-layer decree alone;
    * single_source: one strong OCR/announcement observation without corroboration (or weak only);
    * conflict: strong observations disagree beyond ``tolerance`` -> value is None until resolved;
    * missing: no observation.
    """
    out = []
    for period in sorted(period_values):
        all_obs = [o for o in period_values[period] if o.get("value") is not None]
        strong = [o for o in all_obs if o["kind"] in STRONG_KINDS]
        weak = [o for o in all_obs if o["kind"] in WEAK_KINDS]
        if not strong:
            if weak:
                weak_kinds = ", ".join(sorted({o["kind"] for o in weak}))
                out.append(
                    ReconciledMonth(period, weak[0]["value"], "single_source", all_obs, f"only {weak_kinds}")
                )
            else:
                out.append(ReconciledMonth(period, None, "missing", period_values[period]))
            continue
        values = sorted({round(o["value"], 2) for o in strong})
        dissent_note = ""
        if len(values) > 1 and (values[-1] - values[0]) > tolerance:
            # Majority rule: when two or more strong observations agree and exactly one dissents, the
            # agreeing value stands and the dissent is recorded (an announcement occasionally quotes a
            # different figure, e.g. a benchmark, than the decree it reports on).
            groups: dict[float, list[dict[str, Any]]] = {}
            for o in strong + [w for w in weak if w["kind"] == "announcement_backref"]:
                key = next(
                    (k for k in groups if abs(k - round(o["value"], 2)) <= tolerance), round(o["value"], 2)
                )
                groups.setdefault(key, []).append(o)
            ranked = sorted(groups.items(), key=lambda kv: -len(kv[1]))
            winner = ranked[0][1]
            losers = [o for _, grp in ranked[1:] for o in grp]
            winner_strong = [o for o in winner if o["kind"] in STRONG_KINDS]
            decisive = (
                len(winner) >= 2
                and winner_strong
                and (len(winner_strong) >= 2 or any(o["kind"] == "decree_text" for o in winner_strong))
            )
            if decisive and len([o for o in losers if o["kind"] in STRONG_KINDS]) == 1:
                dissenter = next(o for o in losers if o["kind"] in STRONG_KINDS)
                dissent_note = f"{dissenter['kind']} differs ({dissenter['value']}) - outvoted by {len(winner)} agreeing sources"
                strong = winner_strong
                weak = [w for w in weak if w not in losers]
                values = [ranked[0][0]]
            else:
                out.append(ReconciledMonth(period, None, "conflict", all_obs, f"disagreeing values {values}"))
                continue
        kinds = {o["kind"] for o in strong}
        value = values[0]
        agreeing = [o for o in weak if abs(round(o["value"], 2) - value) <= tolerance]
        disagreeing = [o for o in weak if abs(round(o["value"], 2) - value) > tolerance]
        notes = ([dissent_note] if dissent_note else []) + [
            f"{o['kind']} differs ({o['value']})" for o in disagreeing
        ]
        legacy_diff = [o for o in disagreeing if o["kind"] == "legacy_csv"]
        if legacy_diff:
            notes.append(f"corrects legacy value {legacy_diff[0]['value']}")
        note = "; ".join(notes)
        if len(strong) >= 2 or "decree_text" in kinds or agreeing:
            out.append(ReconciledMonth(period, value, "verified", all_obs, note))
        else:
            detail = f"only {', '.join(sorted(kinds))}" + (f"; {note}" if note else "")
            out.append(ReconciledMonth(period, value, "single_source", all_obs, detail))
    return out


def evidence_from(month: ReconciledMonth, retrieved_at: str | None = None) -> list[ValueEvidence]:
    ts = retrieved_at or utcnow_iso()
    return [
        ValueEvidence(
            month.period,
            float(o["value"]),
            o["url"],
            o.get("sha256"),
            o["kind"],
            month.status,
            ts,
            reference=o.get("reference"),
            notes=o.get("note"),
        )
        for o in month.sources
        if o.get("value") is not None
    ]


def to_frame(months: list[ReconciledMonth]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "period": m.period,
                "value": m.value,
                "status": m.status,
                "n_sources": len(m.sources),
                "note": m.note,
            }
            for m in months
        ]
    )


__all__ = [
    "ARCHIVE_URL",
    "NEWS_INDEX_URL",
    "ArchiveEntry",
    "ArticleExtraction",
    "DecreeExtraction",
    "ReconciledMonth",
    "evidence_from",
    "extract_decree",
    "extract_from_text",
    "fetch_archive",
    "list_article_links",
    "parse_archive_table",
    "parse_article",
    "reconcile",
    "to_frame",
]
