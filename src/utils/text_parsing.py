# text_parsing.py — extract ICP price and date from Kepmen PDF text

from __future__ import annotations

import re
from typing import Optional, Tuple

MONTH_MAP: dict[str, int] = {
    "januari": 1, "februari": 2, "maret": 3, "april": 4,
    "mei": 5, "juni": 6, "juli": 7, "agustus": 8,
    "september": 9, "oktober": 10, "november": 11, "desember": 12,
}

_MONTH_RE = "|".join(MONTH_MAP.keys())

# Anchor: "ditetapkan sebesar US$ <price>"
_PAT_ANCHOR = re.compile(
    r"(?:ditetapkan\s+sebesar|sebesar)\s+US\$\s*(?P<price>\d+[.,]\d+)",
    re.IGNORECASE,
)

# Month + year lookup for the look-back window
_PAT_MONTH_YEAR = re.compile(
    r"(?:untuk\s+)?(?:bulan\s+)?(?P<month>" + _MONTH_RE + r")\s+(?P<year>20\d{2})",
    re.IGNORECASE,
)

# Fallback broad scan: month + year + up to 300 chars + US$ price
_PAT_BROAD = re.compile(
    r"(?P<month>" + _MONTH_RE + r")\s+(?P<year>20\d{2})"
    r"(?:.{0,300}?)"
    r"US\$\s*(?P<price>\d+[.,]\d+)",
    re.IGNORECASE | re.DOTALL,
)


def month_name_to_number(name: str) -> Optional[int]:
    return MONTH_MAP.get(name.lower().strip())


def _flatten(text: str) -> str:
    # Collapse line breaks so mid-sentence splits don't break matching
    return re.sub(r"[ \t]*\n[ \t]*", " ", text)


def _to_price(raw: str) -> float:
    return float(raw.strip().replace(",", "."))


def _valid_price(p: float) -> bool:
    return 20.0 < p < 200.0


def parse_icp_price(text: str) -> Optional[Tuple[str, float]]:
    flat = _flatten(text)

    # Strategy 1: find price anchor, look back for month+year
    for anchor in _PAT_ANCHOR.finditer(flat):
        try:
            price = _to_price(anchor.group("price"))
        except ValueError:
            continue
        if not _valid_price(price):
            continue
        window = flat[max(0, anchor.start() - 300):anchor.start()]
        best = None
        for my in _PAT_MONTH_YEAR.finditer(window):
            best = my
        if best:
            month_num = month_name_to_number(best.group("month"))
            if month_num:
                return f"{best.group('year')}-{month_num:02d}", price

    # Strategy 2: broad scan
    for m in _PAT_BROAD.finditer(flat):
        month_num = month_name_to_number(m.group("month"))
        if not month_num:
            continue
        try:
            price = _to_price(m.group("price"))
        except ValueError:
            continue
        if _valid_price(price):
            return f"{m.group('year')}-{month_num:02d}", price

    return None


def parse_date_from_filename(filename: str) -> Optional[str]:
    m = re.search(r"icp_(\d{4})_(\d{2})", filename, re.IGNORECASE)
    return f"{m.group(1)}-{m.group(2)}" if m else None
