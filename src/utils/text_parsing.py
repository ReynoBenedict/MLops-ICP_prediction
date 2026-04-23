# text_parsing.py — Ekstraksi harga dan tanggal ICP dari teks PDF Kepmen

from __future__ import annotations

import re
from typing import Optional, Tuple

MONTH_MAP: dict[str, int] = {
    "januari": 1, "februari": 2, "maret": 3, "april": 4,
    "mei": 5, "juni": 6, "juli": 7, "agustus": 8,
    "september": 9, "oktober": 10, "november": 11, "desember": 12,
}

_MONTH_RE = "|".join(MONTH_MAP.keys())

# Pola: "ditetapkan sebesar US$ <harga>"
_PAT_ANCHOR = re.compile(
    r"(?:ditetapkan\s+sebesar|sebesar)\s+US\$\s*(?P<price>\d+[.,]\d+)",
    re.IGNORECASE,
)

# Pola: bulan + tahun dalam teks
_PAT_MONTH_YEAR = re.compile(
    r"(?:untuk\s+)?(?:bulan\s+)?(?P<month>" + _MONTH_RE + r")\s+(?P<year>20\d{2})",
    re.IGNORECASE,
)

# Pola luas: bulan + tahun → harga dalam 300 karakter
_PAT_BROAD = re.compile(
    r"(?P<month>" + _MONTH_RE + r")\s+(?P<year>20\d{2})"
    r"(?:.{0,300}?)"
    r"US\$\s*(?P<price>\d+[.,]\d+)",
    re.IGNORECASE | re.DOTALL,
)

# Variasi: "USD <harga>" (beberapa laporan terbaru)
_PAT_ICP_USD = re.compile(
    r"(?P<month>" + _MONTH_RE + r")\s+(?P<year>20\d{2})"
    r"(?:.{0,400}?)"
    r"(?:USD|US\$)\s*(?P<price>\d+[.,]\d+)",
    re.IGNORECASE | re.DOTALL,
)

# Pola: "<harga> US$/bbl"
_PAT_BBL = re.compile(
    r"(?P<month>" + _MONTH_RE + r")\s+(?P<year>20\d{2})"
    r"(?:.{0,400}?)"
    r"(?P<price>\d+[.,]\d+)\s*US\$/[Bb][Bb][Ll]",
    re.IGNORECASE | re.DOTALL,
)

# Pola tabular: harga diikuti "/bbl" atau sejenisnya
_PAT_TABULAR = re.compile(
    r"(?P<price>\d{2,3}[.,]\d{1,2})\s*(?:US\$)?/(?:bbl|barrel)",
    re.IGNORECASE,
)


def month_name_to_number(name: str) -> Optional[int]:
    return MONTH_MAP.get(name.lower().strip())


def _flatten(text: str) -> str:
    # Hapus line break agar pencocokan harga tidak terputus di tengah kalimat
    return re.sub(r"[ \t]*\n[ \t]*", " ", text)


def _to_price(raw: str) -> float:
    return float(raw.strip().replace(",", "."))


def _valid_price(p: float) -> bool:
    return 15.0 < p < 250.0


def parse_icp_price(text: str) -> Optional[Tuple[str, float]]:
    flat = _flatten(text)

    # Strategi 1: frasa anchor "ditetapkan sebesar US$ <harga>", cari bulan+tahun 400 karakter sebelumnya
    for anchor in _PAT_ANCHOR.finditer(flat):
        try:
            price = _to_price(anchor.group("price"))
        except ValueError:
            continue
        if not _valid_price(price):
            continue
        window = flat[max(0, anchor.start() - 400):anchor.start()]
        best = None
        for my in _PAT_MONTH_YEAR.finditer(window):
            best = my
        if best:
            month_num = month_name_to_number(best.group("month"))
            if month_num:
                return f"{best.group('year')}-{month_num:02d}", price

    # Strategi 2: scan luas — bulan + tahun diikuti harga US$
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

    # Strategi 3: variasi "USD <harga>"
    for m in _PAT_ICP_USD.finditer(flat):
        month_num = month_name_to_number(m.group("month"))
        if not month_num:
            continue
        try:
            price = _to_price(m.group("price"))
        except ValueError:
            continue
        if _valid_price(price):
            return f"{m.group('year')}-{month_num:02d}", price

    # Strategi 4: "<harga> US$/bbl"
    for m in _PAT_BBL.finditer(flat):
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
