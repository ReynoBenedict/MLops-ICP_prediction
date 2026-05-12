# text_parsing.py — Ekstraksi harga dan tanggal ICP dari teks PDF Kepmen

from __future__ import annotations

import logging
import re
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

MONTH_MAP: dict[str, int] = {
    "januari": 1, "februari": 2, "maret": 3, "april": 4,
    "mei": 5, "juni": 6, "juli": 7, "agustus": 8,
    "september": 9, "oktober": 10, "november": 11, "desember": 12,
}

_MONTH_RE = "|".join(MONTH_MAP.keys())

# Strategi 1: frasa anchor "ditetapkan sebesar US$ <harga>"
_PAT_ANCHOR = re.compile(
    r"(?:ditetapkan\s+sebesar|sebesar)\s+US\$\s*(?P<price>\d+[.,]\d+)",
    re.IGNORECASE,
)

# Pola: bulan + tahun dalam teks
_PAT_MONTH_YEAR = re.compile(
    r"(?:untuk\s+)?(?:bulan\s+)?(?P<month>" + _MONTH_RE + r")\s+(?P<year>20\d{2})",
    re.IGNORECASE,
)

# Strategi 2: bulan + tahun -> harga US$ dalam 300 karakter
_PAT_BROAD = re.compile(
    r"(?P<month>" + _MONTH_RE + r")\s+(?P<year>20\d{2})"
    r"(?:.{0,300}?)"
    r"US\$\s*(?P<price>\d+[.,]\d+)",
    re.IGNORECASE | re.DOTALL,
)

# Strategi 3: variasi "USD <harga>"
_PAT_ICP_USD = re.compile(
    r"(?P<month>" + _MONTH_RE + r")\s+(?P<year>20\d{2})"
    r"(?:.{0,400}?)"
    r"(?:USD|US\$)\s*(?P<price>\d+[.,]\d+)",
    re.IGNORECASE | re.DOTALL,
)

# Strategi 4: "<harga> US$/bbl"
_PAT_BBL = re.compile(
    r"(?P<month>" + _MONTH_RE + r")\s+(?P<year>20\d{2})"
    r"(?:.{0,400}?)"
    r"(?P<price>\d+[.,]\d+)\s*US\$/[Bb][Bb][Ll]",
    re.IGNORECASE | re.DOTALL,
)

# Strategi 5 (FALLBACK): "RATA-RATA" atau "RATA - RATA" diikuti harga tanpa prefix US$
# Menangani format tabel lama dimana harga ICP rata-rata muncul di baris RATA-RATA
_PAT_RATA = re.compile(
    r"RATA[-\s]*RATA\s*[|]?\s*(?P<price>\d{2,3}[.,]\d{1,2})\b",
    re.IGNORECASE,
)

# Strategi 6 (FALLBACK LUAS): angka standalone dalam konteks dokumen ICP
# Hanya berlaku jika dokumen terbukti laporan ICP (ada header + bulan/tahun)
# Pola: angka 2-3 digit dengan desimal, tidak diikuti "/bbl" atau "%" (hindari formula)
_PAT_BARE_NUMBER = re.compile(
    r"(?<![/\w])(\d{2,3}[.,]\d{2})(?!\s*(?:/[Bb][Bb][Ll]|%|\s*x\s))",
)

# Pola tabular (tidak dipakai di alur utama, tersedia untuk referensi)
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
    # Harga ICP yang masuk akal secara ekonomi: 15-250 USD/bbl
    return 15.0 < p < 250.0


def _extract_month_year(flat: str) -> Optional[tuple[str, int]]:
    """
    Cari pasangan (year_str, month_num) pertama yang valid dalam teks.
    Digunakan oleh fallback untuk menentukan konteks bulan/tahun.
    """
    best = None
    for my in _PAT_MONTH_YEAR.finditer(flat):
        best = my
    if best:
        month_num = month_name_to_number(best.group("month"))
        if month_num:
            return best.group("year"), month_num
    return None


def parse_icp_price(text: str) -> Optional[Tuple[str, float]]:
    """
    Ekstrak (date_str, price) dari teks PDF laporan ICP ESDM.

    Mencoba 6 strategi secara berurutan:
    1. Anchor "ditetapkan sebesar US$ <harga>"
    2. Scan luas bulan+tahun -> US$ <harga>
    3. Variasi "USD <harga>"
    4. Format "<harga> US$/bbl"
    5. [FALLBACK] Label RATA-RATA diikuti harga tanpa prefix
    6. [FALLBACK LUAS] Angka standalone dalam dokumen ICP yang teridentifikasi

    Log tags: [PARSE] [FALLBACK] [SUCCESS] [WARNING]
    """
    flat = _flatten(text)
    source = repr(text[:60].strip())

    logger.debug("[PARSE] Mulai parsing. Panjang teks: %d karakter.", len(text))

    # ── Strategi 1: frasa anchor ──────────────────────────────────────────
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
                date_str = f"{best.group('year')}-{month_num:02d}"
                logger.debug("[SUCCESS] Strategi 1 (anchor): %s -> %.2f", date_str, price)
                return date_str, price

    # ── Strategi 2: scan luas US$ ─────────────────────────────────────────
    for m in _PAT_BROAD.finditer(flat):
        month_num = month_name_to_number(m.group("month"))
        if not month_num:
            continue
        try:
            price = _to_price(m.group("price"))
        except ValueError:
            continue
        if _valid_price(price):
            date_str = f"{m.group('year')}-{month_num:02d}"
            logger.debug("[SUCCESS] Strategi 2 (broad US$): %s -> %.2f", date_str, price)
            return date_str, price

    # ── Strategi 3: variasi USD ───────────────────────────────────────────
    for m in _PAT_ICP_USD.finditer(flat):
        month_num = month_name_to_number(m.group("month"))
        if not month_num:
            continue
        try:
            price = _to_price(m.group("price"))
        except ValueError:
            continue
        if _valid_price(price):
            date_str = f"{m.group('year')}-{month_num:02d}"
            logger.debug("[SUCCESS] Strategi 3 (USD): %s -> %.2f", date_str, price)
            return date_str, price

    # ── Strategi 4: <harga> US$/bbl ──────────────────────────────────────
    for m in _PAT_BBL.finditer(flat):
        month_num = month_name_to_number(m.group("month"))
        if not month_num:
            continue
        try:
            price = _to_price(m.group("price"))
        except ValueError:
            continue
        if _valid_price(price):
            date_str = f"{m.group('year')}-{month_num:02d}"
            logger.debug("[SUCCESS] Strategi 4 (bbl): %s -> %.2f", date_str, price)
            return date_str, price

    # ── Strategi 5 [FALLBACK]: label RATA-RATA ────────────────────────────
    # Menangani tabel lama dimana harga muncul langsung setelah "RATA-RATA"
    # tanpa prefix "US$"
    logger.debug("[FALLBACK] Strategi 1-4 gagal. Mencoba RATA-RATA proximity...")
    for m in _PAT_RATA.finditer(flat):
        try:
            price = _to_price(m.group("price"))
        except ValueError:
            continue
        if not _valid_price(price):
            continue
        # Cari bulan/tahun dalam 800 karakter sebelum label RATA-RATA
        window = flat[max(0, m.start() - 800):m.start()]
        ctx = _extract_month_year(window)
        if ctx:
            year_str, month_num = ctx
            date_str = f"{year_str}-{month_num:02d}"
            logger.info("[FALLBACK] Strategi 5 (RATA-RATA): %s -> %.2f", date_str, price)
            return date_str, price

    # ── Strategi 6 [FALLBACK LUAS]: angka standalone dalam dokumen ICP ────
    # Hanya berlaku jika:
    # (a) dokumen mengandung header laporan ICP yang jelas
    # (b) ada bulan/tahun yang teridentifikasi
    # (c) ada angka standalone dalam rentang harga yang wajar
    # Ambil MEDIAN untuk menghindari outlier (bukan mean)
    is_icp_doc = bool(re.search(
        r"HARGA\s+MINYAK\s+MENTAH\s+INDONESIA\s+BULAN",
        flat, re.IGNORECASE,
    ))
    if is_icp_doc:
        logger.debug("[FALLBACK] Dokumen ICP teridentifikasi. Mencoba ekstraksi angka standalone...")
        ctx = _extract_month_year(flat)
        if ctx:
            year_str, month_num = ctx
            # Kumpulkan semua angka standalone valid; buang yang terlampir formula
            candidates: list[float] = []
            for num_m in _PAT_BARE_NUMBER.finditer(flat):
                raw = num_m.group(1)
                try:
                    val = _to_price(raw)
                except ValueError:
                    continue
                if _valid_price(val):
                    # Hanya ambil angka yang muncul di akhir baris / setelah spasi panjang
                    # (ciri khas kolom "HARGA" di tabel) — bukan di tengah formula
                    candidates.append(val)

            if candidates:
                candidates.sort()
                # Ambil median
                mid = len(candidates) // 2
                median_price = candidates[mid]
                date_str = f"{year_str}-{month_num:02d}"
                logger.info(
                    "[FALLBACK] Strategi 6 (median %d angka): %s -> %.2f",
                    len(candidates), date_str, median_price,
                )
                return date_str, median_price

    # ── Semua strategi gagal ──────────────────────────────────────────────
    logger.warning(
        "[WARNING] Semua strategi parsing gagal. Teks: %s...",
        source,
    )
    return None


def parse_date_from_filename(filename: str) -> Optional[str]:
    m = re.search(r"icp_(\d{4})_(\d{2})", filename, re.IGNORECASE)
    return f"{m.group(1)}-{m.group(2)}" if m else None
