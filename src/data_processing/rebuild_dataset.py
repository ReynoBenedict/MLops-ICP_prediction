# rebuild_dataset.py — Rebuild icp_dataset.csv from all PDFs in data/raw/ and data/raw/pdfs/

from __future__ import annotations

import csv
import logging
import re
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("rebuild_dataset")

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Scan both locations: direct data/raw/ files AND the data/raw/pdfs/ subfolder
RAW_DIRS = [
    PROJECT_ROOT / "data" / "raw",
    PROJECT_ROOT / "data" / "raw" / "pdfs",
]
OUTPUT_CSV = PROJECT_ROOT / "data" / "processed" / "icp_dataset.csv"

PRICE_MIN = 15.0
PRICE_MAX = 250.0

MONTH_MAP = {
    "januari": 1, "februari": 2, "maret": 3, "april": 4,
    "mei": 5, "juni": 6, "juli": 7, "agustus": 8,
    "september": 9, "oktober": 10, "november": 11, "desember": 12,
}
MONTH_RE = "|".join(MONTH_MAP.keys())

# Pattern: "ditetapkan sebesar US$ <price>"
_PAT_ANCHOR = re.compile(
    r"(?:ditetapkan\s+sebesar|sebesar)\s+US\$\s*(?P<price>\d+[.,]\d+)",
    re.IGNORECASE,
)
_PAT_MONTH_YEAR = re.compile(
    r"(?:untuk\s+)?(?:bulan\s+)?(?P<month>" + MONTH_RE + r")\s+(?P<year>20\d{2})",
    re.IGNORECASE,
)
# Broad: month + year → US$ price within 300 chars
_PAT_BROAD = re.compile(
    r"(?P<month>" + MONTH_RE + r")\s+(?P<year>20\d{2})"
    r"(?:.{0,300}?)"
    r"US\$\s*(?P<price>\d+[.,]\d+)",
    re.IGNORECASE | re.DOTALL,
)
# Alternative USD phrasing
_PAT_USD = re.compile(
    r"(?P<month>" + MONTH_RE + r")\s+(?P<year>20\d{2})"
    r"(?:.{0,400}?)"
    r"(?:USD|US\$)\s*(?P<price>\d+[.,]\d+)",
    re.IGNORECASE | re.DOTALL,
)
# "<price> US$/bbl" pattern
_PAT_BBL = re.compile(
    r"(?P<month>" + MONTH_RE + r")\s+(?P<year>20\d{2})"
    r"(?:.{0,400}?)"
    r"(?P<price>\d+[.,]\d+)\s*US\$/[Bb][Bb][Ll]",
    re.IGNORECASE | re.DOTALL,
)


def _flatten(text: str) -> str:
    return re.sub(r"[ \t]*\n[ \t]*", " ", text)


def _to_price(raw: str) -> float:
    return float(raw.replace(",", "."))


def _valid_price(p: float) -> bool:
    return PRICE_MIN < p < PRICE_MAX


def _words_to_text(words: list) -> str:
    # Mengembalikan objek word dari pdfplumber menjadi sekumpulan teks baris.
    if not words:
        return ""
    words_sorted = sorted(words, key=lambda w: (round(w["top"]), w["x0"]))
    lines: list[list[str]] = []
    current_top = None
    current_line: list[str] = []
    for w in words_sorted:
        top = round(w["top"])
        if current_top is None or abs(top - current_top) > 5:
            if current_line:
                lines.append(current_line)
            current_line = [w["text"]]
            current_top = top
        else:
            current_line.append(w["text"])
    if current_line:
        lines.append(current_line)
    return "\n".join(" ".join(line) for line in lines)


def _extract_text(pdf_path: Path) -> str:
    # Ekstrak semua teks dari PDF menggunakan pendekatan baris, 
    # dilanjutkan pendekatan karakter (jika karakter disematkan tapi bentuk rusak).
    try:
        import pdfplumber
    except ImportError:
        raise ImportError("Run: pip install pdfplumber")
    page_texts: list[str] = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                if not text.strip():
                    try:
                        words = page.extract_words(x_tolerance=3, y_tolerance=3)
                        text = _words_to_text(words)
                    except Exception:
                        text = ""
                page_texts.append(text)
    except Exception as exc:
        logger.warning("pdfplumber failed on '%s': %s", pdf_path.name, exc)
        return ""
    return "\n".join(page_texts)


def _parse_price(text: str):
    # Mengembalikan (date_str, price) atau kembalikan None jika gagals.
    flat = _flatten(text)

    # Strategy 1: anchor phrase, look back for month+year
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
            month_num = MONTH_MAP.get(best.group("month").lower())
            if month_num:
                return f"{best.group('year')}-{month_num:02d}", price

    # Strategy 2: broad match — month+year → "US$ price" within 300 chars
    for m in _PAT_BROAD.finditer(flat):
        month_num = MONTH_MAP.get(m.group("month").lower())
        if not month_num:
            continue
        try:
            price = _to_price(m.group("price"))
        except ValueError:
            continue
        if _valid_price(price):
            return f"{m.group('year')}-{month_num:02d}", price

    # Strategy 3: "USD <price>" variant
    for m in _PAT_USD.finditer(flat):
        month_num = MONTH_MAP.get(m.group("month").lower())
        if not month_num:
            continue
        try:
            price = _to_price(m.group("price"))
        except ValueError:
            continue
        if _valid_price(price):
            return f"{m.group('year')}-{month_num:02d}", price

    # Strategy 4: "<price> US$/bbl"
    for m in _PAT_BBL.finditer(flat):
        month_num = MONTH_MAP.get(m.group("month").lower())
        if not month_num:
            continue
        try:
            price = _to_price(m.group("price"))
        except ValueError:
            continue
        if _valid_price(price):
            return f"{m.group('year')}-{month_num:02d}", price

    return None


def _collect_pdf_files() -> list[Path]:
    # Mengambil list dokumen PDF dari daftar folder mentahan.
    seen: dict[str, Path] = {}
    for raw_dir in RAW_DIRS:
        if not raw_dir.exists():
            continue
        for pdf in sorted(raw_dir.glob("*.pdf")):
            # Later directories (data/raw/pdfs/) win on name collision
            seen[pdf.name] = pdf
    return sorted(seen.values(), key=lambda p: p.name)


def rebuild_dataset():
    # Menghidupkan ulang isi dataset CSV lewat file PDF di direktori raw.
    pdf_files = _collect_pdf_files()
    if not pdf_files:
        logger.error("No PDFs found in %s", [str(d) for d in RAW_DIRS])
        sys.exit(1)

    logger.info("Found %d PDFs — extracting with pdfplumber (no OCR)", len(pdf_files))

    rows: list[tuple[str, float, str]] = []
    seen_dates: set[str] = set()
    skipped_empty: list[str] = []
    skipped_no_price: list[str] = []

    for pdf in pdf_files:
        logger.info("Processing: %s", pdf.name)
        text = _extract_text(pdf)
        if not text.strip():
            logger.info("  SKIP (image-only / no text): %s", pdf.name)
            skipped_empty.append(pdf.name)
            continue

        result = _parse_price(text)
        if not result:
            logger.warning("  SKIP (price not found): %s", pdf.name)
            skipped_no_price.append(pdf.name)
            continue

        date_str, price = result
        if date_str not in seen_dates:
            seen_dates.add(date_str)
            rows.append((date_str, price, pdf.name))
            logger.info("  OK  %s → US$ %.2f  (%s)", date_str, price, pdf.name)
        else:
            logger.info("  DUP date %s — skipping %s", date_str, pdf.name)

    rows.sort(key=lambda r: r[0])

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["date", "icp_price", "source_pdf"])
        for d, p, src in rows:
            writer.writerow([d, f"{p:.2f}", src])

    print(f"\n{'='*55}")
    print(f"Done: {len(rows)} records written → {OUTPUT_CSV}")
    print(f"{'='*55}")
    print("\ndate,icp_price,source_pdf")
    for d, p, src in rows:
        print(f"{d},{p:.2f},{src}")

    if skipped_empty:
        print(f"\nSkipped (no extractable text): {len(skipped_empty)} PDFs")
        for f in skipped_empty:
            print(f"  - {f}")
    if skipped_no_price:
        print(f"\nSkipped (price not parsed): {len(skipped_no_price)} PDFs")
        for f in skipped_no_price:
            print(f"  - {f}")


# ----------------------------------------------------------------------------
# Aliases
# ----------------------------------------------------------------------------
rebuild = rebuild_dataset


def main():
    # Memproses pembentukan data ICP dari eksekusi berkas terminal
    rebuild_dataset()


if __name__ == "__main__":
    main()
