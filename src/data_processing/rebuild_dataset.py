# Rebuild dataset from PDFs in data/raw

from __future__ import annotations

import csv
import logging
import re
import sys
from pathlib import Path

_SRC_DIR = Path(__file__).resolve().parents[1]
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("rebuild_dataset")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR      = PROJECT_ROOT / "data" / "raw"
OUTPUT_CSV   = PROJECT_ROOT / "data" / "processed" / "icp_dataset.csv"

PRICE_MIN = 20.0
PRICE_MAX = 200.0

MONTH_MAP = {
    "januari": 1, "februari": 2, "maret": 3, "april": 4,
    "mei": 5, "juni": 6, "juli": 7, "agustus": 8,
    "september": 9, "oktober": 10, "november": 11, "desember": 12,
}
MONTH_RE = "|".join(MONTH_MAP.keys())

# Find price anchor
_PAT_ANCHOR = re.compile(
    r"(?:ditetapkan\s+sebesar|sebesar)\s+US\$\s*(?P<price>\d+[.,]\d+)",
    re.IGNORECASE,
)
_PAT_MONTH_YEAR = re.compile(
    r"(?:untuk\s+)?(?:bulan\s+)?(?P<month>" + MONTH_RE + r")\s+(?P<year>20\d{2})",
    re.IGNORECASE,
)
# Fallback pattern
_PAT_BROAD = re.compile(
    r"(?P<month>" + MONTH_RE + r")\s+(?P<year>20\d{2})(?:.{0,300}?)US\$\s*(?P<price>\d+[.,]\d+)",
    re.IGNORECASE | re.DOTALL,
)


def _flatten(text: str) -> str:
    return re.sub(r"[ \t]*\n[ \t]*", " ", text)


def _to_price(raw: str) -> float:
    return float(raw.replace(",", "."))


def _parse_price(text: str):
    flat = _flatten(text)
    for anchor in _PAT_ANCHOR.finditer(flat):
        try:
            price = _to_price(anchor.group("price"))
        except ValueError:
            continue
        if not (PRICE_MIN < price < PRICE_MAX):
            continue
        window = flat[max(0, anchor.start() - 300):anchor.start()]
        best = None
        for my in _PAT_MONTH_YEAR.finditer(window):
            best = my
        if best:
            month_num = MONTH_MAP.get(best.group("month").lower())
            if month_num:
                return f"{best.group('year')}-{month_num:02d}", price
    for m in _PAT_BROAD.finditer(flat):
        month_num = MONTH_MAP.get(m.group("month").lower())
        if not month_num:
            continue
        try:
            price = _to_price(m.group("price"))
        except ValueError:
            continue
        if PRICE_MIN < price < PRICE_MAX:
            return f"{m.group('year')}-{month_num:02d}", price
    return None


def _extract_text(pdf_path: Path) -> str:
    try:
        import pdfplumber
    except ImportError:
        raise ImportError("Run: pip install pdfplumber")
    try:
        with pdfplumber.open(pdf_path) as pdf:
            return "\n".join(p.extract_text() or "" for p in pdf.pages)
    except Exception as exc:
        logger.warning("pdfplumber failed on '%s': %s", pdf_path.name, exc)
        return ""


def rebuild_dataset():

    pdf_files = sorted(RAW_DIR.glob("icp_*.pdf"))
    if not pdf_files:
        logger.error("No PDFs found in %s", RAW_DIR)
        sys.exit(1)

    logger.info("Found %d PDFs — processing with pdfplumber (no OCR)", len(pdf_files))

    seen: dict[str, float] = {}
    skipped_empty: list[str] = []
    skipped_no_price: list[str] = []

    for pdf in pdf_files:
        text = _extract_text(pdf)
        if not text.strip():
            logger.info("SKIP (image-only): %s", pdf.name)
            skipped_empty.append(pdf.name)
            continue
        result = _parse_price(text)
        if not result:
            logger.warning("SKIP (price not found): %s", pdf.name)
            skipped_no_price.append(pdf.name)
            continue
        date_str, price = result
        if date_str not in seen:
            seen[date_str] = price
            logger.info("OK  %s → US$ %.2f  (%s)", date_str, price, pdf.name)

    sorted_rows = sorted(seen.items())
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["date", "icp_price"])
        for d, p in sorted_rows:
            writer.writerow([d, f"{p:.2f}"])

    print(f"\n{'='*50}")
    print(f"Done: {len(sorted_rows)} records → {OUTPUT_CSV}")
    print(f"{'='*50}")
    print("\ndate,icp_price")
    for d, p in sorted_rows:
        print(f"{d},{p:.2f}")

    if skipped_empty:
        print(f"\nSkipped (image-only): {len(skipped_empty)} PDFs")
    if skipped_no_price:
        print(f"Skipped (no price detected): {len(skipped_no_price)} PDFs")
        for f in skipped_no_price:
            print(f"  - {f}")


# Alias
rebuild = rebuild_dataset


def main():
    """Entry point for command-line execution."""
    rebuild_dataset()


if __name__ == "__main__":
    main()
