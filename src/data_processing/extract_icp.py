# extract_icp.py — Ekstraksi teks dari PDF ICP dan penyimpanan ke data/processed/

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Memastikan direktori src/ dapat diakses
_SRC_DIR = Path(__file__).resolve().parents[1]
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

# Historical PDFs live in data/raw/; freshly downloaded ones in data/raw/pdfs/
RAW_PDF_DIR   = _PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = _PROJECT_ROOT / "data" / "processed"


# ---------------------------------------------------------------------------
# Core text extraction
# ---------------------------------------------------------------------------

def _words_to_text(words: list) -> str:
    """Convert pdfplumber word objects to a plain string, grouped by line."""
    if not words:
        return ""
    import re
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


def extract_text_from_pdf(pdf_path: str | Path) -> str:
    """
    Extract all textual content from a PDF using pdfplumber.

    Per-page strategy:
      1. extract_text()  — stream-based approach (fast)
      2. extract_words() — character-level grouping (fallback for pages
         where the text stream is absent but glyphs are still embedded)
    Returns an empty string for truly rasterised image-only pages.
    """
    try:
        import pdfplumber
    except ImportError:
        raise ImportError("Run: pip install pdfplumber")

    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        logger.error("PDF not found: %s", pdf_path)
        return ""
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
    result = "\n".join(page_texts)
    logger.debug("Extracted %d chars from '%s'.", len(result), pdf_path.name)
    return result.strip()


# ---------------------------------------------------------------------------
# Single-PDF pipeline: extract → parse → return record
# ---------------------------------------------------------------------------

def extract_icp_from_pdf(pdf_path: str | Path) -> Optional[tuple[str, float]]:
    """
    Extract ICP date + price from a single PDF.
    Returns (date_str, price) or None if not found.
    """
    from utils.pdf_utils import extract_text
    from utils.text_parsing import parse_icp_price, parse_date_from_filename

    pdf_path = Path(pdf_path)
    text = extract_text(pdf_path, fallback_ocr=False)

    if text.strip():
        result = parse_icp_price(text)
        if result:
            date_str, price = result
            logger.info("Extracted %s → %.2f  from '%s'", date_str, price, pdf_path.name)
            return date_str, price
        logger.warning("Text found but price not parsed in '%s'.", pdf_path.name)
    else:
        logger.warning("No text in '%s' — image-only PDF, skipped.", pdf_path.name)

    return None


# ---------------------------------------------------------------------------
# Batch extraction: all PDFs in a directory
# ---------------------------------------------------------------------------

def extract_all_pdfs(
    raw_dir: str | Path = RAW_PDF_DIR,
    processed_dir: str | Path = PROCESSED_DIR,
    pattern: str = "*.pdf",
    save_txt: bool = True,
) -> list[dict]:
    # Iterasi setiap PDF di raw_dir, ekstrak teks, opsional simpan .txt, dan return record terstruktur
    from utils.text_parsing import parse_icp_price, parse_date_from_filename

    raw_dir       = Path(raw_dir)
    processed_dir = Path(processed_dir)
    processed_dir.mkdir(parents=True, exist_ok=True)

    pdf_files = sorted(raw_dir.glob(pattern))
    if not pdf_files:
        logger.warning("No PDFs found in '%s'.", raw_dir)
        return []

    logger.info("[TRANSFORM] Processing %d PDF file(s).", len(pdf_files))
    records: list[dict] = []

    for pdf in pdf_files:
        logger.info("[TRANSFORM] Reading: %s", pdf.name)
        text = extract_text_from_pdf(pdf)

        # Save raw extracted text
        if save_txt:
            txt_path = processed_dir / (pdf.stem + ".txt")
            txt_path.write_text(text, encoding="utf-8")
            logger.info("            Saved text → %s", txt_path.name)

        if not text.strip():
            logger.info("            (image-only/empty — skipped)")
            continue

        result = parse_icp_price(text)
        if result:
            date_str, price = result
        else:
            date_str = parse_date_from_filename(pdf.name)
            price    = None
            if date_str:
                logger.info("            Price not found; date from filename: %s", date_str)
            else:
                logger.warning("            Cannot extract date or price — skipped.")
                continue

        records.append({
            "date":            date_str,
            "icp_price":       price,
            "source_document": pdf.name,
        })
        if price:
            logger.info("            → %s  US$ %.2f", date_str, price)

    logger.info("[TRANSFORM] Extracted %d record(s).", len(records))
    return records


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    records = extract_all_pdfs(RAW_PDF_DIR, PROCESSED_DIR)
    print(f"\nExtracted {len(records)} records:")
    for r in records:
        print(r)
