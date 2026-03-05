# Extract ICP price from a PDF.
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def extract_icp_from_pdf(pdf_path: str | Path) -> Optional[tuple[str, float]]:
    import sys
    _src = Path(__file__).resolve().parents[1]
    if str(_src) not in sys.path:
        sys.path.insert(0, str(_src))

    from utils.pdf_utils import extract_text
    from utils.text_parsing import parse_icp_price, parse_date_from_filename

    pdf_path = Path(pdf_path)
    text = extract_text(pdf_path, fallback_ocr=False)

    if text.strip():
        result = parse_icp_price(text)
        if result:
            date_str, price = result
            logger.info("Extracted %s → %.2f from '%s'", date_str, price, pdf_path.name)
            return date_str, price
        logger.warning("Text found but price not parsed in '%s'.", pdf_path.name)
    else:
        logger.warning("No text in '%s' — image-only PDF, skipped.", pdf_path.name)

    return None


def extract_all_pdfs(raw_dir: str | Path, pattern: str = "icp_*.pdf") -> list[dict]:
    raw_dir = Path(raw_dir)
    pdf_files = sorted(raw_dir.glob(pattern))

    if not pdf_files:
        logger.warning("No PDFs found in '%s'.", raw_dir)
        return []

    logger.info("Processing %d PDF files.", len(pdf_files))
    records = []
    for pdf in pdf_files:
        result = extract_icp_from_pdf(pdf)
        if result:
            date_str, price = result
            records.append({"date": date_str, "icp_price": price})

    logger.info("Extracted %d records.", len(records))
    return records
