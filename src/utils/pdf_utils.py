# pdf_utils.py — extract text from PDF files using pdfplumber

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def extract_text_pdfplumber(pdf_path: str | Path) -> str:
    try:
        import pdfplumber
    except ImportError:
        raise ImportError("Run: pip install pdfplumber")

    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        logger.error("PDF not found: %s", pdf_path)
        return ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = "\n".join(p.extract_text() or "" for p in pdf.pages)
        logger.debug("Extracted %d chars from '%s'.", len(text), pdf_path.name)
        return text
    except Exception as exc:
        logger.warning("pdfplumber failed on '%s': %s", pdf_path.name, exc)
        return ""


def extract_text(pdf_path: str | Path, fallback_ocr: bool = False) -> str:
    # OCR fallback is disabled by default — image-only PDFs are skipped
    return extract_text_pdfplumber(pdf_path)
