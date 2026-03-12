# pdf_utils.py — extract text from PDF files using pdfplumber
# Uses only packages already in requirements.txt (pdfplumber)

from __future__ import annotations

import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)


def _words_to_text(words: list) -> str:
    # Mengonversi kata dari pdfplumber menjadi teks sederhana, baris demi baris, 
    # untuk PDF tanpa aliran teks tetapi memiliki glyph tertanam.
    if not words:
        return ""
    # Sort by (top, x0)
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


def extract_text_pdfplumber(pdf_path: str | Path) -> str:
    # Ekstrak seluruh teks menggunakan pemanggilan dua strategi: jalur reguler dan opsi karakter.
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
                # --- strategy 1: extract_text ---
                text = page.extract_text() or ""
                if not text.strip():
                    # --- strategy 2: extract_words (character-level) ---
                    try:
                        words = page.extract_words(
                            x_tolerance=3,
                            y_tolerance=3,
                            keep_blank_chars=False,
                            use_text_flow=False,
                        )
                        text = _words_to_text(words)
                    except Exception as exc:
                        logger.debug("extract_words failed on page: %s", exc)
                        text = ""
                page_texts.append(text)
    except Exception as exc:
        logger.warning("pdfplumber failed on '%s': %s", pdf_path.name, exc)
        return ""

    result = "\n".join(page_texts)
    logger.debug("Extracted %d chars from '%s'.", len(result), pdf_path.name)
    return result


def extract_text(pdf_path: str | Path, fallback_ocr: bool = False) -> str:
    # Membantu penggalian data untuk sistem publik, mengabaikan opsi OCR fallback karna diluar lingkup LK3.
    return extract_text_pdfplumber(pdf_path)
