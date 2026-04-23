# run_ingestion.py
# Pipeline ETL: unduh laporan ICP dari ESDM ke data/raw/
# Cara pakai: python src/data_processing/run_ingestion.py

from __future__ import annotations

import logging
import re
import sys
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse, unquote

_SRC_DIR = Path(__file__).resolve().parents[1]
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run_ingestion")

SOURCE_URL    = "https://migas.esdm.go.id/post/harga-minyak-mentah"
RAW_PDF_DIR   = _PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = _PROJECT_ROOT / "data" / "processed"
DATASET_CSV   = RAW_PDF_DIR / "dataset.csv"

TARGET_YEAR    = 2019
DOWNLOAD_DELAY = 1.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "id-ID,id;q=0.9,en;q=0.8",
    "Referer": "https://migas.esdm.go.id/",
}

MONTH_MAP_ID: dict[str, int] = {
    "januari": 1, "februari": 2, "maret": 3, "april": 4,
    "mei": 5, "juni": 6, "juli": 7, "agustus": 8,
    "september": 9, "oktober": 10, "november": 11, "desember": 12,
}
_MONTH_NAMES = set(MONTH_MAP_ID.keys())
_MONTH_RE_STR = "|".join(MONTH_MAP_ID.keys())

MONTH_MAP_EN: dict[str, int] = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "mei": 5, "jun": 6, "jul": 7, "ags": 8, "aug": 8,
    "sep": 9, "sept": 9, "oct": 10, "okto": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9,
    "october": 10, "november": 11, "december": 12,
}
_MONTH_MAP_ALL = {**MONTH_MAP_ID, **MONTH_MAP_EN}
_MONTH_RE_ALL = "|".join(sorted(_MONTH_MAP_ALL.keys(), key=len, reverse=True))

_PAT_URL_MONTH_YEAR = re.compile(
    r"(?P<month>" + _MONTH_RE_ALL + r")[_\s%-]+(?P<year>20\d{2})",
    re.IGNORECASE,
)
_PAT_URL_YEAR_MONTH = re.compile(
    r"(?P<year>20\d{2})[/_-](?:0?(?P<month_num>\d{1,2})(?:[/_-]|$)|"
    r"(?P<month>" + _MONTH_RE_ALL + r"))",
    re.IGNORECASE,
)
_PAT_FILENAME_DATE = re.compile(r"(\d{4})[_-](\d{2})", re.IGNORECASE)


def _get_session():
    try:
        import requests
    except ImportError:
        raise ImportError("Run: pip install requests")
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _parse_html(html: str):
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        raise ImportError("Run: pip install beautifulsoup4 lxml")
    return BeautifulSoup(html, "lxml")


def _infer_filename(pdf_url: str, link_text: str, column_year: Optional[int]) -> Optional[str]:
    decoded_url = unquote(pdf_url)
    url_path    = urlparse(decoded_url).path

    m = _PAT_URL_MONTH_YEAR.search(url_path)
    if m:
        month_num = _MONTH_MAP_ALL.get(m.group("month").lower())
        year      = m.group("year")
        if month_num:
            return f"icp_{year}_{month_num:02d}.pdf"

    m2 = _PAT_FILENAME_DATE.search(url_path)
    if m2:
        year, month = m2.group(1), m2.group(2).zfill(2)
        if 1 <= int(month) <= 12:
            return f"icp_{year}_{month}.pdf"

    clean_text = link_text.strip().lower()
    year_m = re.search(r"20\d{2}", url_path)
    inferred_year = int(year_m.group()) if year_m else column_year

    month_num = _MONTH_MAP_ALL.get(clean_text)
    if month_num and inferred_year:
        return f"icp_{inferred_year}_{month_num:02d}.pdf"

    for token, mnum in sorted(_MONTH_MAP_ALL.items(), key=lambda x: len(x[0]), reverse=True):
        if token in url_path.lower():
            if inferred_year:
                return f"icp_{inferred_year}_{mnum:02d}.pdf"

    logger.debug("Tidak bisa tentukan nama file dari URL: %s  teks: %s", pdf_url, link_text)
    return None


def collect_pdf_links(source_url: str, session, target_year: int = None) -> list[dict]:
    logger.info("=" * 60)
    logger.info("[EXTRACT] Crawling: %s  (target year: %d)", source_url, target_year)
    logger.info("=" * 60)

    resp = session.get(source_url, timeout=30)
    resp.raise_for_status()
    soup = _parse_html(resp.text)

    pdf_entries: list[dict] = []
    seen_urls: set[str] = set()

    for table in soup.find_all("table"):
        headers: list[Optional[int]] = []
        header_row = table.find("tr")
        if header_row:
            for th in header_row.find_all(["th", "td"]):
                text = th.get_text(strip=True)
                headers.append(int(text) if re.fullmatch(r"20\d{2}", text) else None)

        if not any(h for h in headers):
            continue

        for row in table.find_all("tr")[1:]:
            cells = row.find_all("td")
            for col_idx, cell in enumerate(cells):
                col_year = headers[col_idx] if col_idx < len(headers) else None
                if col_year != target_year:
                    continue
                for a_tag in cell.find_all("a", href=True):
                    href = a_tag["href"].strip()
                    if not href.lower().endswith(".pdf"):
                        continue
                    full_url = urljoin(source_url, href)
                    if full_url in seen_urls:
                        continue
                    seen_urls.add(full_url)
                    link_text = a_tag.get_text(strip=True)
                    filename  = _infer_filename(full_url, link_text, col_year)
                    month_num = MONTH_MAP_ID.get(link_text.strip().lower())
                    pdf_entries.append({
                        "url":       full_url,
                        "filename":  filename,
                        "year":      col_year,
                        "month":     month_num,
                        "link_text": link_text,
                    })

    if not pdf_entries:
        logger.warning("Strategi tabel kosong — fallback ke semua link PDF di halaman.")
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"].strip()
            if not href.lower().endswith(".pdf"):
                continue
            decoded_href = unquote(href)
            m_year = re.search(r"20\d{2}", decoded_href)
            inferred_year = int(m_year.group()) if m_year else None
            if inferred_year != target_year:
                continue
            full_url = urljoin(source_url, href)
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)
            link_text = a_tag.get_text(strip=True)
            month_num = _MONTH_MAP_ALL.get(link_text.strip().lower())
            filename  = _infer_filename(full_url, link_text, inferred_year)
            pdf_entries.append({
                "url":       full_url,
                "filename":  filename,
                "year":      inferred_year,
                "month":     month_num,
                "link_text": link_text,
            })

    logger.info("[EXTRACT] Ditemukan %d link PDF untuk tahun %d.", len(pdf_entries), target_year)
    return pdf_entries


def download_pdfs(
    pdf_entries: list[dict],
    dest_dir: Path,
    session,
    delay: float = DOWNLOAD_DELAY,
) -> list[Path]:
    # Mengunduh setiap PDF ke folder tujuan
    dest_dir.mkdir(parents=True, exist_ok=True)
    downloaded: list[Path] = []
    total = len(pdf_entries)

    for idx, entry in enumerate(pdf_entries, start=1):
        pdf_url  = entry["url"]
        filename = entry.get("filename")

        if not filename:
            raw_name = Path(urlparse(pdf_url).path).name
            filename = raw_name if raw_name.endswith(".pdf") else None

        if not filename:
            logger.warning("[%d/%d] Tidak bisa tentukan nama file — dilewati: %s", idx, total, pdf_url)
            continue

        dest_path = dest_dir / filename

        logger.info("[%d/%d] Mengunduh → %s", idx, total, filename)
        try:
            with session.get(pdf_url, stream=True, timeout=60) as resp:
                resp.raise_for_status()
                with open(dest_path, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=8192):
                        fh.write(chunk)
            size_kb = dest_path.stat().st_size // 1024
            logger.info("          Tersimpan  %s  (%d KB)", dest_path.name, size_kb)
            downloaded.append(dest_path)
        except Exception as exc:
            logger.error("          GAGAL: %s — %s", pdf_url, exc)
            if dest_path.exists():
                dest_path.unlink()

        if idx < total:
            time.sleep(delay)

    return downloaded


def _words_to_text(words: list) -> str:
    # Mengonversi kata dari pdfplumber menjadi teks baris
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


def _ocr_pdf(pdf_path: Path) -> str:
    try:
        import cv2
        import numpy as np
        import pytesseract
        import fitz
    except ImportError:
        logger.warning("OCR libraries not available (pytesseract, opencv-python, pymupdf).")
        return ""
    pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    texts: list[str] = []
    try:
        doc = fitz.open(str(pdf_path))
        for page in doc:
            pix = page.get_pixmap(dpi=200)
            img_np = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.h, pix.w, pix.n)
            if pix.n == 4:
                import cv2 as _cv2
                img_np = _cv2.cvtColor(img_np, cv2.COLOR_RGBA2RGB)
            gray = cv2.cvtColor(img_np, cv2.COLOR_RGB2GRAY)
            denoised = cv2.fastNlMeansDenoising(gray, h=10)
            _, thresh = cv2.threshold(denoised, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            resized = cv2.resize(thresh, None, fx=2, fy=2, interpolation=cv2.INTER_LINEAR)
            texts.append(pytesseract.image_to_string(resized, lang="ind+eng"))
        doc.close()
    except Exception as exc:
        logger.warning("OCR gagal pada '%s': %s", pdf_path.name, exc)
    return "\n".join(texts)


def extract_text_from_pdfs(
    pdf_dir: Path,
    output_dir: Path,
    extra_dirs: list[Path] | None = None,
) -> list[dict]:
    try:
        import pdfplumber
    except ImportError:
        raise ImportError("Run: pip install pdfplumber")

    output_dir.mkdir(parents=True, exist_ok=True)

    all_dirs = [pdf_dir] + (extra_dirs or [])
    seen: dict[str, Path] = {}
    for d in all_dirs:
        if d.exists():
            for p in sorted(d.glob("*.pdf")):
                seen[p.name] = p
    pdf_files = sorted(seen.values(), key=lambda p: p.name)

    if not pdf_files:
        logger.warning("[TRANSFORM] Tidak ada PDF ditemukan di %s", [str(d) for d in all_dirs])
        return []

    logger.info("=" * 60)
    logger.info("[TRANSFORM] Ekstrak teks dari %d PDF ...", len(pdf_files))
    logger.info("=" * 60)

    results: list[dict] = []
    for pdf_path in pdf_files:
        logger.info("[TRANSFORM] Memproses: %s", pdf_path.name)
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
            logger.warning("          pdfplumber error pada '%s': %s", pdf_path.name, exc)

        text = "\n".join(page_texts).strip()

        if not text:
            logger.info("          Teks kosong — mencoba OCR ...")
            text = _ocr_pdf(pdf_path)
            if text.strip():
                logger.info("          OCR berhasil.")
            else:
                logger.info("          OCR juga kosong.")

        txt_path = output_dir / (pdf_path.stem + ".txt")
        txt_path.write_text(text, encoding="utf-8")
        logger.info("          → %s", txt_path.name)

        results.append({
            "pdf_name": pdf_path.name,
            "pdf_path": str(pdf_path),
            "txt_path": str(txt_path),
            "text":     text,
            "has_text": bool(text),
        })

    logger.info("[TRANSFORM] Selesai. %d file diproses.", len(results))
    return results


_FNAME_MONTH_MAP: dict[str, int] = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "mei": 5, "jun": 6, "juli": 7, "jul": 7,
    "ags": 8, "aug": 8, "sep": 9, "sept": 9,
    "okto": 10, "oct": 10, "nov": 11, "dec": 12, "des": 12,
}


def _parse_date_from_any_filename(filename: str) -> Optional[str]:
    stem = Path(filename).stem.lower()
    year_m = re.search(r"20(\d{2})", stem)
    if not year_m:
        return None
    year = year_m.group(0)
    for token in sorted(_FNAME_MONTH_MAP.keys(), key=len, reverse=True):
        if token in stem:
            month_num = _FNAME_MONTH_MAP[token]
            return f"{year}-{month_num:02d}"
    return None


def build_csv_dataset(
    extraction_results: list[dict],
    csv_path: Path,
    target_year: int = None,
) -> None:
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("Run: pip install pandas")

    sys.path.insert(0, str(_SRC_DIR))
    from utils.text_parsing import parse_icp_price

    logger.info("=" * 60)
    logger.info("[LOAD] Membangun dataset CSV (target %d) ...", target_year)
    logger.info("=" * 60)

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    records: list[dict] = []

    for item in extraction_results:
        pdf_name = item["pdf_name"]
        text     = item.get("text", "")

        price_date = None
        if text.strip():
            price_date = parse_icp_price(text)

        if price_date:
            date_str, price = price_date
        else:
            date_str = _parse_date_from_any_filename(pdf_name)
            price    = None

        if not date_str:
            logger.warning("  SKIP (tidak bisa tentukan tanggal): %s", pdf_name)
            continue

        year_val = int(date_str[:4])
        if year_val not in [2019, 2020]:
            continue

        month_val = int(date_str[5:7])
        logger.info("  %s -> month=%d  price=%s", pdf_name, month_val, price)
        records.append({"month": month_val, "year": year_val, "icp_price": price})

    df = pd.DataFrame(records, columns=["month", "year", "icp_price"])
    df = df.drop_duplicates(subset=["year", "month"], keep="first")

    df = df.sort_values(["year", "month"]).reset_index(drop=True)
    df["icp_price"] = pd.to_numeric(df["icp_price"], errors="coerce")
    df["icp_price"] = df["icp_price"].interpolate(method="linear").ffill().bfill()
    df["icp_price"] = df["icp_price"].round(2)
    df = df.sort_values(["year", "month"]).reset_index(drop=True)

    df.to_csv(csv_path, index=False)

    logger.info("=" * 60)
    logger.info("[LOAD] Dataset tersimpan: %d record -> %s", len(df), csv_path)
    logger.info("=" * 60)

    print("\n" + "=" * 60)
    print("DATASET ({}) -- {} record".format(target_year, len(df)))
    print("=" * 60)
    print(df.to_string(index=False))
    print("=" * 60)
    print("=" * 60)


def run_local(
    raw_pdf_dir:  Path = RAW_PDF_DIR,
    processed_dir: Path = PROCESSED_DIR,
    target_year:  int   = TARGET_YEAR,
) -> None:
    raw_pdf_dir   = Path(raw_pdf_dir)
    processed_dir = Path(processed_dir)
    csv_path      = raw_pdf_dir / "dataset.csv"

    raw_pdf_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("[LOCAL] Ekstrak dari PDF yang sudah ada di %s", raw_pdf_dir)
    logger.info("=" * 60)

    extraction_results = extract_text_from_pdfs(raw_pdf_dir, processed_dir)

    if not extraction_results:
        logger.error("[LOCAL] Tidak ada PDF yang berhasil diekstrak.")
        return

    build_csv_dataset(extraction_results, csv_path, target_year=target_year)

    logger.info("[LOCAL] Selesai.")
    logger.info("  PDF mentah  : %s", raw_pdf_dir)
    logger.info("  Dataset CSV : %s", csv_path)


def run_ingestion(
    source_url:   str   = SOURCE_URL,
    raw_pdf_dir:  Path  = RAW_PDF_DIR,
    processed_dir: Path = PROCESSED_DIR,
    delay:        float = DOWNLOAD_DELAY,
    target_year:  int   = TARGET_YEAR,
) -> None:
    raw_pdf_dir   = Path(raw_pdf_dir)
    processed_dir = Path(processed_dir)
    csv_path      = raw_pdf_dir / "dataset.csv"

    raw_pdf_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    session = _get_session()

    pdf_entries = collect_pdf_links(source_url, session, target_year=target_year)

    if not pdf_entries:
        logger.error("[EXTRACT] Tidak ada link PDF ditemukan untuk tahun %d.", target_year)
        return

    logger.info("=" * 60)
    logger.info("[EXTRACT] Mengunduh %d PDF (tahun %d) ...", len(pdf_entries), target_year)
    logger.info("=" * 60)
    downloaded = download_pdfs(pdf_entries, raw_pdf_dir, session, delay)
    logger.info("[EXTRACT] %d PDF berhasil diunduh.", len(downloaded))

    if not downloaded:
        logger.error("[EXTRACT] Tidak ada PDF yang diunduh.")
        return

    extraction_results = extract_text_from_pdfs(raw_pdf_dir, processed_dir)
    build_csv_dataset(extraction_results, csv_path, target_year=target_year)

    logger.info("Pipeline selesai.")
    logger.info("  PDF mentah  : %s", raw_pdf_dir)
    logger.info("  Dataset CSV : %s", csv_path)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="ICP ETL Pipeline -- unduh & ekstrak laporan harga minyak mentah ESDM."
    )
    parser.add_argument("--url",         default=SOURCE_URL,         help="URL halaman sumber ESDM")
    parser.add_argument("--raw-dir",     default=str(RAW_PDF_DIR),   help="Direktori simpan PDF")
    parser.add_argument("--out-dir",     default=str(PROCESSED_DIR), help="Direktori output processed")
    parser.add_argument("--delay",       type=float, default=DOWNLOAD_DELAY, help="Jeda antar-unduhan (detik)")
    parser.add_argument("--year",        type=int,   default=TARGET_YEAR,    help="Tahun target")
    parser.add_argument("--local",       action="store_true",                help="Gunakan PDF lokal, skip download")
    args = parser.parse_args()

    if args.local:
        run_local(
            raw_pdf_dir   = Path(args.raw_dir),
            processed_dir = Path(args.out_dir),
            target_year   = args.year,
        )
    else:
        run_ingestion(
            source_url    = args.url,
            raw_pdf_dir   = Path(args.raw_dir),
            processed_dir = Path(args.out_dir),
            delay         = args.delay,
            target_year   = args.year,
        )


if __name__ == "__main__":
    main()
