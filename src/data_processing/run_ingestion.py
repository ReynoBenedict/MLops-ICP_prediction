# run_ingestion.py
# ETL Pipeline untuk mengunduh laporan ICP dari ESDM ke data/raw/
# Cara pakai: python src/data_processing/run_ingestion.py

from __future__ import annotations

import logging
import re
import shutil
import sys
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse, unquote

# ---------------------------------------------------------------------------
# PATH SETUP
# ---------------------------------------------------------------------------
_SRC_DIR = Path(__file__).resolve().parents[1]
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run_ingestion")

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
SOURCE_URL   = "https://migas.esdm.go.id/post/harga-minyak-mentah"
RAW_PDF_DIR  = _PROJECT_ROOT / "data" / "raw"
# Hapus RAW_LEGACY_DIR karena sudah digabung ke data/raw/
PROCESSED_DIR = _PROJECT_ROOT / "data" / "processed"
DATASET_CSV  = PROCESSED_DIR / "icp_dataset.csv"

DOWNLOAD_DELAY = 1.0  # seconds between requests

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

# Patterns for extracting year-month from URL or filename
_PAT_URL_MONTH_YEAR = re.compile(
    r"(?P<month>" + _MONTH_RE_STR + r")[_\s%-](?P<year>20\d{2})",
    re.IGNORECASE,
)
_PAT_URL_YEAR_MONTH = re.compile(
    r"(?P<year>20\d{2})[/_-](?:0?(?P<month_num>\d{1,2})(?:[/_-]|$)|"
    r"(?P<month>" + _MONTH_RE_STR + r"))",
    re.IGNORECASE,
)
_PAT_FILENAME_DATE = re.compile(r"(\d{4})[_-](\d{2})", re.IGNORECASE)


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

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
    # Menebak nama file icp_YYYY_MM.pdf dari URL PDF atau teks link.
    decoded_url = unquote(pdf_url)
    url_path    = urlparse(decoded_url).path

    # --- Try: year & month-name in URL path (most common) ---
    m = _PAT_URL_MONTH_YEAR.search(url_path)
    if m:
        month_num = MONTH_MAP_ID.get(m.group("month").lower())
        year      = m.group("year")
        if month_num:
            return f"icp_{year}_{month_num:02d}.pdf"

    # --- Try: YYYY/MM numeric pattern in URL ---
    m2 = _PAT_FILENAME_DATE.search(url_path)
    if m2:
        year, month = m2.group(1), m2.group(2).zfill(2)
        if 1 <= int(month) <= 12:
            return f"icp_{year}_{month}.pdf"

    # --- Try: link text = month name + known column year ---
    clean_text = link_text.strip().lower()
    if clean_text in _MONTH_NAMES and column_year:
        month_num = MONTH_MAP_ID[clean_text]
        return f"icp_{column_year}_{month_num:02d}.pdf"

    # --- Try: month name in URL with numeric year ---
    for month_name, month_num in MONTH_MAP_ID.items():
        if month_name in url_path.lower():
            year_m = re.search(r"20\d{2}", url_path)
            if year_m:
                return f"icp_{year_m.group()}_{month_num:02d}.pdf"

    logger.debug("Cannot determine filename from URL: %s  text: %s", pdf_url, link_text)
    return None


# ---------------------------------------------------------------------------
# STEP 1 — EXTRACT: crawl webpage and collect PDF links
# ---------------------------------------------------------------------------

def collect_pdf_links(source_url: str, session) -> list[dict]:
    # Mengambil daftar link PDF dari halaman ESDM.
    logger.info("=" * 60)
    logger.info("[EXTRACT] Crawling: %s", source_url)
    logger.info("=" * 60)

    resp = session.get(source_url, timeout=30)
    resp.raise_for_status()
    soup = _parse_html(resp.text)

    pdf_entries: list[dict] = []
    seen_urls: set[str] = set()

    # ---- Strategy 1: parse the year-column table -------------------------
    # The page contains a <table> whose <th> cells are year numbers and
    # whose <td> cells hold month-name links pointing directly to PDFs.
    for table in soup.find_all("table"):
        headers: list[Optional[int]] = []

        # Collect column years from the header row
        header_row = table.find("tr")
        if header_row:
            for th in header_row.find_all(["th", "td"]):
                text = th.get_text(strip=True)
                if re.fullmatch(r"20\d{2}", text):
                    headers.append(int(text))
                else:
                    headers.append(None)

        if not any(h for h in headers):
            continue  # this table has no year columns — skip

        # Iterate data rows
        for row in table.find_all("tr")[1:]:
            cells = row.find_all("td")
            for col_idx, cell in enumerate(cells):
                col_year = headers[col_idx] if col_idx < len(headers) else None
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
                        "url":      full_url,
                        "filename": filename,
                        "year":     col_year,
                        "month":    month_num,
                        "link_text": link_text,
                    })

    # ---- Strategy 2: fallback — any .pdf link on the page ---------------
    if not pdf_entries:
        logger.warning("Table strategy found nothing — falling back to all-link scan.")
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"].strip()
            if not href.lower().endswith(".pdf"):
                continue
            full_url = urljoin(source_url, href)
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)
            link_text = a_tag.get_text(strip=True)
            filename  = _infer_filename(full_url, link_text, None)
            pdf_entries.append({
                "url":      full_url,
                "filename": filename,
                "year":     None,
                "month":    None,
                "link_text": link_text,
            })

    logger.info("[EXTRACT] Found %d PDF links.", len(pdf_entries))
    return pdf_entries


# ---------------------------------------------------------------------------
# STEP 2 — EXTRACT continued: download PDFs
# ---------------------------------------------------------------------------

def download_pdfs(
    pdf_entries: list[dict],
    dest_dir: Path,
    session,
    delay: float = DOWNLOAD_DELAY,
) -> list[Path]:
    # Mengunduh setiap PDF ke folder tujuan.
    dest_dir.mkdir(parents=True, exist_ok=True)
    downloaded: list[Path] = []
    total = len(pdf_entries)

    for idx, entry in enumerate(pdf_entries, start=1):
        pdf_url  = entry["url"]
        filename = entry.get("filename")

        if not filename:
            # Use the raw basename from the URL as a last resort
            raw_name = Path(urlparse(pdf_url).path).name
            filename = raw_name if raw_name.endswith(".pdf") else None

        if not filename:
            logger.warning("[%d/%d] Cannot determine filename — skipping: %s", idx, total, pdf_url)
            continue

        dest_path = dest_dir / filename

        logger.info("[%d/%d] Downloading → %s", idx, total, filename)
        try:
            with session.get(pdf_url, stream=True, timeout=60) as resp:
                resp.raise_for_status()
                with open(dest_path, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=8192):
                        fh.write(chunk)
            size_kb = dest_path.stat().st_size // 1024
            logger.info("          Saved  %s  (%d KB)", dest_path.name, size_kb)
            downloaded.append(dest_path)
        except Exception as exc:
            logger.error("          FAILED: %s — %s", pdf_url, exc)
            if dest_path.exists():
                dest_path.unlink()

        if idx < total:
            time.sleep(delay)

    return downloaded


# ---------------------------------------------------------------------------
# STEP 3 — TRANSFORM: extract text from PDFs
# ---------------------------------------------------------------------------

def _words_to_text(words: list) -> str:
    # Mengonversi kata dari pdfplumber menjadi teks baris.
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


def extract_text_from_pdfs(
    pdf_dir: Path,
    output_dir: Path,
    extra_dirs: list[Path] | None = None,
) -> list[dict]:
    # Ekstrak teks dari PDF menggunakan pdfplumber dengan strategi baris & karakter.
    try:
        import pdfplumber
    except ImportError:
        raise ImportError("Run: pip install pdfplumber")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Collect PDFs from all specified directories (de-duplicate by filename)
    all_dirs = [pdf_dir] + (extra_dirs or [])
    seen: dict[str, Path] = {}
    for d in all_dirs:
        if d.exists():
            for p in sorted(d.glob("*.pdf")):
                seen[p.name] = p   # later dirs win on name collision
    pdf_files = sorted(seen.values(), key=lambda p: p.name)

    if not pdf_files:
        logger.warning("[TRANSFORM] No PDFs found in %s", [str(d) for d in all_dirs])
        return []

    logger.info("=" * 60)
    logger.info("[TRANSFORM] Extracting text from %d PDFs ...", len(pdf_files))
    logger.info("=" * 60)

    results: list[dict] = []
    for pdf_path in pdf_files:
        logger.info("[TRANSFORM] Processing: %s", pdf_path.name)
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
            logger.warning("          pdfplumber error on '%s': %s", pdf_path.name, exc)

        text = "\n".join(page_texts).strip()

        if not text:
            logger.info("          (image-only or empty PDF — no text extracted)")

        # Save raw text
        txt_path = output_dir / (pdf_path.stem + ".txt")
        txt_path.write_text(text, encoding="utf-8")
        logger.info("          Text saved → %s", txt_path.name)

        results.append({
            "pdf_name": pdf_path.name,
            "pdf_path": str(pdf_path),
            "txt_path": str(txt_path),
            "text":     text,
            "has_text": bool(text),
        })

    logger.info("[TRANSFORM] Done. %d files processed.", len(results))
    return results


# ---------------------------------------------------------------------------
# STEP 4 — LOAD: build structured CSV dataset
# ---------------------------------------------------------------------------

def build_csv_dataset(extraction_results: list[dict], csv_path: Path) -> None:
    # Membuat atau memperbarui CSV dataset dari hasil ekstraksi.
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("Run: pip install pandas")

    try:
        from data_processing.rebuild_dataset import _parse_price, _extract_text as _rb_extract
    except ImportError:
        _rb_extract = None

    # Import price-parsing utilities
    sys.path.insert(0, str(_SRC_DIR))
    from utils.text_parsing import parse_icp_price, parse_date_from_filename

    logger.info("=" * 60)
    logger.info("[LOAD] Building dataset CSV ...")
    logger.info("=" * 60)

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    records: list[dict] = []
    skipped_empty: list[str] = []
    skipped_no_price: list[str] = []

    for item in extraction_results:
        pdf_name = item["pdf_name"]
        text     = item.get("text", "")

        if not text.strip():
            logger.info("  SKIP (no text): %s", pdf_name)
            skipped_empty.append(pdf_name)
            continue

        result = parse_icp_price(text)
        if not result:
            # Fallback: use date from filename
            date_str = parse_date_from_filename(pdf_name)
            if not date_str:
                logger.warning("  SKIP (price not found): %s", pdf_name)
                skipped_no_price.append(pdf_name)
                continue
            logger.info("  Partial (date from filename only): %s → %s", pdf_name, date_str)
            records.append({"date": date_str, "icp_price": None, "source_pdf": pdf_name})
            continue

        date_str, price = result
        logger.info("  OK  %s → %s  US$ %.2f", pdf_name, date_str, price)
        records.append({"date": date_str, "icp_price": price, "source_pdf": pdf_name})

    if not records:
        logger.warning("[LOAD] No records extracted — CSV not written.")
        return

    df = pd.DataFrame(records, columns=["date", "icp_price", "source_pdf"])
    df = df.drop_duplicates(subset="date", keep="first")
    df = df.sort_values("date").reset_index(drop=True)
    df.to_csv(csv_path, index=False)

    logger.info("=" * 60)
    logger.info("[LOAD] Dataset saved: %d records → %s", len(df), csv_path)
    if skipped_empty:
        logger.info("       Skipped (image-only): %d", len(skipped_empty))
    if skipped_no_price:
        logger.info("       Skipped (price not found): %d", len(skipped_no_price))
        for f in skipped_no_price:
            logger.info("         - %s", f)
    logger.info("=" * 60)

    # Print preview
    print("\n" + "=" * 60)
    print(f"DATASET PREVIEW — {len(df)} records")
    print("=" * 60)
    print(df.to_string(index=False))
    print("=" * 60)


# ---------------------------------------------------------------------------
# MAIN ETL ORCHESTRATOR
# ---------------------------------------------------------------------------

def run_ingestion(
    source_url: str  = SOURCE_URL,
    raw_pdf_dir: Path = RAW_PDF_DIR,
    processed_dir: Path = PROCESSED_DIR,
    delay: float      = DOWNLOAD_DELAY,
) -> None:
    # Pipeline ETL lengkap: crawl web, unduh PDF ke data/raw/, dan simpan ke CSV.
    raw_pdf_dir  = Path(raw_pdf_dir)
    processed_dir = Path(processed_dir)
    csv_path      = processed_dir / "icp_dataset.csv"

    # ------------------------------------------------------------------
    # STEP 0 — Clear existing PDFs
    # ------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("[INIT] Clearing existing PDFs in %s ...", raw_pdf_dir)
    logger.info("=" * 60)
    if raw_pdf_dir.exists():
        shutil.rmtree(raw_pdf_dir)
        logger.info("[INIT] Removed directory: %s", raw_pdf_dir)
    raw_pdf_dir.mkdir(parents=True, exist_ok=True)
    logger.info("[INIT] Created fresh directory: %s", raw_pdf_dir)

    session = _get_session()

    # ------------------------------------------------------------------
    # STEP 1 — Collect PDF links
    # ------------------------------------------------------------------
    pdf_entries = collect_pdf_links(source_url, session)

    if not pdf_entries:
        logger.error("[EXTRACT] No PDF links found. Aborting.")
        return

    # ------------------------------------------------------------------
    # STEP 2 — Download PDFs
    # ------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("[EXTRACT] Downloading %d PDFs ...", len(pdf_entries))
    logger.info("=" * 60)
    downloaded = download_pdfs(pdf_entries, raw_pdf_dir, session, delay)
    logger.info("[EXTRACT] %d PDFs downloaded successfully.", len(downloaded))

    if not downloaded:
        logger.error("[EXTRACT] No PDFs downloaded. Aborting.")
        return

    # ------------------------------------------------------------------
    # STEP 3 — Extract text
    # ------------------------------------------------------------------
    extraction_results = extract_text_from_pdfs(
        raw_pdf_dir,
        processed_dir
    )

    # ------------------------------------------------------------------
    # STEP 4 — Build dataset CSV
    # ------------------------------------------------------------------
    build_csv_dataset(extraction_results, csv_path)

    logger.info("Pipeline completed successfully.")
    logger.info("  Raw PDFs   : %s", raw_pdf_dir)
    logger.info("  Processed  : %s", processed_dir)
    logger.info("  Dataset CSV: %s", csv_path)


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="ICP ETL Pipeline — unduh & ekstrak laporan harga minyak mentah ESDM."
    )
    parser.add_argument("--url",     default=SOURCE_URL,        help="URL halaman sumber ESDM")
    parser.add_argument("--raw-dir", default=str(RAW_PDF_DIR),  help="Direktori simpan PDF")
    parser.add_argument("--out-dir", default=str(PROCESSED_DIR), help="Direktori output processed")
    parser.add_argument("--delay",   type=float, default=DOWNLOAD_DELAY, help="Jeda antar-unduhan (detik)")
    args = parser.parse_args()

    run_ingestion(
        source_url    = args.url,
        raw_pdf_dir   = Path(args.raw_dir),
        processed_dir = Path(args.out_dir),
        delay         = args.delay,
    )


if __name__ == "__main__":
    main()
