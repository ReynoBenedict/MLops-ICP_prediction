# run_ingestion.py
# Pipeline ETL: unduh laporan ICP dari ESDM ke data/raw/
# Cara pakai: python src/data_processing/run_ingestion.py

from __future__ import annotations

import logging
import re
import sys
import time
from pathlib import Path
from typing import Any, Optional
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

# target_year=None berarti proses semua tahun yang tersedia
TARGET_YEAR    = None
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


# ---------------------------------------------------------------------------
# Deteksi titik lanjut: bulan/tahun terakhir yang sudah ada di dataset.csv
# ---------------------------------------------------------------------------

def detect_latest_month(csv_path: Path = DATASET_CSV) -> Optional[tuple[int, int]]:
    """
    Baca dataset.csv dan kembalikan (year, month) dari baris terakhir secara kronologis.

    Return:
        (year, month) jika file ditemukan dan valid, atau None jika kosong/tidak ada.

    Log tags yang digunakan:
        [LATEST]  — titik lanjut terdeteksi
        [WARNING] — masalah data (duplikat, urutan salah, baris rusak)
    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("Run: pip install pandas")

    # File belum ada -> mulai dari awal (wajar untuk setup pertama kali)
    if not csv_path.exists():
        logger.warning("[WARNING] dataset.csv tidak ditemukan di %s — akan mulai dari awal.", csv_path)
        return None

    try:
        df = pd.read_csv(csv_path)
    except Exception as exc:
        logger.warning("[WARNING] Gagal membaca dataset.csv: %s — akan mulai dari awal.", exc)
        return None

    # Validasi kolom wajib
    required = {"year", "month"}
    if not required.issubset(df.columns):
        logger.warning(
            "[WARNING] dataset.csv tidak punya kolom %s (ada: %s) — dilewati.",
            required, list(df.columns),
        )
        return None

    # Buang baris yang kolom year/month-nya tidak bisa dikonversi ke integer
    before = len(df)
    df["year"]  = pd.to_numeric(df["year"],  errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    df = df.dropna(subset=["year", "month"])
    df["year"]  = df["year"].astype(int)
    df["month"] = df["month"].astype(int)
    removed = before - len(df)
    if removed:
        logger.warning("[WARNING] %d baris dengan year/month tidak valid dibuang.", removed)

    if df.empty:
        logger.warning("[WARNING] dataset.csv kosong setelah validasi — akan mulai dari awal.")
        return None

    # Validasi nilai bulan (1–12)
    bad_month = df[(df["month"] < 1) | (df["month"] > 12)]
    if not bad_month.empty:
        logger.warning("[WARNING] Ditemukan %d baris dengan bulan di luar 1–12:", len(bad_month))
        for _, row in bad_month.iterrows():
            logger.warning("          year=%s month=%s", row["year"], row["month"])

    # Deteksi duplikat (year, month)
    dupes = df[df.duplicated(subset=["year", "month"], keep=False)]
    if not dupes.empty:
        logger.warning(
            "[WARNING] Ditemukan %d baris duplikat pada pasangan (year, month):",
            len(dupes),
        )
        for _, row in dupes.drop_duplicates(subset=["year", "month"]).iterrows():
            logger.warning("          [WARNING] duplikat: year=%d month=%d", row["year"], row["month"])

    # Urutkan kronologis dan periksa apakah dataset sudah terurut
    df_sorted = df.sort_values(["year", "month"]).reset_index(drop=True)
    if not df.reset_index(drop=True)[["year", "month"]].equals(df_sorted[["year", "month"]]):
        logger.warning("[WARNING] dataset.csv tidak terurut kronologis — akan diurutkan sementara untuk deteksi.")

    # Deteksi bulan yang hilang dalam urutan kronologis
    _report_missing_months(df_sorted)

    # Ambil titik terakhir
    last = df_sorted.iloc[-1]
    latest_year  = int(last["year"])
    latest_month = int(last["month"])

    logger.info(
        "[LATEST] Titik data terakhir terdeteksi: %04d-%02d (baris=%d)",
        latest_year, latest_month, len(df_sorted),
    )
    return (latest_year, latest_month)


def _report_missing_months(df_sorted: Any) -> None:
    """
    Cetak peringatan untuk setiap bulan yang hilang dalam urutan kronologis.
    df_sorted harus sudah diurutkan berdasarkan [year, month].
    """
    try:
        import pandas as pd
    except ImportError:
        return

    if df_sorted.empty:
        return

    # Buat rangkaian bulan lengkap dari awal hingga akhir dataset
    first = df_sorted.iloc[0]
    last  = df_sorted.iloc[-1]

    start = pd.Period(f"{int(first['year'])}-{int(first['month']):02d}", freq="M")
    end   = pd.Period(f"{int(last['year'])}-{int(last['month']):02d}",   freq="M")

    full_range = pd.period_range(start=start, end=end, freq="M")

    # Set bulan yang benar-benar ada
    existing = set(
        zip(df_sorted["year"].tolist(), df_sorted["month"].tolist())
    )

    missing = [
        p for p in full_range
        if (p.year, p.month) not in existing
    ]

    if missing:
        logger.warning("[WARNING] Ditemukan %d bulan yang hilang dalam dataset:", len(missing))
        for p in missing:
            logger.warning("          [WARNING] bulan hilang: %04d-%02d", p.year, p.month)
    else:
        logger.info("[LATEST] Tidak ada bulan yang hilang — urutan kronologis lengkap.")


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
    # gunakan %s agar tidak crash saat target_year=None
    logger.info("[EXTRACT] Crawling: %s  (target year: %s)", source_url, target_year)
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

    logger.info("[EXTRACT] Ditemukan %d link PDF untuk tahun %s.", len(pdf_entries), target_year)
    return pdf_entries


def download_pdfs(
    pdf_entries: list[dict],
    dest_dir: Path,
    session,
    delay: float = DOWNLOAD_DELAY,
) -> list[Path]:
    # Mengunduh setiap PDF ke folder tujuan; lewati jika sudah ada di lokal
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

        # [SKIP] Jangan unduh ulang jika PDF sudah ada di disk
        if dest_path.exists():
            size_kb = dest_path.stat().st_size // 1024
            logger.info(
                "[EXISTS] [%d/%d] PDF sudah ada, dilewati: %s (%d KB)",
                idx, total, filename, size_kb,
            )
            downloaded.append(dest_path)
            continue

        logger.info("[DOWNLOAD] [%d/%d] Mengunduh -> %s", idx, total, filename)
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
    latest_month: Optional[tuple[int, int]] = None,
) -> list[dict]:
    """
    Ekstrak teks dari semua PDF di pdf_dir.

    Perilaku inkremental:
    - [OCR SKIP] Jika file .txt sudah ada -> langsung baca dari disk, skip ekstraksi
    - [NEW]      Jika (year, month) file > latest_month -> proses sebagai data baru
    - [SKIP]     Jika (year, month) file <= latest_month DAN .txt sudah ada -> lewati
    """
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
    logger.info(
        "[TRANSFORM] %d PDF ditemukan. Titik lanjut: %s",
        len(pdf_files),
        "%04d-%02d" % latest_month if latest_month else "(mulai dari awal)",
    )
    logger.info("=" * 60)

    results: list[dict] = []
    skipped_old = 0

    for pdf_path in pdf_files:
        txt_path = output_dir / (pdf_path.stem + ".txt")

        # Tentukan (year, month) dari nama file untuk keputusan inkremental
        file_date_str = _parse_date_from_any_filename(pdf_path.name)
        file_ym: Optional[tuple[int, int]] = None
        if file_date_str:
            try:
                fy, fm = int(file_date_str[:4]), int(file_date_str[5:7])
                file_ym = (fy, fm)
            except (ValueError, IndexError):
                pass

        # [OCR SKIP] TXT sudah ada -> baca langsung dari disk, tidak perlu ekstraksi ulang
        if txt_path.exists():
            text = txt_path.read_text(encoding="utf-8", errors="replace")

            # Jika data sudah di-cover oleh dataset sebelumnya, catat sebagai skip
            if latest_month and file_ym and file_ym <= latest_month:
                logger.debug(
                    "[SKIP] %s sudah ada di dataset (%04d-%02d <= %04d-%02d)",
                    pdf_path.name, file_ym[0], file_ym[1],
                    latest_month[0], latest_month[1],
                )
                skipped_old += 1
            else:
                logger.info("[OCR SKIP] TXT sudah ada, baca dari disk: %s", txt_path.name)

            results.append({
                "pdf_name": pdf_path.name,
                "pdf_path": str(pdf_path),
                "txt_path": str(txt_path),
                "text":     text,
                "has_text": bool(text.strip()),
            })
            continue

        # [NEW] File belum pernah diekstrak -> proses sekarang
        if file_ym:
            logger.info(
                "[NEW] Bulan baru terdeteksi: %04d-%02d — memproses %s",
                file_ym[0], file_ym[1], pdf_path.name,
            )
        else:
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

        txt_path.write_text(text, encoding="utf-8")
        logger.info("          -> %s", txt_path.name)

        results.append({
            "pdf_name": pdf_path.name,
            "pdf_path": str(pdf_path),
            "txt_path": str(txt_path),
            "text":     text,
            "has_text": bool(text),
        })

    if skipped_old:
        logger.info("[SKIP] %d file lama dilewati (sudah ada di dataset).", skipped_old)
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
    """
    Bangun atau perbarui dataset.csv secara inkremental.

    Perilaku:
    - Jika dataset.csv sudah ada -> muat, gabungkan dengan data baru, dedup
    - Jika belum ada -> buat baru dari nol
    - TIDAK pernah menghapus baris lama yang sudah valid
    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("Run: pip install pandas")

    sys.path.insert(0, str(_SRC_DIR))
    from utils.text_parsing import parse_icp_price

    logger.info("=" * 60)
    logger.info("[LOAD] Membangun dataset CSV (target %s) ...", target_year)
    logger.info("=" * 60)

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    new_records: list[dict] = []

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
        if target_year is not None and year_val != target_year:
            continue

        month_val = int(date_str[5:7])
        logger.info("  %s -> month=%d  price=%s", pdf_name, month_val, price)
        new_records.append({"month": month_val, "year": year_val, "icp_price": price})

    df_new = pd.DataFrame(new_records, columns=["month", "year", "icp_price"])

    # Muat dataset lama jika sudah ada, lalu gabungkan — jaga data historis
    if csv_path.exists():
        try:
            df_old = pd.read_csv(csv_path)
            rows_before = len(df_old)
            df_combined = pd.concat([df_old, df_new], ignore_index=True)
            logger.info("[LOAD] Menggabungkan %d baris lama + %d baris baru.", rows_before, len(df_new))
        except Exception as exc:
            logger.warning("[WARNING] Gagal membaca CSV lama (%s) — hanya pakai data baru.", exc)
            df_combined = df_new
    else:
        logger.info("[LOAD] Dataset baru dibuat dari nol.")
        df_combined = df_new

    # Dedup: pertahankan baris dengan icp_price yang tidak null jika ada pilihan
    df_combined["icp_price"] = pd.to_numeric(df_combined["icp_price"], errors="coerce")
    df_combined = df_combined.sort_values(
        ["year", "month", "icp_price"], na_position="last"
    )
    df_combined = df_combined.drop_duplicates(subset=["year", "month"], keep="first")
    df_combined = df_combined.sort_values(["year", "month"]).reset_index(drop=True)

    # Interpolasi harga yang masih kosong
    df_combined["icp_price"] = (
        df_combined["icp_price"]
        .interpolate(method="linear")
        .ffill()
        .bfill()
        .round(2)
    )

    df_combined.to_csv(csv_path, index=False)

    logger.info("=" * 60)
    logger.info("[LOAD] Dataset tersimpan: %d record -> %s", len(df_combined), csv_path)
    logger.info("=" * 60)

    print("\n" + "=" * 60)
    print("DATASET ({}) -- {} record".format(target_year, len(df_combined)))
    print("=" * 60)
    print(df_combined.to_string(index=False))
    print("=" * 60)
    print("=" * 60)


def run_local(
    raw_pdf_dir:  Path = RAW_PDF_DIR,
    processed_dir: Path = PROCESSED_DIR,
    target_year:  int   = TARGET_YEAR,  # None = semua tahun
) -> None:
    raw_pdf_dir   = Path(raw_pdf_dir)
    processed_dir = Path(processed_dir)
    csv_path      = raw_pdf_dir / "dataset.csv"

    raw_pdf_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    # Deteksi titik lanjut agar PDF/TXT lama tidak diproses ulang
    latest_month = detect_latest_month(csv_path)

    logger.info("=" * 60)
    logger.info("[LOCAL] Ekstrak dari PDF yang sudah ada di %s", raw_pdf_dir)
    logger.info("=" * 60)

    extraction_results = extract_text_from_pdfs(
        raw_pdf_dir, processed_dir, latest_month=latest_month
    )

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
    target_year:  int   = TARGET_YEAR,  # None = semua tahun
) -> None:
    raw_pdf_dir   = Path(raw_pdf_dir)
    processed_dir = Path(processed_dir)
    csv_path      = raw_pdf_dir / "dataset.csv"

    raw_pdf_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    # Deteksi titik lanjut sebelum mulai unduh — hindari kerja ulang
    latest_month = detect_latest_month(csv_path)

    session = _get_session()

    pdf_entries = collect_pdf_links(source_url, session, target_year=target_year)

    if not pdf_entries:
        logger.error("[EXTRACT] Tidak ada link PDF ditemukan untuk tahun %s.", target_year)
        return

    logger.info("=" * 60)
    logger.info("[EXTRACT] Mengunduh %d PDF (tahun %s) ...", len(pdf_entries), target_year)
    logger.info("=" * 60)
    downloaded = download_pdfs(pdf_entries, raw_pdf_dir, session, delay)
    logger.info("[EXTRACT] %d PDF berhasil diunduh.", len(downloaded))

    if not downloaded:
        logger.error("[EXTRACT] Tidak ada PDF yang diunduh.")
        return

    extraction_results = extract_text_from_pdfs(
        raw_pdf_dir, processed_dir, latest_month=latest_month
    )
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
    parser.add_argument("--year",        type=int,   default=TARGET_YEAR,    help="Tahun target (kosongkan = semua tahun)")
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
