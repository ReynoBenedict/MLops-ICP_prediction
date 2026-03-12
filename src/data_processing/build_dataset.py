# build_dataset.py — Compile extracted PDF records into icp_dataset.csv
from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_SRC_DIR      = Path(__file__).resolve().parents[1]
_PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_PDF_DIR   = _PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = _PROJECT_ROOT / "data" / "processed"
DATASET_CSV   = PROCESSED_DIR / "icp_dataset.csv"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_existing(csv_path: Path) -> pd.DataFrame:
    if csv_path.exists():
        try:
            return pd.read_csv(
                csv_path,
                dtype={"date": str, "icp_price": float, "source_document": str},
            )
        except Exception as exc:
            logger.warning("Could not read existing CSV (%s) — starting fresh.", exc)
    return pd.DataFrame(columns=["date", "icp_price", "source_document"])


def _merge_and_clean(existing: pd.DataFrame, new_records: list[dict]) -> pd.DataFrame:
    if not new_records:
        return existing
    new_df   = pd.DataFrame(new_records, columns=["date", "icp_price", "source_document"])
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset="date", keep="last")   # prefer newest
    combined = combined.sort_values("date").reset_index(drop=True)
    return combined


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def update_dataset(new_records: list[dict], csv_path: str | Path) -> pd.DataFrame:
    # Memperbarui isi dataset CSV dengan array record baru. 
    # Masing-masing record harus memiliki date, icp_price, source_document.
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    existing = _load_existing(csv_path)
    merged   = _merge_and_clean(existing, new_records)
    merged.to_csv(csv_path, index=False)
    logger.info("[LOAD] Dataset saved: %d rows → '%s'.", len(merged), csv_path)
    return merged


def build_dataset_from_dir(
    raw_dir:  str | Path = RAW_PDF_DIR,
    csv_path: str | Path = DATASET_CSV,
    processed_dir: str | Path = PROCESSED_DIR,
) -> pd.DataFrame:
    # Memproses pembentukan dataset komplit: dari mulai ekstrak teks opsi raw sampai ke penulisan CSV.
    if str(_SRC_DIR) not in sys.path:
        sys.path.insert(0, str(_SRC_DIR))

    from data_processing.extract_icp import extract_all_pdfs

    logger.info("[BUILD] Extracting records from PDFs in '%s' ...", raw_dir)
    records = extract_all_pdfs(
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        save_txt=True,
    )
    return update_dataset(records, csv_path)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    if str(_SRC_DIR) not in sys.path:
        sys.path.insert(0, str(_SRC_DIR))
    df = build_dataset_from_dir(
        raw_dir=RAW_PDF_DIR,
        csv_path=DATASET_CSV,
        processed_dir=PROCESSED_DIR,
    )
    print(df.to_string(index=False))
