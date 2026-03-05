# Merge extracted PDF records into dataset.
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def _load_existing(csv_path: Path) -> pd.DataFrame:
    if csv_path.exists():
        try:
            return pd.read_csv(csv_path, dtype={"date": str, "icp_price": float})
        except Exception as exc:
            logger.warning("Could not read existing CSV (%s) — starting fresh.", exc)
    return pd.DataFrame(columns=["date", "icp_price"])


def _merge_and_clean(existing: pd.DataFrame, new_records: list[dict]) -> pd.DataFrame:
    if not new_records:
        return existing
    new_df = pd.DataFrame(new_records, columns=["date", "icp_price"])
    combined = pd.concat([existing, new_df], ignore_index=True)
    # Deduplicate and drop NaNs
    combined = combined.drop_duplicates(subset="date", keep="first")
    combined = combined.dropna(subset=["icp_price"]).reset_index(drop=True)
    return combined.sort_values("date").reset_index(drop=True)


def update_dataset(new_records: list[dict], csv_path: str | Path) -> pd.DataFrame:
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    existing = _load_existing(csv_path)
    merged = _merge_and_clean(existing, new_records)
    merged.to_csv(csv_path, index=False)
    logger.info("Dataset saved: %d rows → '%s'.", len(merged), csv_path)
    return merged


def build_dataset_from_dir(raw_dir: str | Path, csv_path: str | Path) -> pd.DataFrame:
    import sys
    _src = Path(__file__).resolve().parents[1]
    if str(_src) not in sys.path:
        sys.path.insert(0, str(_src))
    from data_processing.extract_icp import extract_all_pdfs
    records = extract_all_pdfs(raw_dir)
    return update_dataset(records, csv_path)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    _root = Path(__file__).parents[2]
    sys.path.insert(0, str(Path(__file__).parents[1]))
    df = build_dataset_from_dir(
        raw_dir=_root / "data" / "raw",
        csv_path=_root / "data" / "processed" / "icp_dataset.csv",
    )
    print(df.to_string(index=False))
