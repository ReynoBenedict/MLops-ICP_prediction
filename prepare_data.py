# Membuat fitur lag_1 dari dataset mentah dan menyimpan ke data/processed/clean_data.csv
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent
RAW_CSV = PROJECT_ROOT / "data" / "raw" / "dataset.csv"
OUT_CSV = PROJECT_ROOT / "data" / "processed" / "clean_data.csv"

CANDIDATE_TARGET_COLS = ["icp_price", "icp", "price", "harga"]


def detect_target_column(df: pd.DataFrame) -> str:
    """Deteksi nama kolom target secara otomatis."""
    for col in CANDIDATE_TARGET_COLS:
        if col in df.columns:
            return col
    numeric_cols = df.select_dtypes("number").columns.tolist()
    skip = {"month", "year", "bulan", "tahun"}
    candidates = [c for c in numeric_cols if c.lower() not in skip]
    if candidates:
        return candidates[0]
    raise ValueError(f"Kolom target tidak ditemukan. Kolom tersedia: {list(df.columns)}")


def prepare(raw_path: Path = RAW_CSV, out_path: Path = OUT_CSV) -> Path:
    if not raw_path.exists():
        print(f"[ERROR] File dataset tidak ditemukan: {raw_path}")
        print("        Pastikan pipeline ingestion sudah dijalankan terlebih dahulu.")
        sys.exit(1)

    try:
        df = pd.read_csv(raw_path)
    except Exception as exc:
        print(f"[ERROR] Gagal membaca {raw_path}: {exc}")
        sys.exit(1)

    print(f"[INFO] Memuat {len(df)} baris dari {raw_path}")
    print(f"[INFO] Kolom tersedia: {list(df.columns)}")

    try:
        target_col = detect_target_column(df)
    except ValueError as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)

    print(f"[INFO] Kolom target yang digunakan: '{target_col}'")

    # Buat fitur lag_1 lalu hapus baris NA hasil shift
    df = df.copy()
    df["lag_1"] = df[target_col].shift(1)

    before = len(df)
    df = df.dropna().reset_index(drop=True)
    after = len(df)
    print(f"[INFO] Baris setelah drop NA: {after} (dihapus {before - after} baris)")

    if after < 2:
        print("[ERROR] Data terlalu sedikit setelah drop NA. Minimal 2 baris dibutuhkan.")
        sys.exit(1)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"[OK]   clean_data.csv disimpan ke: {out_path}")
    print(f"[INFO] Preview:\n{df.head()}")

    return out_path


if __name__ == "__main__":
    prepare()
