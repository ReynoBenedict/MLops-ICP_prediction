# Membuat fitur temporal ICP + WTI dan menyimpan ke data/processed/clean_data.csv
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_CSV     = PROJECT_ROOT / "data" / "raw" / "dataset.csv"
WTI_CSV     = PROJECT_ROOT / "data" / "raw" / "wti.csv"
OUT_CSV     = PROJECT_ROOT / "data" / "processed" / "clean_data.csv"

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


def _load_wti(wti_path: Path, icp_min: str, icp_max: str) -> pd.DataFrame:
    """Load wti.csv and crop to ICP date range. Returns df with col 'date' (YYYY-MM)."""
    if not wti_path.exists():
        print(f"[WARNING] wti.csv not found at {wti_path} — skipping WTI merge.")
        return pd.DataFrame()

    wti = pd.read_csv(wti_path)
    wti["date"] = wti["date"].astype(str).str.strip()

    # Crop to ICP range — removes historical data before 2019 and future leakage
    wti = wti[(wti["date"] >= icp_min) & (wti["date"] <= icp_max)].copy()
    wti = wti.sort_values("date").reset_index(drop=True)

    # Lightweight validation
    dupes = wti.duplicated("date").sum()
    if dupes:
        print(f"[WARNING] WTI has {dupes} duplicate dates — keeping first.")
        wti = wti.drop_duplicates("date", keep="first")
    nulls = wti["wti_price"].isna().sum()
    if nulls:
        print(f"[WARNING] WTI has {nulls} NULL prices after crop.")

    print(f"[INFO] WTI cropped to {len(wti)} rows ({icp_min} -> {icp_max})")
    return wti


def prepare(raw_path: Path = RAW_CSV, wti_path: Path = WTI_CSV, out_path: Path = OUT_CSV) -> Path:
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

    # Pastikan urutan kronologis sebelum membuat fitur temporal
    df = df.copy()
    if "year" in df.columns and "month" in df.columns:
        df = df.sort_values(["year", "month"]).reset_index(drop=True)

    # Build YYYY-MM merge key from ICP year/month columns
    df["date"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)

    # Merge WTI if available
    icp_min, icp_max = df["date"].min(), df["date"].max()
    wti = _load_wti(wti_path, icp_min, icp_max)

    if not wti.empty:
        before = len(df)
        df = df.merge(wti[["date", "wti_price"]], on="date", how="inner")
        after = len(df)
        if before != after:
            print(f"[WARNING] Inner join dropped {before - after} ICP rows with no WTI match.")
        print(f"[INFO] Merged ICP + WTI: {after} rows")

        # WTI features — shift(1) ensures no leakage (only past data used)
        df["wti_lag_1"]          = df["wti_price"].shift(1)
        df["wti_rolling_mean_3"] = df["wti_price"].shift(1).rolling(window=3, min_periods=1).mean()
    else:
        print("[INFO] Running without WTI features.")

    # Drop the date helper column — not needed downstream
    df = df.drop(columns=["date"], errors="ignore")

    # Buat fitur lag — hanya menggunakan data masa lalu (tidak ada data leakage)
    df["lag_1"] = df[target_col].shift(1)
    df["lag_3"] = df[target_col].shift(3)
    df["lag_6"] = df[target_col].shift(6)

    # Rolling mean 3 bulan — geser dulu agar hanya pakai data masa lalu
    df["rolling_mean_3"] = df[target_col].shift(1).rolling(window=3, min_periods=1).mean()

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
