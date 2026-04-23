# conftest.py — root-level pytest configuration
# Tambahkan src/ ke sys.path agar semua test dapat mengimpor modul proyek
import sys
from pathlib import Path

# Pastikan src/ selalu ada di path saat pytest dijalankan
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
