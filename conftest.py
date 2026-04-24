# Tambahkan src/ ke sys.path agar semua test bisa mengimpor modul proyek
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
