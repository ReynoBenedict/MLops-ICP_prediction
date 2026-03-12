# Prediksi ICP (Indonesian Crude Price) - MLOps

Proyek ini bertujuan untuk membangun jalur kerja (*pipeline*) **Machine Learning Operations (MLOps)** untuk melakukan peramalan (*forecasting*) harga historis **Indonesian Crude Price (ICP)**.

Pada tahap pengembangan selanjutnya, sistem ini akan mengimplementasikan model **hybrid ARIMAX-LSTM** untuk memodelkan pola data deret waktu (*time series*) harga minyak mentah Indonesia.

Saat ini proyek berada pada tahap **inisialisasi awal (MLOps LK-02)** yang difokuskan pada beberapa aktivitas utama, yaitu:

- Perancangan struktur proyek berbasis praktik MLOps
- Pengumpulan data (*data collection*)
- Analisis data eksploratif (*Exploratory Data Analysis / EDA*)

---

# 1. Struktur Proyek

Struktur direktori proyek dirancang untuk memisahkan berbagai komponen sistem secara modular sehingga proses pengembangan, pemeliharaan, serta pengujian sistem dapat dilakukan dengan lebih terorganisir.

```text
MLops-ICP_prediction/
├── configs/            # Direktori konfigurasi pipeline utama (contoh: config.yaml)
├── data/
│   ├── raw/            # Penyimpanan data mentah (dokumen laporan ICP dalam format PDF)
│   └── processed/      # Dataset hasil ekstraksi dalam format CSV
├── docs/               # Dokumentasi tambahan proyek dan referensi teknis
├── models/             # Penyimpanan artefak model machine learning
├── notebooks/          # Jupyter Notebook untuk eksplorasi data dan eksperimen
├── src/                # Direktori source code utama
│   ├── data_processing/# Script ekstraksi data dan pembuatan dataset
│   ├── utils/          # Fungsi utilitas pendukung (pemrosesan teks, pembacaan PDF, dll)
│   ├── data_loader.py  # Modul untuk memuat dataset ke dalam DataFrame
│   └── preprocessing.py# Modul preprocessing data (missing value handling, normalisasi, dll)
├── tests/              # Unit testing untuk memastikan stabilitas kode
├── requirements.txt    # Daftar dependency Python yang digunakan
└── README.md           # Dokumentasi proyek
```

Struktur ini mengikuti praktik umum dalam pengembangan sistem Machine Learning dan MLOps, sehingga memudahkan pengelolaan data, kode, serta eksperimen model.

## 2. Instruksi Penggunaan Sistem

Bagian ini menjelaskan langkah-langkah untuk menjalankan sistem pada lingkungan lokal (*local environment*).

### A. Persiapan Lingkungan Sistem (Environment Setup)

Untuk menghindari konflik dependency dengan proyek Python lain, disarankan menggunakan virtual environment.

Sistem ini memerlukan Python versi 3.9 atau lebih baru.

Buat virtual environment:

```bash
python -m venv venv
```

Aktifkan virtual environment.

Windows:

```cmd
venv\Scripts\activate
```

Linux / MacOS:

```bash
source venv/bin/activate
```

Setelah environment aktif, instal seluruh dependency proyek melalui perintah berikut pada root directory proyek:

```bash
pip install -r requirements.txt
```

Perintah ini akan menginstal berbagai library yang dibutuhkan seperti pandas, numpy, scikit-learn, serta library pendukung lainnya.

### B. Konfigurasi Parameter Sistem

Parameter konfigurasi sistem disimpan secara terpusat pada file berikut:

`configs/config.yaml`

File ini berisi beberapa pengaturan penting seperti:

- lokasi direktori data
- parameter preprocessing
- konfigurasi pipeline pemrosesan data
- pengaturan output dataset

Dengan pendekatan ini, perubahan konfigurasi dapat dilakukan tanpa perlu memodifikasi langsung kode pada direktori `src/`.

### C. Ekstraksi dan Pembuatan Dataset (Data Processing)

Tahap ini digunakan untuk mengekstrak data Indonesian Crude Price (ICP) dari laporan bulanan dalam format PDF.

Proses yang dilakukan meliputi:

- Membaca seluruh dokumen PDF pada direktori: `data/raw/`
- Mengekstrak informasi harga ICP dari dokumen tersebut.
- Mengubah data menjadi dataset terstruktur.
- Menyimpan hasil ekstraksi ke dalam direktori: `data/processed/`

Proses ekstraksi dataset dapat dijalankan dengan perintah berikut:

```bash
python src/data_processing/rebuild_dataset.py
```

Script tersebut akan membaca dokumen PDF, mengekstrak informasi yang relevan, dan menghasilkan dataset historis dalam format CSV yang siap digunakan untuk analisis maupun pemodelan.

### D. Exploratory Data Analysis (EDA)

Analisis data eksploratif dilakukan menggunakan Jupyter Notebook yang tersedia pada direktori:

`notebooks/`

Notebook ini digunakan untuk:

- memahami karakteristik data
- menganalisis distribusi data
- mengidentifikasi pola tren pada data time series
- mendeteksi missing values
- membuat visualisasi grafik harga ICP

Notebook dapat dijalankan dengan perintah berikut:

```bash
jupyter notebook notebooks/01_eda.ipynb
```

Selain menggunakan Jupyter Notebook, file ini juga dapat dijalankan melalui Visual Studio Code yang memiliki dukungan bawaan untuk menjalankan notebook secara interaktif.

### E. Pengujian Sistem (Unit Testing)

Untuk memastikan kualitas dan stabilitas kode, proyek ini juga menyediakan unit testing.

Seluruh script pengujian berada pada direktori:

`tests/`

Pengujian dapat dijalankan menggunakan framework pytest dengan perintah berikut:

```bash
pytest tests/
```

Jika seluruh pengujian berhasil dijalankan, terminal akan menampilkan status passed, yang menandakan bahwa fungsi-fungsi utama sistem berjalan dengan baik tanpa error.

## Keterbatasan Saat Ini
Pada tahap LK-02 ini proyek masih berada pada fase inisialisasi, sehingga beberapa komponen sistem belum sepenuhnya lengkap. Dataset ICP yang dihasilkan masih terbatas karena sebagian dokumen PDF bersifat *image-based* sehingga belum dapat diekstrak secara otomatis tanpa implementasi OCR. Selain itu, tahap pemodelan machine learning seperti implementasi model ARIMAX, LSTM, maupun model hybrid ARIMAX–LSTM belum diintegrasikan ke dalam pipeline. Pengembangan selanjutnya akan difokuskan pada penyempurnaan dataset, integrasi variabel eksternal (seperti harga Brent dan nilai tukar USD/IDR), serta pembangunan pipeline pelatihan dan evaluasi model secara penuh.