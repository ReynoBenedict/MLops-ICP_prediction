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
├── .dvc/               # Direktori konfigurasi DVC (dihasilkan otomatis oleh `dvc init`)
├── .dvcignore          # File daftar pola yang dikecualikan dari pelacakan DVC
├── .gitignore          # File daftar pola yang dikecualikan dari Git
├── configs/
│   └── config.yaml         # Konfigurasi pipeline (path data, parameter ingestion)
├── data/
│   ├── raw/                # Data mentah: PDF laporan ICP dan dataset yang dilacak DVC
│   │   ├── dataset.csv         # Dataset utama hasil ingestion (dilacak oleh DVC)
│   │   └── dataset.csv.dvc     # File metadata DVC untuk dataset.csv
│   └── processed/          # Output dataset hasil preprocessing (CSV)
├── notebooks/
│   ├── 01_eda.ipynb            # Notebook EDA utama
│   └── icp_exploration.ipynb   # Notebook eksplorasi tambahan
├── src/
│   ├── data_processing/
│   │   └── run_ingestion.py    # Script ingestion utama (jalankan dengan flag --local)
│   ├── utils/
│   │   └── text_parsing.py     # Fungsi parsing teks dari PDF
│   ├── data_loader.py          # Modul pemuatan dataset ke DataFrame
│   └── preprocessing.py        # Modul preprocessing (normalisasi, missing value)
├── tests/
│   ├── test_data_loader.py     # Unit test untuk data_loader
│   └── test_preprocessing.py   # Unit test untuk preprocessing
├── requirements.txt        # Daftar dependency Python
└── README.md               # Dokumentasi proyek
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

---

## 3. Data Versioning Menggunakan DVC (LK-05)

Pada tahap ini dilakukan implementasi **Data Version Control (DVC)** untuk mengelola dan melacak perubahan dataset secara terstruktur dalam pipeline MLOps. Fokus utama adalah mendokumentasikan **alur penambahan versi data (data versioning workflow)**.

---

### 3.1 Alur Penambahan Versi Data

Proses versioning dataset dalam proyek ini dilakukan melalui tahapan berikut:

1. **Inisialisasi DVC**
   Repository diinisialisasi menggunakan DVC untuk memungkinkan pelacakan dataset berbasis hash.

2. **Tracking Dataset Awal (Versi 1)**
   Dataset awal dihasilkan dari proses ingestion data ICP tahun 2019 dan disimpan dalam format CSV. Dataset ini kemudian dilacak menggunakan DVC sebagai versi pertama (*version 1*).

3. **Penambahan Data Baru (Simulasi Continual Learning)**
   Dataset diperbarui dengan menambahkan data baru (tahun 2020) melalui proses ingestion ulang. Tahap ini merepresentasikan skenario *continual learning*, di mana data bertambah seiring waktu.

4. **Versioning Dataset (Versi 2)**
   Dataset yang telah diperbarui kembali dilacak menggunakan DVC, sehingga menghasilkan versi kedua (*version 2*) dengan hash yang berbeda.

5. **Audit dan Perbandingan Versi Data**
   Perbedaan antara dataset versi lama dan versi baru dianalisis menggunakan fitur `dvc diff` untuk memastikan bahwa perubahan data telah tercatat dengan baik.

---

### 3.2 Implementasi Teknis

Berikut adalah perintah yang digunakan dalam setiap tahapan:

#### a. Inisialisasi DVC

```bash
dvc init
git add .dvc .gitignore
git commit -m "init DVC"
```

#### b. Tracking Dataset Versi 1 (2019)

```bash
dvc add data/raw/dataset.csv
git add data/raw/dataset.csv.dvc
git commit -m "dataset v1 (2019)"
```

#### c. Penambahan Data Baru

```bash
python src/data_processing/run_ingestion.py --local
```

#### d. Tracking Dataset Versi 2 (2019 + 2020)

```bash
dvc add data/raw/dataset.csv
git add data/raw/dataset.csv.dvc
git commit -m "dataset v2 (add 2020)"
```

#### e. Perbandingan Versi Dataset

```bash
dvc diff HEAD~1 HEAD
```

---

### 3.3 Hasil Versioning

Hasil dari proses `dvc diff` menunjukkan bahwa dataset mengalami perubahan:

```text
Modified:
    data/raw/dataset.csv
```

Perubahan ini mengindikasikan bahwa:

- Dataset telah diperbarui dengan data baru
- DVC berhasil mendeteksi perubahan melalui perbedaan nilai hash
- Riwayat perubahan dataset tersimpan dengan baik dalam repository

---

### 3.4 Kesimpulan

Implementasi DVC dalam proyek ini berhasil mendukung proses versioning dataset secara sistematis. Alur penambahan versi data dimulai dari pembuatan dataset awal, dilanjutkan dengan penambahan data baru, hingga pelacakan perubahan antar versi menggunakan DVC.

Pendekatan ini memungkinkan:

- Pengelolaan dataset secara efisien
- Pelacakan perubahan data secara transparan
- Mendukung prinsip reproducibility dalam MLOps

---

## 4. Manajemen Eksperimen dan Pelacakan Metrik dengan MLflow (LK-06)

Pada tahap ini dilakukan integrasi **MLflow** ke dalam proses pelatihan model untuk mencatat, melacak, dan membandingkan eksperimen secara sistematis.

---

### 4.1 Variasi Eksperimen

Eksperimen dilakukan dengan beberapa variasi model sebagai berikut:

- **Linear Regression** — model baseline tanpa hyperparameter tambahan
- **Random Forest** dengan variasi `n_estimators`:
  - `n_estimators = 10`
  - `n_estimators = 50`
  - `n_estimators = 100`

Setiap run MLflow mencatat:

- **Parameter**: `model_type`, `n_estimators`
- **Metrik evaluasi**: RMSE (*Root Mean Squared Error*) dan MAE (*Mean Absolute Error*)
- **Artefak**: visualisasi perbandingan nilai aktual dan prediksi dalam format PNG

---

### 4.2 Hasil Eksperimen

Berikut adalah rangkuman hasil evaluasi dari seluruh eksperimen:

| Model | RMSE | MAE |
|---|---|---|
| LinearRegression | 18.54 | 17.61 |
| RandomForest (n=10) | 8.72 | 8.24 |
| **RandomForest (n=50)** | **7.14** | **6.90** |
| RandomForest (n=100) | 7.78 | 7.50 |

Berdasarkan hasil di atas, **Random Forest dengan `n_estimators = 50`** memberikan performa terbaik. Penambahan estimator hingga 100 tidak menghasilkan peningkatan yang signifikan, sementara Linear Regression menunjukkan performa terendah.

Nilai error yang relatif tinggi dipengaruhi oleh beberapa faktor, antara lain:

- Jumlah data yang terbatas
- Fluktuasi harga yang ekstrem pada periode tertentu
- Penggunaan fitur yang masih sederhana (hanya `lag_1`)

---

### 4.3 Model Terpilih

Model yang dipilih pada tahap ini adalah **Random Forest (`n_estimators = 50`)** karena memberikan hasil paling optimal berdasarkan evaluasi kuantitatif.

Penggunaan model yang lebih kompleks seperti ARIMAX dan LSTM belum dilakukan pada tahap ini karena keterbatasan jumlah data yang dapat menyebabkan *overfitting* dan hasil yang tidak stabil. Model tersebut direncanakan untuk diimplementasikan pada tahap selanjutnya ketika data yang tersedia lebih memadai.

---

### 4.4 Cara Menjalankan Pipeline

#### a. Persiapan Data

```bash
python prepare_data.py
```

#### b. Menjalankan Training dan Pencatatan Eksperimen

```bash
python train.py
```

#### c. Membuka MLflow UI

```bash
mlflow ui --backend-store-uri sqlite:///mlflow.db --port 5000 --workers 1
```

Lalu buka browser dan akses: `http://127.0.0.1:5000`

---

### 4.5 Kesimpulan

Integrasi MLflow berhasil mendukung proses pelacakan eksperimen secara sistematis. Setiap run tercatat dengan lengkap beserta parameter, metrik, dan artefak visualisasi, sehingga memudahkan perbandingan antar model.

Pendekatan ini memungkinkan:

- Pelacakan eksperimen secara transparan dan reproducible
- Perbandingan model yang terstruktur berdasarkan metrik kuantitatif
- Fondasi yang kuat untuk pengembangan model lebih lanjut dalam pipeline MLOps

---

## 5. Model Registry and Inference (LK-07)

Model **ICP_Price_Model** versi 4 (*n_estimators = 100*) saat ini berada pada stage **Production** di MLflow Model Registry. Meskipun versi 3 (*n_estimators = 50*) menghasilkan RMSE yang lebih rendah (7.14 vs 7.78), versi 4 dipromosikan ke Production untuk mensimulasikan alur *lifecycle* model secara penuh dalam MLflow—mencakup proses registrasi, versioning bertahap, dan stage transition (*None → Staging → Production*). Versi 3 dipertahankan pada stage **Staging** sebagai kandidat model.

| Version | `n_estimators` | Stage | RMSE |
|---------|---------------|-------|------|
| v3 | 50 | Staging | 7.14 |
| **v4** | **100** | **Production** | **7.78** |

Proses inferensi dijalankan melalui perintah:

```bash
python infer.py
```

Model dimuat secara langsung dari MLflow Model Registry menggunakan URI `models:/ICP_Price_Model/Production` melalui fungsi `mlflow.pyfunc.load_model`, tanpa bergantung pada file model lokal. Pendekatan ini memastikan sistem inferensi selalu menggunakan versi model yang aktif di Production secara otomatis.

Penggunaan model berbasis *time series* seperti ARIMAX atau LSTM belum diterapkan pada tahap ini mengingat keterbatasan jumlah data (±18 bulan), yang berpotensi mengakibatkan *overfitting* pada model temporal yang lebih kompleks.

---

## 6. [LK-09] Orkestrasi Layanan ML Terintegrasi Menggunakan Docker Compose

Seluruh layanan pada sistem MLOps dijalankan menggunakan Docker Compose sehingga seluruh container dapat di-*orchestrate* secara otomatis dalam satu environment.

---

### 6.1 Menjalankan Sistem dengan Docker Compose

Untuk menjalankan seluruh sistem:

```bash
docker compose up -d
```

Perintah tersebut akan menjalankan:

- **Container Streamlit** sebagai aplikasi inference
- **Container MLflow Tracking Server**
- **Container PostgreSQL** sebagai backend metadata store

---

### 6.2 Memeriksa Status Container

Gunakan perintah berikut untuk memastikan seluruh container berjalan dengan baik:

```bash
docker compose ps
```

Container yang berjalan dengan benar akan memiliki status `running` or `healthy`.

---

### 6.3 Akses Service

Setelah seluruh container berjalan, layanan dapat diakses melalui:

| Service | URL |
| :--- | :--- |
| Streamlit App | http://localhost:8501 |
| MLflow Tracking Server | http://localhost:5000 |

---

### 6.4 Hasil Orkestrasi

Docker Compose berhasil mengintegrasikan layanan Streamlit, MLflow, dan PostgreSQL dalam satu environment terorkestrasi. Aplikasi Streamlit berhasil melakukan inference model menggunakan model yang diambil dari MLflow Model Registry melalui komunikasi antar container.

---

## 7. [LK-10] FastAPI Model Serving & Horizontal Scaling

### 7.1 Mengakses Endpoint API

Pastikan seluruh layanan sudah berjalan:

```bash
docker compose up -d
```

Endpoint yang tersedia:

| Method | Endpoint | Fungsi |
|---------|-----------|---------|
| GET | `/health` | Memeriksa status layanan model serving |
| POST | `/predict` | Mengirim data inferensi untuk prediksi harga ICP |
| GET | `/docs` | Swagger API documentation |

Base URL:

```text
http://localhost:8000
```

Contoh pengecekan health endpoint:

```bash
curl http://localhost:8000/health
```

Contoh request prediksi:

```bash
curl -X POST http://localhost:8000/predict \
-H "Content-Type: application/json" \
-d '{
"wti_price":72,
"wti_lag_1":71,
"wti_rolling_mean_3":71.5,
"lag_1":70,
"lag_3":68,
"lag_6":65,
"rolling_mean_3":69
}'
```

Alternatif pengujian menggunakan Postman:

1. Buat request `GET`
```
http://localhost:8000/health
```

2. Buat request `POST`
```
http://localhost:8000/predict
```

Body → Raw → JSON:

```json
{
  "wti_price":72,
  "wti_lag_1":71,
  "wti_rolling_mean_3":71.5,
  "lag_1":70,
  "lag_3":68,
  "lag_6":65,
  "rolling_mean_3":69
}
```

---

### 7.2 Menambah Jumlah Replika Secara Dinamis

Melakukan horizontal scaling layanan model serving:

```bash
docker compose up -d --scale model-api=3
```

Memverifikasi replika yang berjalan:

```bash
docker compose ps
```

Output akan menunjukkan beberapa instance container:

```
model-api-1
model-api-2
model-api-3
```

Menambah replika lain secara dinamis:

```bash
docker compose up -d --scale model-api=5
```

Docker Compose akan membuat instance tambahan tanpa mengubah layanan lain yang sedang berjalan.