# Ketentuan layanan, privasi, dan atribusi (ringkasan dokumen produk)

*Dokumen ini merangkum ketentuan yang ditampilkan di dalam aplikasi (halaman Syarat Penggunaan dan
Kebijakan Privasi) agar dapat ditinjau oleh penasihat hukum sebelum peluncuran berbayar. Belum
ditinjau oleh penasihat hukum. Entitas hukum penyelenggara belum ditetapkan; teks menggunakan
"Operator layanan ICP Intelligence".*

## Syarat penggunaan (pokok-pokok)

1. **Sifat layanan.** ICP Intelligence menyajikan riwayat harga resmi, data acuan publik, estimasi
   dan proyeksi berbasis metode statistik, insight otomatis, laporan dan pemberitahuan. Layanan
   **bukan** nasihat investasi, bukan penetapan harga resmi, dan bukan pengganti keputusan
   Kementerian ESDM.
2. **Akurasi dan keterbatasan.** Estimasi dan proyeksi memiliki kesalahan yang diukur dan
   ditampilkan (rentang ketidakpastian empiris). Proyeksi bulan mendatang bersyarat pada asumsi
   harga acuan. Operator tidak menjamin hasil keputusan bisnis yang didasarkan pada layanan.
3. **Akun dan organisasi.** Pengguna bertanggung jawab atas kerahasiaan kata sandi, token sesi dan
   kunci API. Administrator organisasi mengelola anggota; pemilik bertanggung jawab atas penggunaan
   oleh organisasinya. Paket dan batasnya ditetapkan oleh operator (belum ada pembayaran daring).
4. **Penggunaan yang dilarang.** Mengakali batas paket, membagi kunci API ke pihak ketiga,
   mengambil data secara massal untuk dijual kembali sebagai data mentah, atau menyajikan keluaran
   layanan sebagai angka resmi pemerintah.
5. **Data pihak ketiga.** Nilai ICP bersumber dari dokumen Kementerian ESDM; harga acuan dari U.S.
   EIA (domain publik) dan World Bank Pink Sheet (CC BY 4.0); kurs dari Bank Indonesia (JISDOR).
   Pengguna wajib mematuhi ketentuan sumber tersebut bila mendistribusikan ulang (lihat
   `DATA_SOURCES.md`).
6. **Perubahan dan penghentian.** Operator dapat mengubah fitur, paket, atau menangguhkan akun yang
   melanggar ketentuan, dengan pemberitahuan di dalam aplikasi.

## Kebijakan privasi (pokok-pokok)

| Hal | Praktik aktual dalam kode |
|---|---|
| Data pribadi yang disimpan | alamat email, nama tampilan, peran, waktu login terakhir, *user-agent* sesi |
| Kata sandi | disimpan sebagai *hash* scrypt (garam 16 byte); tidak pernah dapat dibaca kembali |
| Token sesi / kunci API | hanya *hash* SHA-256 yang disimpan; token ditampilkan sekali |
| Log dan audit | tindakan sensitif dicatat (aktor, aksi, waktu, ID permintaan); tanpa kata sandi, token, atau alamat IP mentah |
| Pembayaran | tidak ada data pembayaran yang diproses (belum ada integrasi penyedia pembayaran) |
| Penyimpanan peramban | token sesi di `sessionStorage` (per tab); preferensi bahasa di `localStorage` |
| Retensi | sesi kedaluwarsa 30 hari; catatan audit disimpan untuk keperluan keamanan; penghapusan akun atas permintaan dilakukan oleh operator (belum otomatis) |
| Berbagi data | data organisasi tidak pernah ditampilkan ke organisasi lain (diuji otomatis); tidak ada pihak ketiga pemroses kecuali infrastruktur hosting yang dipilih saat penerapan |
| Lokasi data | lokal selama pengembangan; saat penerapan: Cloudflare D1/R2 dan basis data PostgreSQL penyelenggara (lihat `DEPLOYMENT.md`) |

Kontak permintaan akses/penghapusan data: melalui operator layanan (alamat kontak ditetapkan saat
peluncuran).

## Atribusi

- Data: Kementerian ESDM Republik Indonesia (ICP resmi); U.S. Energy Information Administration
  (WTI, Brent); World Bank Commodity Price Data – The Pink Sheet (Dubai), CC BY 4.0; Bank Indonesia
  (JISDOR).
- Foto: Nafisathallah (CC BY 4.0); Sabung.hamster (CC BY-SA 4.0); Andrew Thomas (CC BY-SA 2.0) – via
  Wikimedia Commons. Detail di `ASSET_INVENTORY.md`.
- Perangkat lunak sumber terbuka sesuai lisensi masing-masing (lihat `pyproject.toml`,
  `package.json`).
