# Laporan UI/UX Enhancement V3 – ICP Intelligence (17 Oktober 2026)

Enhancement terhadap aplikasi yang ada, bukan rewrite. Framework, router (react-router 7,
`BrowserRouter`), API, design system, dan semua halaman dipertahankan; tidak ada library baru.
Tidak ada operasi git, deployment, perubahan secret, dataset, model, atau pipeline.

## 1. Temuan sebelum revisi (audit kode + browser, prioritas)

| Prio | Temuan | Bukti |
|---|---|---|
| P1 | **Logout tidak memberi umpan balik dan tidak berpindah halaman.** Setelah "Keluar" pengguna tetap di route yang sama dengan shell anonim (tombol Masuk/Daftar) tanpa pesan apa pun. | `before/1280-harga-after-logout.png` |
| P1 | **Logout "berpura-pura" berhasil.** `signOut()` menelan semua error: bila server tidak terjangkau, token lokal tetap dihapus walau sesi di server masih aktif. | `auth.tsx` (sebelum) |
| P1 | **Backend offline tampil sebagai "belum ada proyeksi".** `/forecasts/latest` di Ringkasan dan Proyeksi memakai `.catch(() => null)`, sehingga timeout gateway (504) diperlihatkan sebagai kondisi normal. | `OverviewPage`, `ForecastPage` |
| P1 | **Skeleton tak berujung saat API gagal**: KPI "Perubahan bulanan", panel "Pembaruan data terakhir", KPI dan paket di landing page, halaman Akun. Eksplorasi Harga menggambar grafik kosong saat data gagal dimuat. | uji offline, `after/1440-offline-ringkasan.png` (setelah perbaikan) |
| P2 | **Kilasan "Masuk diperlukan"** pada deep link halaman privat saat sesi masih diverifikasi (halaman dirender sebelum `whoami` selesai). | `AccountPage`/`ScenariosPage`/`ReportsPage` |
| P2 | **Riwayat ganda setelah login**: halaman masuk memanggil `navigate(next)` dan sekaligus `<Navigate replace>`, sehingga tombol Back mengulang halaman tujuan. Registrasi memakai `setTimeout` 600 ms tanpa pembersihan dan mengabaikan `next`. | `AuthPages.tsx` |
| P2 | **Sesi kedaluwarsa/dicabut tidak dikenali**: 401 dari request biasa hanya tampil sebagai error lokal; shell tetap menampilkan pengguna sebagai login. | `api/client.ts` |
| P2 | **Arsip skenario dan hapus aturan pemberitahuan tanpa konfirmasi**; hasil perbandingan yang memuat skenario yang diarsipkan tetap tampil. | `ScenariosPage`, `ReportsPage` |
| P2 | **Fokus ke hasil tidak berpindah**: `h2.focus()` dipanggil pada heading tanpa `tabindex`, fokus jatuh ke `BODY` (diukur di Chrome). | cek browser |
| P2 | **Drawer mobile tertutup tetap dapat difokus** dengan keyboard (hanya digeser keluar layar); fokus tidak masuk/kembali saat buka/tutup. | `layout.tsx` |
| P2 | Simpan skenario dan laporan selesai tanpa umpan balik; unduhan laporan tanpa status proses; lebih dari 4 skenario dapat dicentang lalu dipotong diam-diam. | halaman terkait |
| P3 | Tooltip grafik mengikuti kursor dengan transisi 400 ms bawaan Recharts (terasa lamban); semua animasi grafik dimatikan; kartu skenario memeras teks ke kolom sempit. | `before/1440-skenario.png`, cek browser |

Tidak ditemukan P0: route privat sudah dijaga di server (403/401) dan di klien (GateNotice);
`safeNext` sudah menolak redirect lintas origin.

**Kebijakan route (dari desain aktual, bukan asumsi):** `/`, `/masuk`, `/daftar`, `/syarat`,
`/privasi` adalah halaman publik tanpa shell. `/ringkasan`, `/harga`, `/proyeksi`, `/insight`
adalah halaman pasar **publik di dalam shell** (fitur terbatas untuk anonim, sesuai desain produk).
`/skenario`, `/laporan`, `/akun` memerlukan login anggota; `/operasi/*` hanya operator. Jadi `/harga`
setelah logout bukan kebocoran, tetapi alurnya membingungkan dan tidak memberi umpan balik.

## 2. Perbaikan navigasi dan sesi

- **Logout** (`components/layout.tsx`, `auth.tsx`): server dulu, lalu klien. Tombol menampilkan
  progres dan tidak bisa diklik ganda; setelah server mengonfirmasi, token dihapus, pengguna diarahkan
  ke Beranda publik (`replace`) dan toast "Anda telah keluar." muncul. Bila server tidak terjangkau,
  sesi **dipertahankan** dan toast error menjelaskannya. 401 dari server dianggap sesi sudah berakhir.
- **Pembersihan state privat**: isi shell di-*remount* ketika route **atau** principal berubah
  (`key` = jenis principal + organisasi + path), sehingga data privat tidak tertinggal.
- **Gerbang kesiapan**: shell menampilkan skeleton netral sampai server mengonfirmasi sesi (tidak ada
  kilasan "Masuk diperlukan" pada deep link).
- **Sesi kedaluwarsa**: setiap 401 pada request bertoken memicu verifikasi `whoami`; bila memang
  berakhir, sesi lokal dibersihkan dan toast "Sesi Anda telah berakhir" tampil.
- **Login/registrasi**: tidak lagi menambah entri riwayat ganda (pengalihan hanya lewat
  `<Navigate replace>`), deep link `next` dipertahankan juga lewat tautan Daftar ↔ Masuk dan tombol
  Masuk di shell (termasuk query string), toast sambutan, tanpa timer yang bocor.
- **Back setelah logout** membuka halaman privat sebelumnya dalam keadaan anonim (GateNotice), bukan
  data lama.

## 3. Animasi yang diterapkan (CSS saja, tanpa library baru)

| Elemen | Perilaku | Durasi |
|---|---|---|
| Pindah halaman di shell | fade + naik 4 px (sudah ada, dipertahankan); fallback lazy-load tidak beranimasi agar tidak berkedip dua kali | 200 ms |
| Halaman tanpa shell (landing, masuk, daftar, legal) | fade opacity | 200 ms |
| Hasil proyeksi / kartu proyeksi terbit | fade + naik 6 px, fokus ke judul hasil | 220 ms |
| Nilai KPI | fade saat skeleton diganti angka final (tanpa *count-up*, agar tidak ada angka antara yang palsu) | 180 ms |
| Grafik ICP/Brent/Dubai/WTI, proyeksi, skenario | *reveal* kiri→kanan pada garis (clip-path) sekali saat data pertama tampil atau saat dataset berganti (rentang/acuan/set skenario); area interval dan zona proyeksi fade setelahnya. Sumbu dan grid diam; nilai selalu final | 560 ms garis, 420 ms area |
| Tooltip/crosshair | mengikuti kursor tanpa jeda (transisi bawaan Recharts dimatikan) | 0 ms |
| Indikator navigasi aktif | muncul dengan skala vertikal | 200 ms |
| Drawer mobile | geser + scrim fade; fokus masuk ke tombol tutup, Escape menutup, fokus kembali ke tombol menu; drawer tertutup `inert` | 220 ms |
| Accordion (`<details>`) | isi fade + naik saat dibuka | 200 ms |
| Toast | muncul (fade + naik + skala 0,98), keluar fade; tutup otomatis 4,5 s (sukses) / 8 s (error) | 180 / 160 ms |
| Dialog konfirmasi | backdrop fade, kotak pop-in; portal ke `<body>` | 160 / 180 ms |

Semua animasi memakai opacity/transform/clip-path, `fill-mode: backwards` (tidak meninggalkan
transform), tidak menunda navigasi, dan dimatikan oleh `prefers-reduced-motion: reduce`.

## 4. Mikrointeraksi dan feedback

Toast global (live region polite/assertive), `ConfirmDialog` aksesibel (alertdialog, fokus awal di
Batal, Tab terkunci, Escape/klik backdrop membatalkan, fokus kembali ke pemicu, tombol konfirmasi
menunggu jawaban server), arsip skenario dan hapus aturan kini meminta konfirmasi dan baru memperbarui
tampilan setelah server mengonfirmasi, toast untuk simpan/arsip skenario dan laporan selesai, status
proses per tombol unduhan laporan, pencegahan *double submit* pada proyeksi, skenario, laporan, dan
logout, checkbox skenario ke-5 dinonaktifkan (bukan dipotong diam-diam), status `aria-busy` dengan
kursor progress, kartu KPI/proyeksi memberi bayangan saat hover.

## 5. Loading dan error state

Pesan khusus **"Layanan data sedang tidak tersedia"** untuk 502/503/504/52x dan kode gateway
(`UPSTREAM_TIMEOUT_OR_UNAVAILABLE`, `GATEWAY_MISCONFIGURED`, …) yang menyatakan angka tidak
diperbarui dan menawarkan **Coba lagi**; tidak pernah menyatakan layanan normal. `nullOn404()`
membedakan "belum ada" (404) dari "tidak terjangkau". Semua skeleton yang ditemukan kini berhenti
saat request gagal, digantikan error + retry atau "Data belum dapat dimuat"; tombol Unduh CSV di
Eksplorasi nonaktif tanpa data.

## 6. Pengujian

| Uji | Hasil |
|---|---|
| ESLint (`--max-warnings 0`), TypeScript | lulus |
| Vitest web | **27/27** lulus (19 sebelumnya + 8 baru di `src/flows.test.tsx`: login→deep link→logout→Beranda+toast→Back memeriksa sesi→login lagi; logout gagal mempertahankan sesi; tanpa kilasan gate saat verifikasi; sesi dicabut di server; `safeNext`; 404 vs offline; ErrorState offline + retry; dialog konfirmasi fokus/Escape). Satu kegagalan intermiten muncul sekali saat CPU sibuk; enam run berikutnya lulus; batas tunggu Testing Library dinaikkan ke 3 s |
| `npm run check` repositori | lihat bagian 9 |
| Chrome headless, sesi anggota nyata (akun uji lokal), 18 cek | **18/18** lulus: 7 route utama tanpa error konsol/overflow; reveal grafik berjalan sekali, berhenti, tidak terulang saat hover, terulang saat rentang diganti; proyeksi: tombol sibuk, hasil muncul, fokus ke H2; drawer 390 px: fokus masuk/kembali, Escape, `inert`, tertutup setelah navigasi; logout: request server, ke `/`, toast, token hilang; `/akun` setelah logout menampilkan gate; semua request same-origin; tanpa error konsol |
| Chrome headless, 8 cek tambahan | **8/8** lulus: backend offline (504 disimulasikan via CDP) di Ringkasan/Eksplorasi/Proyeksi → pesan jelas, Coba lagi, tanpa skeleton tak berujung, tanpa klaim "belum ada proyeksi"; pulih saat backend kembali; `prefers-reduced-motion` mematikan animasi; arsip: dialog, fokus Batal, Escape membatalkan, konfirmasi → server lalu daftar + toast |

Hasil mentah: `docs/screenshots/ux-v3/after/browser-checks.txt`, `browser-checks-2.txt`.

## 7. Screenshot sebelum/sesudah

`docs/screenshots/ux-v3/before/` menyimpan hanya tangkapan yang memperlihatkan perubahan: perilaku
logout lama (`1280-harga-after-logout.png`, `1280-akun-after-logout.png`) dan tata letak kartu skenario
lama (`1440-skenario.png`). Tangkapan *before* lain identik secara visual dengan `after/` (perubahan v3
sebagian besar perilaku dan gerak, bukan tampilan statis) dan dihapus pada 17 Oktober 2026 bersama
varian lebar yang duplikat. `docs/screenshots/ux-v3/after/` (1440, 1280, 390 px) diambil dengan sesi
anggota yang valid kecuali yang bernama `after-logout` dan `offline`; hasil cek browser mentah ada di
`browser-checks.txt` dan `browser-checks-2.txt`.

## 8. Dampak performa

Tidak ada dependensi baru. Animasi hanya pada opacity/transform/clip-path dan berjalan sekali per
dataset; cek browser memastikan tidak ada animasi yang tetap berjalan setelah reveal atau saat hover,
tidak ada request ganda dari interaksi yang diuji, dan semua listener/timer (drawer, dialog, toast,
media query) dibersihkan saat unmount. Ukuran bundle naik ≈ 2,7 kB gzip (bagian 9). Waktu render
atau penggunaan CPU/RAM per halaman tidak diukur dengan profiler.

## 9. Regresi repositori

`npm run check` (17 Oktober 2026) lulus dengan exit 0: ruff dan mypy bersih, kontrak API sinkron,
pytest 96 lulus, vitest web 27 lulus, vitest gateway 20 lulus, build web dan gateway sukses,
pemindaian rahasia tanpa temuan.

Ukuran bundle web dibanding build sebelum revisi (hari yang sama):

| Berkas | Sebelum | Sesudah |
|---|---|---|
| `index-*.js` | 410,85 kB (gzip 129,10) | 418,95 kB (gzip 131,78) |
| `charts-*.js` | 380,93 kB (gzip 109,39) | 381,08 kB (gzip 109,45) |

Tambahan ≈ 2,7 kB gzip untuk toast, dialog, alur sesi, dan teks baru.

## 10. Isu yang masih tersisa

- Animasi pada **semua** tabel, tab, dan halaman operator tidak diaudit satu per satu; halaman
  `/operasi/*` hanya mendapat perbaikan bersama (toast, ErrorState, dialog tersedia) tanpa uji browser
  tersendiri.
- Snapshot pasar publik di staging tetap ±5 s (banyak round-trip D1/KV lewat Worker); skeleton
  menutupinya tetapi optimasi backend belum dilakukan.
- Halaman legal, registrasi, dan landing diuji lewat unit test dan screenshot, tidak dengan skrip
  interaksi browser.
- Akun uji lokal **`ux-v3@uji-lokal.test` / "PT Uji UX V3 (lokal)"** dibuat lewat alur registrasi
  untuk pengujian sesi; kata sandinya hanya di `var/secrets/local-ux-v3.json` (gitignored). Satu
  skenario uji di akun itu diarsipkan oleh pengujian. Tidak ada data pengguna lain yang diubah.
- Perubahan belum dideploy ke Worker staging (di luar lingkup tugas ini).
