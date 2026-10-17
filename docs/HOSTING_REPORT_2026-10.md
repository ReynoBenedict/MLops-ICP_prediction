# Laporan hosting Rp0 – ICP Intelligence (staging), 17 Oktober 2026

Laporan ini mencatat apa yang benar-benar dikerjakan dan diverifikasi untuk menjalankan ICP
Intelligence end-to-end tanpa biaya. Semua batas kuota dikutip dari dokumentasi resmi (tanggal cek
17 Oktober 2026). Tidak ada operasi git, tidak ada kartu pembayaran, tidak ada trial berbayar.
Runbook teknis: `docs/DEPLOYMENT.md`, `infra/render/README.md`.

## 1. Status awal (audit)

- Repositori: monorepo `apps/web` (React/Vite), `apps/gateway` (Cloudflare Worker), `backend/icp_api`
  (FastAPI), `pipeline/icp_ml`; rilis lokal RC 2026-10 lengkap (`docs/RELEASE_REPORT_2026-10.md`).
- Rancangan deployment lama: Worker + D1 lewat REST API, R2 (S3), Postgres + MLflow server, backend di
  Replit Reserved VM. Tiga komponen itu berbayar atau butuh checkout, sehingga tidak memenuhi Rp0.
- Proses lokal yang berjalan: Vite :5173 dan backend lokal :8000 (dibiarkan hidup; dipakai untuk
  pembanding). Tidak ada proses berat (training/ingest) yang berjalan.
- Akun: Cloudflare sudah login (OAuth wrangler, akun `5c4b…8fe2`), belum ada D1/KV/R2; Docker 27.4
  tersedia tanpa login Docker Hub; tidak ada CLI Render/Fly/cloudflared.

## 2. Platform yang dipilih dan alasannya

| Komponen | Platform | Alasan (dokumen resmi) |
|---|---|---|
| Web app + gateway + data plane | Cloudflare Workers Free (`workers.dev`) | 100.000 request/hari, 10 ms CPU; permintaan aset statis gratis tanpa batas; di atas kuota → Error 1027, bukan tagihan |
| Metadata (SQL) | Cloudflare D1 Free | 5 GB, 5 juta baris baca/hari, 100.000 baris tulis/hari; melebihi → error |
| Objek (dataset, ekspor, laporan, bundel model) | Cloudflare Workers KV Free | 1 GB, 100.000 baca/hari, 1.000 tulis/hari, nilai ≤ 25 MiB; melebihi → error |
| Backend Python (FastAPI + inferensi) | Render Free web service (image Docker) | 750 jam instance/bulan per workspace; tidur setelah 15 menit idle; tanpa metode pembayaran layanan **disuspensi**, tidak ditagih |
| Registry image | Docker Hub Personal | 1 repositori privat, 100 pull/jam |

Ditolak: **Replit** (konfigurasi `.replit` lama hanya Reserved VM berbayar dan sudah dihapus; tidak ada deployment FastAPI gratis
non-preview), **Cloudflare R2** (butuh "R2 subscription" lewat checkout; API menolak dengan kode
10042), **Hugging Face Spaces** (Docker Spaces berbayar), **Backblaze B2** (overage otomatis),
**Neon/Postgres** (tidak diperlukan).

## 3. Arsitektur final

```
browser ─► Worker icp-gateway-staging (aset statis apps/web/dist + /api/v1/* proxy + rate limit)
              └─ /api/internal/* (hanya dengan x-gateway-secret) ─► D1 `DB`, KV `OBJECTS`
Render Free (image icp-api) ─► METADATA_BACKEND=d1_gateway, OBJECT_STORE_BACKEND=gateway,
                                MODEL_SOURCE=bundle (tanpa MLflow/Postgres/R2 online)
mesin operator ─► MLflow lokal: train → validate → promote → `icp-api export-model` → KV
```

Perubahan kode minimal (tanpa menyentuh data, ingestion, rekonsiliasi, atau model):
Worker internal data plane (`apps/gateway/src/index.ts`), transport `GatewayD1Executor` dan
`GatewayObjectStore` (`backend/icp_api/storage`), mode bundel (`pipeline/icp_ml/bundle.py`,
`services/models.py`), perintah CLI `export-model` dan `clone-state`, validasi settings, dokumen.
Pemisahan training/inferensi: training dan promosi tetap lokal (MLflow), backend online hanya
memuat bundel skops yang diekspor; `train/validate/promote/rollback` ditolak dengan
`409 OPERATION_UNAVAILABLE` di mode bundel.

## 4. Yang sudah dideploy dan diverifikasi (live)

| Langkah | Hasil |
|---|---|
| D1 `icp-metadata-staging` dibuat (gratis) | id `15fece89-…`, migrasi 0001–0004 diterapkan remote, skema v4, 19 tabel |
| KV `icp-objects-staging` dibuat (gratis) | id `067677c6…` |
| Secret `GATEWAY_SHARED_SECRET` (staging) | diunggah lewat `wrangler secret put` dari berkas lokal; nilai tidak pernah dicetak |
| Worker `icp-gateway-staging` | `https://icp-gateway-staging.icp-gateway.workers.dev`, 41 aset web; `/` dan `/app/*` 200, `/api/gateway/health` ok, `/api/internal/health` 401 tanpa secret dan `{"d1":true,"kv":true}` dengan secret |
| Salin state lokal → D1/KV (`icp-api clone-state`) | 2.318 baris (dataset v4 resmi + bukti dokumen, forecast, promosi, akun, skenario, alert, laporan) + 46 objek termasuk bundel model v5; dijalankan ulang → idempoten |
| Bundel model v5 (`spread_brent_6m`) | `models/bundles/5/*` + `models/production.json` di KV |
| Backend mode staging (proses lokal, lalu image Docker dengan variabel persis Render) | `/health/ready` semua `ok` (metadata `d1_gateway`, objek `gateway`, `model_bundle`, produksi v5, dataset); ringkasan data 93 bulan, Sep-2026 = 112,81; forecast baru 201 dengan v5 asal 2026-09 |
| Kesetaraan prediksi | forecast horizon 6 dengan dan tanpa skenario: titik dan interval **identik** antara backend MLflow lokal dan backend bundel (D1/KV remote) |
| Image `icp-api:staging-20261017` | build sukses (1,69 GB), startup ≈ 1 s, 168 MiB RSS, MLflow tidak diimpor, `train` → 409 |

## 5. Yang belum selesai dan alasannya (diperbarui setelah login Docker Hub dan Render)

Langkah yang sudah dilakukan pemilik akun dan diverifikasi: `docker login` (akun `benedictreyno`),
repositori privat `benedictreyno/icp-api` (API anonim 404), image `staging-20261017` di-push
(digest `sha256:c4924f…`, linux/amd64), `render login` + workspace, kredensial registry read-only
terdaftar di Render.

**Blocker: Render menolak membuat layanan apa pun tanpa kartu.** Setiap percobaan
`services create` dengan paket `free` dijawab API Render `402 Payment information is required to
complete this request` – image privat, image publik, dua region, runtime Python dari repo contoh,
dan Docker-dari-Git. Tidak ada layanan percobaan yang terbentuk. Dokumentasi free instance tidak
menyebut syarat ini; untuk workspace ini syarat itu berlaku. Dengan aturan "tanpa metode
pembayaran", Render tidak dapat dipakai, dan **backend belum online**.

Alternatif yang diperiksa pada dokumen resmi hari yang sama, semuanya gagal syarat Rp0-tanpa-kartu:
Hugging Face Spaces (Docker Spaces butuh PRO), Modal (wajib metode pembayaran), Koyeb (tidak ada
paket gratis), Sevalla ($5/bulan), Leapcell (deploy hanya dari Git + verifikasi kartu $1),
Northflank (verifikasi kartu), Replit/Fly/Railway/Cloud Run/Oracle (berbayar atau kartu).
Cloudflare Quick Tunnel ke mesin sendiri gratis tetapi "tanpa jaminan uptime, untuk pengujian",
dan dalam sesi ini diblokir kebijakan izin (external ingress tunnel). Tabel lengkap dan tiga opsi
keputusan ada di `docs/DEPLOYMENT.md` ("Backend host: finding of 2026-10-17").

**Keputusan pemilik: opsi self-host lewat Cloudflare Quick Tunnel** (gratis, tanpa akun/kartu).
Dijalankan 2026-10-17: container `icp-api-staging` (image `icp-api:staging-20261017`, variabel
persis rencana Render, hanya terikat ke localhost:18000) → `npx wrangler tunnel quick-start` →
`BACKEND_URL` Worker diarahkan ke hostname tunnel → `wrangler deploy --env staging`. Hasil uji
melalui URL publik: smoke 11/11 PASS, uji fitur 18/18 PASS (registrasi, login, profil, ringkasan
ICP, riwayat harga, proyeksi dengan skenario pada bundel v5, ekspor CSV dari KV, simpan/daftar/
bandingkan skenario di D1, laporan pasar PDF + CSV dibuat dan diunduh, rute privat dan internal
ditolak tanpa kredensial, akses langsung ke backend lewat tunnel ditolak 403, logout mematikan
sesi). Cara menjalankan ulang: `node scripts/staging-backend.mjs` (lihat `docs/DEPLOYMENT.md`,
"Self-hosted backend through a Quick Tunnel"). Konsekuensi: API publik mati saat laptop tidur,
proses tunnel berhenti, atau koneksi putus (aset, D1, KV tetap hidup; tidak ada data hilang);
hostname tunnel berganti setiap restart dan skrip memperbarui Worker otomatis. Cloudflare menyebut
Quick Tunnel "tanpa jaminan uptime, untuk pengujian/pengembangan", maks. 200 request bersamaan.
Opsi lain tetap terdokumentasi di `docs/DEPLOYMENT.md` (kartu di Render; host berbasis Git).

## 6. Daftar variabel lingkungan (nama saja)

Render: `APP_ENV=staging`, `METADATA_BACKEND=d1_gateway`, `OBJECT_STORE_BACKEND=gateway`,
`GATEWAY_INTERNAL_URL`, `GATEWAY_SHARED_SECRET` (rahasia), `REQUIRE_GATEWAY_SECRET=true`,
`MODEL_SOURCE=bundle`, `ADMIN_API_TOKENS` (rahasia), `CORS_ALLOWED_ORIGINS`, `LOG_FORMAT=json`;
opsional `METRICS_TOKEN`, `PUBLIC_REGISTRATION_ENABLED`, `EIA_API_KEY`, `EXPOSE_API_DOCS`;
`PORT` disuntik Render. Worker: `BACKEND_URL`, `ENVIRONMENT`, `MAX_BODY_BYTES`,
`UPSTREAM_TIMEOUT_MS`, secret `GATEWAY_SHARED_SECRET`, binding `DB`, `OBJECTS`, rate limiter.
Tidak ada `D1_*`, `S3_*`, `AWS_*`, `MLFLOW_*` di Render. Nilai rahasia staging disimpan di
`var/secrets/` (diabaikan git) dan tidak ada di sumber, dokumen, bundle web, atau log.

## 7. Biaya dan risiko kuota

Biaya bulanan: **Rp0** – tidak ada komponen berbayar, tidak ada metode pembayaran terdaftar, semua
kuota berhenti dengan error/suspensi. Kuota paling ketat: KV 1.000 tulis/hari (setiap versi
dataset, ekspor forecast, laporan = 1 tulis; forecast tetap berhasil bila ekspor gagal), Render
5 GB egress/bulan (hanya respons JSON API), Render tidur 15 menit (permintaan pertama ≤ 60 s;
Worker menunggu 90 s). Perkiraan untuk pemakaian uji/komersial awal: jauh di bawah semua batas.

## 8. Regresi

`npm run check` dijalankan setelah perubahan (ruff, mypy, kontrak, pytest 96, vitest web 19,
vitest gateway 19, build, pemindaian rahasia). Lihat bagian "Verification status" di
`docs/DEPLOYMENT.md`; hasil akhir dicatat pada pesan penutup sesi.

## 9. Keamanan

Rute internal Worker hanya menerima header `x-gateway-secret` (perbandingan waktu-konstan), tidak
pernah dapat dijangkau browser, melewati proxy, dan dilindungi validasi kunci objek. Backend
menolak permintaan langsung tanpa secret (`403 GATEWAY_REQUIRED`). Tidak ada token Cloudflare di
backend; satu-satunya rahasia bersama adalah `GATEWAY_SHARED_SECRET`. Kredensial registry yang
disarankan: PAT Docker Hub read-only.

## 10. Backup dan pemulihan

D1: `wrangler d1 export … --remote` dan Time Travel; KV: `wrangler kv key list/get`; model: MLflow
lokal adalah sumber kebenaran, `export-model --version N` memindahkan pointer tanpa menghapus;
Worker: `wrangler rollback`; backend: deploy tag image sebelumnya. State lokal tetap utuh
(SQLite, `var/objects`, MLflow, DVC) – `clone-state` hanya membaca.

## 11. Jejak perubahan berkas (ringkas)

Worker + konfigurasi + 11 tes; `pipeline/icp_ml/{bundle,serialization,registry_types}.py` + 4 tes;
`backend/icp_api/{settings,container,cli,tasks}.py`, `storage/{sql,objects}.py`,
`services/{models,jobs,forecasts}.py`, `routers/health.py`, `schemas.py`, tes; `apps/web`
(HealthPage menampilkan sumber model); `packages/contracts` regenerasi; `infra/render/*`,
`infra/cloudflare/d1/wrangler.jsonc` (id D1 staging); dokumen DEPLOYMENT, ENVIRONMENTS, STORAGE,
OPERATIONS, ARCHITECTURE, README, `.env.example`, laporan ini.

## 12–17. Ringkasan poin wajib

12. **Tidak ada retraining**; model v5 yang sama diekspor sebagai bundel (artefak tidak berubah).
13. **Tidak ada operasi git** dan tidak ada deploy versi remote yang lebih lama; semua deploy dari
    working tree lokal.
14. **Tidak ada rahasia** di sumber/dokumen/bundle/log; nilai hanya di `wrangler secret`, berkas
    `var/secrets/` lokal, dan (nanti) env Render.
15. **Tidak ada layanan berbayar/trial/metode pembayaran**; R2, Replit, HF Spaces, B2 dihindari.
16. **Status hosting**: website publik berfungsi end-to-end di
    https://icp-gateway-staging.icp-gateway.workers.dev dengan backend self-host lewat Quick Tunnel
    (bukan host cloud selalu-hidup). Render tetap tidak dipakai (wajib kartu, 402). Siap untuk uji
    coba pengguna **selama mesin operator dan proses tunnel hidup**.
17. Langkah berikutnya: jalankan `node scripts/staging-backend.mjs` setiap kali mesin dinyalakan;
    untuk ketersediaan permanen tanpa biaya, pindahkan container ke mesin yang selalu hidup, atau
    putuskan opsi kartu/host Git di `docs/DEPLOYMENT.md`.
