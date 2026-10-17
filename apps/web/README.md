# @icp/web — ICP Intelligence web app

React 19 + Vite + TypeScript single-page app. Indonesian is the default language; English is a
per-browser toggle. The browser talks only to its own origin (`/api/v1/...`): the Vite dev proxy
locally (`vite.config.ts`), the Cloudflare Worker gateway in production. No backend URL or secret
is bundled.

## Run

```bash
npm run dev                 # from the repo root: FastAPI backend + Vite (http://localhost:5173)
npm run dev -w @icp/web     # Vite only (set VITE_DEV_BACKEND if the API is not on 127.0.0.1:8000)
npm run lint -w @icp/web && npm run typecheck -w @icp/web && npm run test -w @icp/web && npm run build -w @icp/web
```

## Information architecture

| Area | Route | Who | Page |
|---|---|---|---|
| Public | `/` | anonymous | Landing page (hero, live public market snapshot, how it works, honesty, plans, sources, photo credits). Signed-in users are redirected to `/ringkasan`. |
| Public | `/masuk`, `/daftar`, `/syarat`, `/privasi` | anonymous | Login (users + operator token), registration (creates a trial organisation), terms, privacy |
| Intelijen Pasar | `/ringkasan` | everyone | Ringkasan Pasar — latest official ICP, MoM/YoY, rupiah equivalent (ICP × JISDOR), benchmarks, freshness, 5-year chart with the published forecast, plain-language summary, data-quality notes |
| | `/harga` | everyone | Eksplorasi Harga — ranges, ICP vs Brent/Dubai/WTI, spreads, rupiah per barrel, monthly table, CSV, provenance & verification status |
| | `/proyeksi` | everyone (horizon ≤ 3 anonymous; scenario needs sign-in) | Proyeksi ICP — origin, issue date, horizon, scenario editor, kinds (estimasi menunggu publikasi vs proyeksi), assumptions with sources, 50/80/95 % ranges, method & error in plain language, baselines, history, CSV, limitations |
| | `/skenario` | members (analyst+) | Simulasi Skenario — saved scenarios, run, compare up to 4 vs baseline (distinct markers) |
| | `/insight` | everyone (3 for anonymous) | Insight Pasar — rule-generated insights with figures, sources, next steps |
| | `/laporan` | members | Laporan & Pemberitahuan — PDF/Excel/CSV reports, alert rules, inbox (unread badge in nav) |
| | `/akun` | members | Akun & Organisasi — profile, password, members, API keys, plan & usage |
| Operasi Platform | `/operasi`, `/operasi/data`, `/operasi/model`, `/operasi/pekerjaan`, `/operasi/audit`, `/operasi/organisasi` | operators | System health, datasets/versions/evidence/imports, model evaluation (horizon tables, intervals, coverage, compare, promote/rollback, drift, model card), jobs, audit, tenant plans |

Legacy routes (`/forecast`, `/data`, `/models`, `/operations`) redirect to the new ones.

### Deep links and post-action flow

| Link | Effect |
|---|---|
| `/masuk?next=/skenario` | after sign-in the user returns to `next` (same-origin app paths only; `/`, `/masuk`, `/daftar` and protocol-relative values are ignored). Every sign-in gate, sidebar button and `AUTH_REQUIRED` error carries the current path. |
| `/laporan?tab=pemberitahuan` | opens the alerts tab (the tab state lives in the query string, so reload and back/forward keep it) |
| `/laporan?jenis=perbandingan&skenario=<id>,<id>` | preselects the scenario-comparison report with those scenarios; the scenario page's comparison result links here ("Buat laporan perbandingan") |
| `/proyeksi#hasil`, `/skenario#hasil` | result panels; after "Buat proyeksi", "Jalankan", or "Bandingkan terpilih" the page scrolls there and moves focus to the panel heading |

### What end users never see

Run ids (`fc_…`), report ids, dataset version ids, method ids (`spread_brent_6m`), environment
and API version, operator sign-in and raw exception text are kept out of the Market Intelligence
pages (`pages.test.tsx` checks the forecast view for leaks). Methods are rendered through
`methodLabel()` ("Brent + rata-rata selisih 6 bulan terakhir"), datasets through `ds.*` labels,
and `ErrorState` shows only authored 4xx messages; server faults get a generic explanation plus a
"Kode referensi" (the request id) for support. The technical identifiers remain in CSV exports and
in the Operasi Platform area. Copy contains no em dashes (`i18n.test.ts`); ranges read
"Apr 2026 s.d. Jun 2026" and missing values render as "-".

### Motion and input

Transitions are 150 ms (controls) and 200 ms (cards, page fade-in); everything is disabled under
`prefers-reduced-motion: reduce` (`styles.css`). Focus rings are visible on the dark sidebar and on
every custom control; on coarse pointers small buttons grow to 40 px.

## i18n

`src/i18n/dictionary.ts` maps every key to `[Indonesian, English]`, so both languages can never
drift (`i18n.test.ts` checks emptiness, placeholder parity and absence of ML jargon in business
namespaces). `useT()` gives `t(key, vars)`, `td(prefix, value)` for dynamic status keys, and the
Intl locale used by `src/format.ts` (`USD 112,81` / `Rp 1.730.696` in Indonesian).

## Auth storage

| Credential | Storage key | Scope |
|---|---|---|
| User session token (`icp_sess_…`) | `localStorage["icp.sessionToken"]` | 30-day server session; revoked on sign-out / password change |
| Operator token | `sessionStorage["icp.operatorToken"]` | current tab only |
| Language | `localStorage["icp.locale"]` | `id` (default) / `en` |

`AuthProvider` re-verifies the stored credential with `GET /auth/whoami` on load and only mirrors
what the server returns (kind, role, plan, `horizon_max`, `must_change_password`). Entitlement
errors (`PLAN_LIMIT`, `FEATURE_NOT_IN_PLAN`, `SEAT_LIMIT`, `AUTH_REQUIRED`) are rendered with an
upgrade / sign-in action; nothing is enforced client-side only.

## Brand assets

`public/brand/` (logo symbol, wordmark light/dark, app icon, hero composite, OG image),
`public/favicon.svg`, `src/components/brand.tsx` (inline logo), `src/components/illustrations.tsx`
(empty-state / explanatory SVGs). Photography in `public/img/` is third-party (see
`docs/ASSET_INVENTORY.md`); the landing page shows the required credits.

## Akun untuk pengujian lokal

Tidak ada akun pengguna bawaan (seed) dengan kata sandi yang dibagikan - kata sandi hanya disimpan
sebagai hash scrypt dan tidak pernah dicatat di repositori.

| Peran | Cara masuk | Sumber |
|---|---|---|
| Operator platform (Operasi Platform) | Halaman `/masuk` → tab operator → tempel token dari `.env` (`ADMIN_API_TOKENS=local-admin:<token>`, dibuat oleh `npm run setup`) | `.env` lokal (gitignored) |
| Pengguna bisnis (Intelijen Pasar) | Daftar organisasi baru di `/daftar` (paket uji coba, peran `owner`), lalu undang anggota dari `/akun` - kata sandi sementara ditampilkan sekali | pendaftaran mandiri (`PUBLIC_REGISTRATION_ENABLED=true`) |

Basis data lokal `var/metadata.db` dapat berisi organisasi contoh yang dibuat saat verifikasi visual
(`PT Energi Nusantara Demo`, pemilik `demo@energi-nusantara.test`); kata sandinya tidak
didokumentasikan, jadi gunakan pendaftaran baru atau minta operator menetapkan kata sandi sementara
melalui `POST /api/v1/org/members/{id}/reset-password` (`?org_id=` untuk operator). Catatan: server
menolak pengaturan ulang kata sandi untuk akun berperan `owner` oleh pihak lain; jalur ini hanya
berlaku untuk anggota admin/analis/pembaca.
