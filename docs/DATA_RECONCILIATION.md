# Historical ICP data reconciliation

*Generated 2026-10-16 22:09 UTC from the ESDM backfill state in `var/sources/esdm/` (`decrees.json`, `articles.json`, `reconciliation.json`). Re-run `icp-api ingest icp` and the report script to refresh.*

## Sources and method

1. **Decrees** (Keputusan Menteri ESDM, PDF) from the official archive `https://www.migas.esdm.go.id/post/harga-minyak-mentah` - one per month 2019-01..2026-09. Text-layer PDFs are parsed directly; scanned PDFs (2019-2021) are OCR'd (Tesseract, up to 6 pages). The operative clause read is "Harga rata-rata Minyak Mentah Indonesia untuk bulan <M> <Y> ditetapkan sebesar US$ N/barrel".
2. **Monthly announcements** (HTML articles on `migas.esdm.go.id`), which state the month's value, the decree number and the previous month's value.
3. **Reconciliation**: `verified` = two independent official observations agree (decree text + announcement, or announcement + the next month's back-reference); `single_source` = one observation; `conflict` = official observations disagree; `missing` = no observation.
4. **Acceptance into the dataset** (bounded to the archive range, 2019-01 onward): `verified`; `single_source` when the observation is an announcement (HTML text); and OCR-only decrees whose operative clause names the archive month **and** whose decree number was read (stored as `machine_extracted`, flagged until a second source confirms). Weak sources (next month's back-reference, the legacy CSV) corroborate but never create conflicts; when two or more sources agree against one dissenting announcement, the majority stands and the dissent is recorded. Remaining conflicts block the month.
5. **Source defects found**: the archive PDF linked for 2023-07 is a different document (a Dec-2022 amendment letter), so July 2023 rests on its announcement; the archive links for 2024-07..2024-11 return HTTP 404, so those months rest on announcements and back-references; the April-2019 announcement states US$63.81 while the decree text and the May-2019 back-reference state US$68.31 (majority rule); the 2020-10 decree download failed repeatedly (server side) and the month rests on its announcement; the September-2026 decree text carries number 276.K/MG.03/MEM.M/2026 while the announcement cites 395.K/MG.03/MEM.M/2026 - the value (112.81) agrees, the numbering difference is recorded, not resolved.

## Coverage summary

| Item | Count |
|---|---|
| Decree PDFs retrieved | 86 (with extracted value: 85; text layer: 38, OCR: 47) |
| Announcement articles parsed with a value | 92 |
| Months `single_source` | 10 |
| Months `verified` | 89 |
| Months accepted into the official dataset | 93 (2019-01..2026-09; of which OCR-only `machine_extracted`: 1) |
| Months found outside the archive range (not stored) | 6 (2016-10, 2016-11, 2018-09, 2018-10, 2018-11, 2018-12) |
| Legacy CSV months (data/raw/dataset.csv) | 79 (2019-01..2026-03, 8 gaps) |
| Accepted months that correct a legacy value | 4 |
| Accepted months new vs legacy | 14 |

## Corrections to the legacy series

| Month | Legacy value | Official value | Δ | Status | Evidence | Reference |
|---|---|---|---|---|---|---|
| 2021-10 | 76.16 | 81.80 | +5.64 | verified | announcement, announcement_backref, decree_ocr | 217.K/HK.02/MEM.M/2021 |
| 2022-01 | 84.54 | 85.89 | +1.35 | verified | announcement, announcement_backref, decree_ocr | 11.K/MG.03/DJM/2022 |
| 2022-03 | 99.12 | 113.50 | +14.38 | verified | announcement, decree_ocr | 33.K/MG.03/DJM/2022 |
| 2023-07 | 80.96 | 75.06 | -5.90 | single_source | announcement | 272.K/MG.03/DJM/2023 |

## Months added beyond the legacy series

| Month | Official value | Status | Evidence | Reference |
|---|---|---|---|---|
| 2020-10 | 38.07 | verified | announcement, decree_ocr | 225K/12/MEM/2020 |
| 2023-05 | 70.12 | single_source | announcement | 216.K/MG.03/DJM/2023 |
| 2023-06 | 70.12 | single_source | announcement | 241.K/MG.03/DJM/2023 |
| 2024-07 | 82.00 | single_source | announcement | 333.K/MG.03/DJM/2024 |
| 2024-08 | 78.51 | single_source | announcement | 348.K/MG.03/DJM/2024 |
| 2024-09 | 72.54 | verified | announcement, announcement_backref | 353.K/MG.03/DJM/2024 |
| 2024-10 | 73.53 | verified | announcement, announcement_backref | 363.K/MG.03/DJM/2024 |
| 2024-11 | 71.83 | single_source | announcement | 373.K/MG.03/DJM/2024 |
| 2026-04 | 117.31 | verified | announcement_backref, decree_ocr | 203.K/MG.03/MEM.M/2026 |
| 2026-05 | 106.56 | verified | announcement, announcement_backref, decree_ocr | 228.K/MG.03/MEM.M/2026 |
| 2026-06 | 83.45 | verified | announcement, announcement_backref, decree_ocr | 282.K/MG.03/MEM.M/2026 |
| 2026-07 | 81.68 | verified | announcement, announcement_backref, decree_ocr | 319.K/MG.03/MEM.M/2026 |
| 2026-08 | 89.43 | verified | announcement, announcement_backref, decree_text | 276.K/MG.03/MEM.M/2026 |
| 2026-09 | 112.81 | verified | announcement, decree_text | 276.K/MG.03/MEM.M/2026 |

## Unresolved

### Conflicts (excluded until resolved)

None.

### Accepted from decree OCR only (`machine_extracted`, awaiting a second source)

- 2020-05: 25.67 (decree_ocr; decree 107.K/12/MEM/2020; legacy CSV 25.67)

### Pending (single weak source, not stored)

None.

### No official observation retrieved

None.

## Full month-by-month table

| Month | Official | Status | Evidence kinds | Reference | Legacy | Δ |
|---|---|---|---|---|---|---|
| 2016-10 | 46.64 | single_source | announcement_backref |  | - | - |
| 2016-11 | 43.25 | single_source | announcement |  | - | - |
| 2018-09 | 75.38 | single_source | announcement_backref |  | - | - |
| 2018-10 | 77.56 | verified | announcement, announcement_backref |  | - | - |
| 2018-11 | 62.98 | verified | announcement, announcement_backref |  | - | - |
| 2018-12 | 54.81 | verified | announcement, announcement_backref |  | - | - |
| 2019-01 | 56.55 | verified | announcement, announcement_backref, decree_text | 20K/12/MEM/2019 | 56.55 | +0.00 |
| 2019-02 | 61.31 | verified | announcement, announcement_backref, decree_ocr | 42K/12/MEM/2019 | 61.31 | +0.00 |
| 2019-03 | 63.60 | verified | announcement, announcement_backref, decree_ocr | 58K/12/MEM/2019 | 63.60 | +0.00 |
| 2019-04 | 68.31 | verified | announcement, announcement_backref, decree_text | 75K/12/MEM/2019 | 68.31 | +0.00 |
| 2019-05 | 68.07 | verified | announcement, decree_text | 96K/12/MEM/2019 | 68.07 | +0.00 |
| 2019-06 | 61.00 | verified | announcement, announcement_backref, decree_ocr | 116K/12/MEM/2019 | 61.00 | +0.00 |
| 2019-07 | 61.32 | verified | announcement, announcement_backref, decree_ocr | 142K/12/MEM/2019 | 61.32 | +0.00 |
| 2019-08 | 57.27 | verified | announcement, announcement_backref, decree_ocr | 156K/12/MEM/2019 | 57.27 | +0.00 |
| 2019-09 | 60.84 | verified | announcement, announcement_backref, decree_ocr | 185K/12/MEM/2019 | 60.84 | +0.00 |
| 2019-10 | 59.82 | verified | announcement, announcement_backref, decree_ocr | 225K/12/MEM/2019 | 59.82 | +0.00 |
| 2019-11 | 63.26 | verified | announcement, announcement_backref, decree_ocr | 244K/12/MEM/2019 | 63.26 | +0.00 |
| 2019-12 | 67.18 | verified | announcement, decree_ocr | 4K/12/MEM/2020 | 67.18 | +0.00 |
| 2020-01 | 65.38 | verified | announcement, announcement_backref, decree_ocr | 38K/12/MEM/2020 | 65.38 | +0.00 |
| 2020-02 | 56.61 | verified | announcement, announcement_backref, decree_text | 68.K/12/MEM/2020 | 56.61 | +0.00 |
| 2020-03 | 34.23 | verified | announcement, decree_text | 71K/12/MEM/2020 | 34.23 | +0.00 |
| 2020-04 | 20.66 | verified | decree_text | 95K/12/MEM/2020 | 20.66 | +0.00 |
| 2020-05 | 25.67 | single_source | decree_ocr | 107.K/12/MEM/2020 | 25.67 | +0.00 |
| 2020-06 | 36.68 | verified | announcement, announcement_backref, decree_ocr | 134K/12/MEM/2020 | 36.68 | +0.00 |
| 2020-07 | 40.64 | verified | announcement, announcement_backref, decree_text | 147K/12/MEM/2020 | 40.64 | +0.00 |
| 2020-08 | 41.63 | verified | announcement, decree_ocr | 158K/12/MEM/2020 | 41.63 | +0.00 |
| 2020-09 | 37.43 | verified | announcement, decree_text | 191K/12/MEM/2020 | 37.43 | +0.00 |
| 2020-10 | 38.07 | verified | announcement, decree_ocr | 225K/12/MEM/2020 | - | - |
| 2020-11 | 40.67 | verified | announcement, announcement_backref, decree_text | 239K/12/MEM/2020 | 40.67 | +0.00 |
| 2020-12 | 47.78 | verified | announcement, announcement_backref, decree_ocr | 4.K/MG.03/MEM.M/2021 | 47.78 | +0.00 |
| 2021-01 | 53.17 | verified | announcement, decree_ocr | 22.K/MG.03/MEM.M/2021 | 53.17 | +0.00 |
| 2021-02 | 60.36 | verified | announcement, announcement_backref, decree_ocr | 39.K/MG.03/MEM.M/2021 | 60.36 | +0.00 |
| 2021-03 | 63.50 | verified | announcement, announcement_backref, decree_text | 64.K/MG.03/MEM.M/2021 | 63.50 | +0.00 |
| 2021-04 | 61.96 | verified | announcement, announcement_backref, decree_text | 84.K/HK.02/MEM.M/2021 | 61.96 | +0.00 |
| 2021-05 | 65.49 | verified | announcement, decree_text | 105.K/HK.02/MEM.M/2021 | 65.49 | +0.00 |
| 2021-06 | 70.23 | verified | announcement, announcement_backref, decree_text | 120.K/HK.02/MEM.M/2021 | 70.23 | +0.00 |
| 2021-07 | 72.17 | verified | announcement, announcement_backref, decree_ocr | 138.K/HK.02/MEM.M/2021 | 72.17 | +0.00 |
| 2021-08 | 67.80 | verified | announcement, announcement_backref, decree_ocr | 166.K/HK.02/MEM.M/2021 | 67.80 | +0.00 |
| 2021-09 | 72.20 | verified | announcement, announcement_backref, decree_ocr | 192.K/HK.02/MEM.M/2021 | 72.20 | +0.00 |
| 2021-10 | 81.80 | verified | announcement, announcement_backref, decree_ocr | 217.K/HK.02/MEM.M/2021 | 76.16 | +5.64 |
| 2021-11 | 80.13 | verified | announcement, announcement_backref, decree_ocr | 181.K/MG.03/DJM/2021 | 80.13 | +0.00 |
| 2021-12 | 73.36 | verified | announcement, announcement_backref, decree_ocr | 2.K/MG.03/DJIM/2022 | 73.36 | +0.00 |
| 2022-01 | 85.89 | verified | announcement, announcement_backref, decree_ocr | 11.K/MG.03/DJM/2022 | 84.54 | +1.35 |
| 2022-02 | 95.72 | verified | announcement, announcement_backref, decree_ocr | 18.K/MG.03/DJM/2022 | 95.72 | +0.00 |
| 2022-03 | 113.50 | verified | announcement, decree_ocr | 33.K/MG.03/DJM/2022 | 99.12 | +14.38 |
| 2022-04 | 102.51 | verified | announcement, announcement_backref, decree_text | 48.K/MG.03/DJM/2022 | 102.51 | +0.00 |
| 2022-05 | 109.61 | verified | announcement, announcement_backref, decree_text | 54.K/MG.03/DJM/2022 | 109.61 | +0.00 |
| 2022-06 | 117.62 | verified | announcement, decree_text | 61.K/MG.01/MEM.M/2022 | 117.62 | +0.00 |
| 2022-07 | 106.73 | verified | announcement, announcement_backref, decree_text | 111.K/MG.03/DJM/2022 | 106.73 | +0.00 |
| 2022-08 | 94.17 | verified | announcement, announcement_backref, decree_ocr | 129.K/MG.03/DJM/2022 | 94.17 | +0.00 |
| 2022-09 | 86.07 | verified | announcement, announcement_backref, decree_text | 140.K/MG.03/DJM/2022 | 86.07 | +0.00 |
| 2022-10 | 89.10 | verified | announcement, announcement_backref, decree_text | 151.K/MG.03/DJM/2022 | 89.10 | +0.00 |
| 2022-11 | 87.50 | verified | announcement, announcement_backref, decree_text | 174.K/MG.03/DJM/2022 | 87.50 | +0.00 |
| 2022-12 | 76.66 | verified | announcement, announcement_backref, decree_text | 2.K/MG.03/DJM/2023 | 76.66 | +0.00 |
| 2023-01 | 78.54 | verified | announcement, announcement_backref, decree_text | 47.K/MG.03/DJM/2023 | 78.54 | +0.00 |
| 2023-02 | 79.48 | verified | announcement, announcement_backref, decree_text | 101.K/MG.03/DJM/2023 | 79.48 | +0.00 |
| 2023-03 | 74.59 | verified | announcement, decree_text | 131.K/MG.03/DJM/2023 | 74.59 | +0.00 |
| 2023-04 | 79.34 | verified | announcement, announcement_backref, decree_text | 174.K/MG.03/DJM/2023 | 79.34 | +0.00 |
| 2023-05 | 70.12 | single_source | announcement | 216.K/MG.03/DJM/2023 | - | - |
| 2023-06 | 70.12 | single_source | announcement | 241.K/MG.03/DJM/2023 | - | - |
| 2023-07 | 75.06 | single_source | announcement | 272.K/MG.03/DJM/2023 | 80.96 | -5.90 |
| 2023-08 | 82.59 | verified | announcement, decree_text | 324.K/MG.03/DJM/2023 | 82.59 | +0.00 |
| 2023-09 | 90.17 | verified | announcement, announcement_backref, decree_text | 341.K/MG.03/DJM/2023 | 90.17 | +0.00 |
| 2023-10 | 86.72 | verified | announcement, decree_text | 61.K/MG.01/MEM.M/2022 | 86.72 | +0.00 |
| 2023-11 | 79.63 | verified | announcement, decree_text | 61.K/MG.01/MEM.M/2022 | 79.63 | +0.00 |
| 2023-12 | 75.51 | verified | decree_text | 61.K/MG.01/MEM.M/2022 | 75.51 | +0.00 |
| 2024-01 | 77.12 | verified | announcement_backref, decree_text | 61.K/MG.01/MEM.M/2022 | 77.12 | +0.00 |
| 2024-02 | 80.09 | verified | announcement, announcement_backref, decree_text | 61.K/MG.01/MEM.M/2022 | 80.09 | +0.00 |
| 2024-03 | 83.78 | verified | announcement, announcement_backref, decree_text | 213.K/MG.03/DJM/2024 | 83.78 | +0.00 |
| 2024-04 | 87.61 | verified | announcement, decree_text | 238.K/MG.03/DJM/2024 | 87.61 | +0.00 |
| 2024-05 | 79.78 | verified | announcement, announcement_backref, decree_text | 261.K/MG.03/DJM/2024 | 79.78 | +0.00 |
| 2024-06 | 79.31 | verified | announcement, decree_text | 327.K/MG.03/DJM/2024 | 79.31 | +0.00 |
| 2024-07 | 82.00 | single_source | announcement | 333.K/MG.03/DJM/2024 | - | - |
| 2024-08 | 78.51 | single_source | announcement | 348.K/MG.03/DJM/2024 | - | - |
| 2024-09 | 72.54 | verified | announcement, announcement_backref | 353.K/MG.03/DJM/2024 | - | - |
| 2024-10 | 73.53 | verified | announcement, announcement_backref | 363.K/MG.03/DJM/2024 | - | - |
| 2024-11 | 71.83 | single_source | announcement | 373.K/MG.03/DJM/2024 | - | - |
| 2024-12 | 71.61 | verified | announcement, decree_ocr | 9.K/MG.01/MEM.M/2025 | 71.61 | +0.00 |
| 2025-01 | 76.81 | verified | announcement, announcement_backref, decree_ocr | 59.K/MG.01/MEM.M/2025 | 76.81 | +0.00 |
| 2025-02 | 74.29 | verified | announcement, announcement_backref, decree_ocr | 90.K/MG.01/MEM.M/2025 | 74.29 | +0.00 |
| 2025-03 | 71.11 | verified | announcement, announcement_backref, decree_ocr | 143.K/MG.01/MEM.M/2025 | 71.11 | +0.00 |
| 2025-04 | 65.29 | verified | announcement, decree_ocr | 176.K/MG.01/MEM.M/2025 | 65.29 | +0.00 |
| 2025-05 | 62.75 | verified | announcement, announcement_backref, decree_ocr | 208.K/MG.03/MEM.M/2025 | 62.75 | +0.00 |
| 2025-06 | 69.33 | verified | announcement, decree_ocr | 229.K/MG.03/MEM.M/2025 | 69.33 | +0.00 |
| 2025-07 | 68.59 | verified | announcement, announcement_backref, decree_ocr | 269.K/MG.01/MEM.M/2025 | 68.59 | +0.00 |
| 2025-08 | 66.07 | verified | announcement, decree_ocr | 304.K/MG.03/MEM.M/2025 | 66.07 | +0.00 |
| 2025-09 | 66.81 | verified | announcement, announcement_backref, decree_ocr | 336.K/MG.03/MEM.M/2025 | 66.81 | +0.00 |
| 2025-10 | 63.62 | verified | announcement, announcement_backref, decree_ocr | 351.K/MG.01/MEM.M/2025 | 63.62 | +0.00 |
| 2025-11 | 62.83 | verified | announcement, announcement_backref, decree_ocr | 416.K/MG.03/MEM.M/2025 | 62.83 | +0.00 |
| 2025-12 | 61.10 | verified | announcement, announcement_backref, decree_ocr | 10.K/MG.03/MEM.M/2026 | 61.10 | +0.00 |
| 2026-01 | 64.41 | verified | announcement, decree_ocr | 98.K/MG.03/MEM.M/2026 | 64.41 | +0.00 |
| 2026-02 | 68.79 | verified | announcement_backref, decree_ocr | 115.K/MG.03/MEM.M/2026 | 68.79 | +0.00 |
| 2026-03 | 102.26 | verified | announcement, decree_ocr | 149.K/MG.03/MEM.M/2026 | 102.26 | +0.00 |
| 2026-04 | 117.31 | verified | announcement_backref, decree_ocr | 203.K/MG.03/MEM.M/2026 | - | - |
| 2026-05 | 106.56 | verified | announcement, announcement_backref, decree_ocr | 228.K/MG.03/MEM.M/2026 | - | - |
| 2026-06 | 83.45 | verified | announcement, announcement_backref, decree_ocr | 282.K/MG.03/MEM.M/2026 | - | - |
| 2026-07 | 81.68 | verified | announcement, announcement_backref, decree_ocr | 319.K/MG.03/MEM.M/2026 | - | - |
| 2026-08 | 89.43 | verified | announcement, announcement_backref, decree_text | 276.K/MG.03/MEM.M/2026 | - | - |
| 2026-09 | 112.81 | verified | announcement, decree_text | 276.K/MG.03/MEM.M/2026 | - | - |

## Acceptance check

- September 2026: official value 112.81 (verified; evidence: announcement, decree_text; decree numbers read from the documents: 276.K/MG.03/MEM.M/2026, 395.K/MG.03/MEM.M/2026). Independently checked against the announcement article `/post/Tensi-Geopolitik-Memanas-ICP-September-2026-Melonjak-ke-Angka-US-112-81-per-Barel` (published 2026-10-13) and the archive decree `Kepmen_ICP_Bulan_September_2026.pdf` (text layer).
- Unverified legacy values are superseded in the metadata store (evidence `rejected`), never deleted; `data/raw/dataset.csv` is untouched.
