# Documentation inventory and consolidation (2026-10-20)

Record of the documentation consolidation done during the v6 remediation. Every file that was under
`docs/` before is listed with its classification and the reason. Inbound references were found with
a repository-wide search (excluding `node_modules`, `.venv`, `var/`, build output and the
`docs.zip` copy at the repository root). A full copy of the previous `docs/` tree is also in git
history (HEAD `09d672c`) except `LEGAL_OWNER_INPUTS.md`, which was never committed; its content is
merged in full into `LEGAL_AND_PRIVACY.md`.

Classification: **CURRENT** (kept as an authoritative document, possibly edited) · **MERGE** (content
moved into another document, file removed) · **ARCHIVE** (kept unchanged as a historical record, with a
banner, under `docs/archive/` or `docs/evidence/`) · **DELETE** (removed; reason given).

## Before and after

| | Before (2026-10-20 morning) | After consolidation | After screenshot pruning (final) |
|---|---|---|---|
| Files under `docs/` | 80 (21 Markdown, 55 PNG, 3 TXT, 1 JSON) | 84 (27 Markdown, 53 PNG, 3 TXT, 1 JSON) | 53 (27 Markdown, 22 PNG, 3 TXT, 1 JSON) |
| Total size | 11,490,228 bytes | 11,377,677 bytes | 3,762,710 bytes (−67 %) |
| Competing documents on one topic | legal (3 places: `LEGAL_NOTICES`, `LEGAL_OWNER_INPUTS`, in-app pages), insight rules (methodology §7a and API notes), four dated reports linked as if current | one authoritative document per topic, indexed in [README.md](README.md) |

## Markdown and data files

| File (before) | Size | Modified | Inbound references | Class | Result / reason |
|---|---|---|---|---|---|
| `API.md` | 15,319 | 2026-10-19 | README, ARCHITECTURE, SECURITY, `apps/web/src/api/v5.ts` (since renamed `product-types.ts`; it no longer cites the file) | CURRENT | Authoritative API and access matrix |
| `ARCHITECTURE.md` | 13,315 | 2026-10-19 | README, `storage/sql.py` | CURRENT | Overview/architecture; overlaps with STORAGE only in a short transport summary that links there |
| `ASSET_INVENTORY.md` | 6,458 | 2026-10-17 | README, `apps/web/README.md`, COMMERCIAL_READINESS | CURRENT | Licences of visual assets |
| `COMMERCIAL_READINESS.md` | 4,805 | 2026-10-19 | README, `entitlements.py`, `services/accounts.py`, `services/alerts.py` | CURRENT | Links to the deleted `LEGAL_NOTICES.md` and the archived hosting report replaced |
| `DATA_DICTIONARY.md` | 5,471 | 2026-10-16 | README | CURRENT | Table list to be extended with migrations 0005+ |
| `DATA_RECONCILIATION.md` | 18,135 | 2026-10-19 | README, `services/icp_official.py`, `pipeline/icp_ml/training.py`, model cards | CURRENT | Official ICP evidence; unique |
| `DATA_SOURCES.md` | 6,635 | 2026-10-16 | README, `.env.example`, `settings.py`, `bi_jisdor.py`, `training.py` | CURRENT | Sources, licences, provenance |
| `DEPLOYMENT.md` | 23,078 | 2026-10-19 | README, `.env.example`, CI workflow, gateway, `cli.py`, `settings.py`, `sql.py`, Dockerfile, `infra/render/*` | CURRENT | Runbook incl. the Render finding (a historical decision kept in place because the runbook depends on it) |
| `ENVIRONMENTS.md` | 6,167 | 2026-10-19 | README, `.env.example`, `settings.py` | CURRENT | Environments and secrets |
| `FORECASTING_METHODOLOGY.md` | 12,354 | 2026-10-19 | README, `MethodologyPage.tsx`, `insights/rules.py`, `features.py`, `forecasting.py` | CURRENT | §7a insight rule table moved to `INSIGHT_ENGINE.md` (pointer kept because `rules.py` cites "Insight rules"); stale "system forecast shown publicly" corrected to the policy-v4 behaviour |
| `FRONTEND_IMPROVEMENT_REPORT_2026-10.md` | 15,420 | 2026-10-17 | README, RELEASE report | ARCHIVE | Dated report → `archive/`; frontend conventions live in ARCHITECTURE |
| `HOSTING_REPORT_2026-10.md` | 12,177 | 2026-10-17 | README, COMMERCIAL_READINESS, RELEASE report | ARCHIVE | Dated report → `archive/`; the runbook is DEPLOYMENT |
| `LEGAL_NOTICES.md` | 3,826 | 2026-10-16 | README, COMMERCIAL_READINESS | MERGE + DELETE | Summary of the old in-app text; misleading against the code (said the session token is in `sessionStorage`, data on R2/PostgreSQL). Still-valid content (attribution, practice table) merged into `LEGAL_AND_PRIVACY.md` with facts re-checked against code |
| `LEGAL_OWNER_INPUTS.md` | 4,013 | 2026-10-19 | README | MERGE | Owner-input table merged unchanged into `LEGAL_AND_PRIVACY.md`; file was untracked |
| `MIGRATION.md` | 9,210 | 2026-10-17 | README, `legacy/README.md`, `legacy/mlflow-history/README.md`, `features.py` | CURRENT | History of the restructure; path is cited from `legacy/` (not editable here), so it stays at the top level. Link to the course README updated |
| `OPERATIONS.md` | 9,303 | 2026-10-17 | README, `cli.py`, `infra/render/README.md`, COMMERCIAL_READINESS | CURRENT | Monitoring and incident-response sections added |
| `RELEASE_REPORT_2026-10.md` | 16,941 | 2026-10-17 | README, FRONTEND and HOSTING reports | ARCHIVE | Dated report → `archive/`; banner corrects its claim that the pre-redesign screenshots show the Streamlit app (they show React v1) |
| `SECURITY.md` | 9,185 | 2026-10-19 | README, ARCHITECTURE, LEGAL_OWNER_INPUTS, RELEASE report | CURRENT | Scan procedure and reviewed false positives added; links to THREAT_MODEL |
| `STORAGE.md` | 7,314 | 2026-10-17 | README, `storage/objects.py` | CURRENT | Kept separate from ARCHITECTURE (cited from code); schema list to be extended with migrations 0005+ |
| `UX_V3_REPORT_2026-10.md` | 12,120 | 2026-10-17 | README | ARCHIVE | Dated report → `archive/`; banner records the removed byte-identical "before" images |
| `assets/photo-sources.json` | 2,810 | 2026-10-16 | ASSET_INVENTORY | CURRENT | Machine-readable photo credits |
| `history/course-milestones-README.md` | 18,480 | 2026-10-16 | README, MIGRATION, package metadata | ARCHIVE | → `archive/course-milestones-README.md`; `history/` removed |
| `screenshots/after-v2/metrics-report.txt` | 4,633 | 2026-10-17 | FRONTEND and RELEASE reports | ARCHIVE | → `evidence/2026-10-17-frontend-v2/` |
| `screenshots/ux-v3/after/browser-checks.txt`, `browser-checks-2.txt` | 2,067 / 1,081 | 2026-10-17 | UX v3 report | ARCHIVE | → `evidence/2026-10-17-ux-v3/after/` |

New documents: [README.md](README.md) (index), [DEVELOPMENT.md](DEVELOPMENT.md) (local development,
tests, CI; previously spread over the project README and OPERATIONS), [THREAT_MODEL.md](THREAT_MODEL.md)
(previously only in agent working notes), [INSIGHT_ENGINE.md](INSIGHT_ENGINE.md),
[LEGAL_AND_PRIVACY.md](LEGAL_AND_PRIVACY.md), [evidence/README.md](evidence/README.md),
this file, and [REMEDIATION_REPORT_2026-10.md](REMEDIATION_REPORT_2026-10.md).

## Screenshots

| Folder (before) | Files | Size | Inbound references | Class | Result / reason |
|---|---|---|---|---|---|
| `screenshots/before/` | 6 PNG | 1,080,942 | RELEASE report (folder) | ARCHIVE | → `evidence/2026-10-16-react-v1/`. Unique record of the first React version on the legacy data; renamed because the RELEASE report wrongly called it the Streamlit app |
| `screenshots/after-v2/` | 29 PNG + 1 TXT | 7,857,954 | README (2 crops), FRONTEND and RELEASE reports | ARCHIVE | → `evidence/2026-10-17-frontend-v2/`. Public, operator and mobile captures with no later equivalent; `*-public` captures are labelled as the pre-v4 access model. Two crops kept only while the project README uses them |
| `screenshots/ux-v3/after/` | 17 PNG + 2 TXT | 2,041,865 | README (4), UX v3 report | ARCHIVE | → `evidence/2026-10-17-ux-v3/after/` |
| `screenshots/ux-v3/before/1280-harga-after-logout.png` | 1 PNG | 121,438 | UX v3 report | ARCHIVE | → `evidence/2026-10-17-ux-v3/before/`; the only "before" capture that differs from its "after" |
| `screenshots/ux-v3/before/1440-skenario.png` | 1 PNG | 101,295 | UX v3 report | DELETE | Byte-identical (MD5 `bf7988f6…`) to `after/1440-skenario.png`, so it cannot show the change the report claims; the `after/` copy is kept |
| `screenshots/ux-v3/before/1280-akun-after-logout.png` | 1 PNG | 54,198 | UX v3 report | DELETE | Byte-identical (MD5 `8ac92ee1…`) to `after/1280-akun-after-logout.png`; the `after/` copy is kept |

All retained screenshots were checked for credentials: they show only test accounts on `.test`
e-mail domains and test organisation names; no token, password or secret is visible.

## Not in scope of this inventory

- `docs.zip` (10.9 MB) at the repository root, untracked, created 2026-10-20 05:44: a copy of the
  previous `docs/` tree, not created by the documentation agent. It is outside `docs/`; whether to keep
  it is the owner's decision.
- READMEs outside `docs/` (`apps/web/README.md`, `infra/render/README.md`, `legacy/**/README.md`) are
  owned by their components and were only checked for links into `docs/`.

## Screenshot pruning (2026-10-20, after verification)

The consolidation first moved the historical captures; they still made up ~11 MB. They were then pruned to
captures with verification or comparison value: one before/after pair per meaningful change, the reference
state before each redesign, and the current views used by the project README (new folder
`evidence/2026-10-20-v6/`, 6 README views plus 11 fix captures from the frontend agent). Everything removed
is superseded by a newer capture of the same page or shows no finding; all of it remains in git history
(HEAD `09d672c`) and in `docs.zip` at the repository root.

| Removed (folder) | Files | Bytes | Reason |
|---|---|---|---|
| `2026-10-16-react-v1/`: `0390-overview`, `1440-data`, `1440-forecast`, `1440-models`, `1440-operations` | 5 | 843,970 | Obsolete UI; `1440-overview.png` alone documents the pre-redesign state |
| `2026-10-17-frontend-v2/`: all captures except `1440-ringkasan-public.png` (landing ×3 incl. the README crop, login/register ×3, terms/privacy ×2, five other `*-public` workspace pages, scenario forecast ×2 incl. the README crop, reports ×2, operator account/scenarios ×2, ops data/health/model ×3, mobile/tablet ×6) | 28 | 7,480,101 | Superseded by the 2026-10-20 captures and by the access-policy change; the kept `*-public` capture is the single before-image for that change; `metrics-report.txt` keeps the measurements |
| `2026-10-17-ux-v3/after/`: member pages at 1440 (×7) and 390 (×4 incl. the open menu), `1280-akun-after-logout`, `1440-offline-harga`, `1440-offline-proyeksi`, `1440-skenario-archive-dialog` | 15 | 1,626,340 | Superseded by current captures; one offline example and the logout before/after pair are kept; the dialog behaviour is recorded in `browser-checks-2.txt` |
