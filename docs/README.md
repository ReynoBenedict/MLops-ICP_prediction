# Documentation index

One authoritative document per topic. If two documents disagree, the one listed here wins and the
other is a defect: fix it or report it. Dated reports are historical and live in [archive/](archive/);
screenshots and raw check output live in [evidence/](evidence/README.md).

## Authoritative documents

| Topic | Document | Covers |
|---|---|---|
| Overview | [../README.md](../README.md) | What the product is, status, features, quick start, known limitations |
| Architecture | [ARCHITECTURE.md](ARCHITECTURE.md) | Components and runtimes, request flow, model semantics summary, jobs, security model summary, frontend conventions |
| Design system | [DESIGN_SYSTEM.md](DESIGN_SYSTEM.md) | Concept, colour, typography (rem scale), space, layout, motion, interaction and focus rules, components, writing rules, stylesheet and string loading |
| Storage | [STORAGE.md](STORAGE.md) | Stores and transports, schema and migration rules, object naming and retention, credentials per store |
| Local development, testing, CI | [DEVELOPMENT.md](DEVELOPMENT.md) | Setup, dev stack, Windows notes, contracts, quality gates and test suites, CI workflows |
| API | [API.md](API.md) | Conventions, error envelope, access-control matrix (policy v4), endpoint list |
| Security | [SECURITY.md](SECURITY.md) | Principals, authentication and authorization, tenant isolation, secrets, edge and request hygiene, audit and privacy, scans, known gaps |
| Threat model | [THREAT_MODEL.md](THREAT_MODEL.md) | Assets, trust boundaries, STRIDE per entry point, v6 changes, security acceptance tests |
| Data sources and provenance | [DATA_SOURCES.md](DATA_SOURCES.md) | Inventory, licences, completeness and freshness rules, provenance per value |
| Data dictionary | [DATA_DICTIONARY.md](DATA_DICTIONARY.md) | Series, value kinds, verification statuses, features, tables, forecast record |
| Official ICP reconciliation and verification | [DATA_RECONCILIATION.md](DATA_RECONCILIATION.md) | Method, coverage, corrections, unresolved months, month-by-month evidence |
| Forecasting methodology and evaluation | [FORECASTING_METHODOLOGY.md](FORECASTING_METHODOLOGY.md) | Target and timing, features, methods, backtest, results, promotion gates, serving semantics |
| Insight engine | [INSIGHT_ENGINE.md](INSIGHT_ENGINE.md) | Pipeline, API, rule catalogue, consistency rules, tests |
| Deployment and rollback | [DEPLOYMENT.md](DEPLOYMENT.md) | Zero-cost topology, free-tier limits, runbook, hosting findings, rollback and recovery |
| Environments and secrets | [ENVIRONMENTS.md](ENVIRONMENTS.md) | Settings per environment, startup validation, Worker variables |
| Operations, monitoring, incidents | [OPERATIONS.md](OPERATIONS.md) | Health, monitoring, incident response, logs, common problems, data refresh, tenant tasks, backups |
| Commercial readiness and limitations | [COMMERCIAL_READINESS.md](COMMERCIAL_READINESS.md) | Capability matrix, external requirements, honest limitations |
| Billing (sandbox only) | [BILLING.md](BILLING.md) | Midtrans sandbox adapter, order state machine, webhook checks, reconciliation CLI, activation gates |
| Production-readiness audit (v7) | [audits/](audits/PRODUCTION_READINESS_PLAN.md) | Baseline, plan, full risk register, owner decisions, provider comparison, remediation log, residual risks |
| Legal and privacy status | [LEGAL_AND_PRIVACY.md](LEGAL_AND_PRIVACY.md) | Draft status, owner inputs, implementation facts behind the privacy text, attribution |
| Visual assets | [ASSET_INVENTORY.md](ASSET_INVENTORY.md) | Fonts, original SVGs and illustrations, product screenshots, the two CC0 photographs the site shows, the CC BY / CC BY-SA photographs kept but not served ([assets/photo-sources.json](assets/photo-sources.json)), and the licence register checked by `npm run asset-licences` |
| Migration history | [MIGRATION.md](MIGRATION.md) | What moved from the Streamlit/LK-12 version, defects corrected, October 2026 changes |

## Reports

| Report | Status |
|---|---|
| [PRODUCT_TRANSFORMATION_2026-10.md](PRODUCT_TRANSFORMATION_2026-10.md) | Current: full-stack product transformation of 2026-10-23 to 27 (baseline findings, design, frontend, backend, data, security, performance, tests, verification, limitations, open decisions); evidence in [evidence/2026-10-25-transformation/](evidence/2026-10-25-transformation/INDEX.md) |
| [REMEDIATION_REPORT_2026-10.md](REMEDIATION_REPORT_2026-10.md) | Previous: v6 remediation findings, fixes, verification |
| [DOCUMENTATION_INVENTORY.md](DOCUMENTATION_INVENTORY.md) | Record of the 2026-10-20 documentation consolidation (what was kept, merged, archived, deleted, and why) |
| [archive/RELEASE_REPORT_2026-10.md](archive/RELEASE_REPORT_2026-10.md) | Archived: release candidate, 2026-10-16/17 |
| [archive/FRONTEND_IMPROVEMENT_REPORT_2026-10.md](archive/FRONTEND_IMPROVEMENT_REPORT_2026-10.md) | Archived: frontend pass, 2026-10-17 |
| [archive/HOSTING_REPORT_2026-10.md](archive/HOSTING_REPORT_2026-10.md) | Archived: zero-cost hosting, 2026-10-17 (Indonesian) |
| [archive/UX_V3_REPORT_2026-10.md](archive/UX_V3_REPORT_2026-10.md) | Archived: UX v3 pass, 2026-10-17 (Indonesian) |
| [archive/course-milestones-README.md](archive/course-milestones-README.md) | Archived: original course-project README (LK-02 … LK-12) |

## Maintenance rules

- Change the authoritative document in the same change as the code; numbers that go stale (test
  counts, scan results) belong in a dated report, not in the topic documents.
- Several source files point at these paths (for example `settings.py` → `ENVIRONMENTS.md`,
  `storage/objects.py` → `STORAGE.md`, `MethodologyPage.tsx` → `FORECASTING_METHODOLOGY.md`,
  `legacy/README.md` → `MIGRATION.md`); do not rename them without updating those references.
- Never put tokens, passwords, secrets or real customer data in documents or screenshots.
