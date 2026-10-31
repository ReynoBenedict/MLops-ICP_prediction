# Insight engine and rule catalogue

Insights are produced by a **deterministic rule engine** in the backend (`backend/icp_api/insights`).
No language model or external AI service is used. Every insight is traceable to dataset versions,
forecast ids and model versions, and no template asserts a cause.

## Pipeline

```
services/insights.py   loads an immutable InsightContext once per request (datasets, forecasts, metrics)
insights/rules.py      rules: InsightContext -> Finding | None (numbers only)
insights/validate.py   evidence contract: findings without complete evidence are dropped and logged
insights/consistency.py dedupe, folding, direction agreement, audience filter, ordering, caps
insights/narrative.py  localized text from templates_id.py / templates_en.py (same keys and slots, test-enforced)
```

- **Engine version:** `ENGINE_VERSION` in `insights/__init__.py` (currently `2026-10.2`; `2026-10.1` was the v5 engine), returned as
  `engine_version` with every response; each item also carries `rule_id` and `rule_version`.
- **API:** `GET /api/v1/insights?lang=id|en` (plan feature `insights`; `403 FEATURE_NOT_IN_PLAN`
  otherwise). Text is already in the requested language; the gateway does not forward
  `Accept-Language`. Contract: `packages/contracts/openapi.json` (`InsightsResponse`).
- **Item shape:** `title`, `body`, `next_step` (plain text, no markup), `facts[]` (headline numbers
  with a ready `display` string), `evidence` (datasets with provider and version, period window and
  `n`, inputs → outputs, plain-language `method.text`, model version, forecast ids), `importance`,
  `audience`.
- **Audience:** operator-only findings (`ingestion_failure`) are returned only to operator tokens.
  Tenant-facing text never contains raw ingestion error text.
- **Wording rules:** no causal words ("karena", "disebabkan", "because", "due to") and no statistical
  jargon ("z", "simpangan baku") in customer text; importance reflects the consequence for reading
  data or forecasts, never the size of a price move alone.

## Evidence contract (since engine 2026-10.2)

`validate_finding` (`insights/validate.py`) publishes a finding only when it carries: at least one dataset
with a version id (except `ingestion_failure` for a dataset without an active version, whose run id is the
input), a period window with start and end, the inputs the rule read, the outputs it computed, a known
method key, and only finite numbers. `build_insights` drops and logs anything else. Before this, five rules
could publish incomplete evidence (`model_evaluation`: no dataset, inputs or window; `forecast_outlook` and
`forecast_revision`: no window; `benchmark_freshness`: no window or outputs; `ingestion_failure`: no inputs
or window). They now carry full evidence: `model_evaluation` uses the training dataset versions from the
registry tags and the backtest origins, the outlook uses origin → target, the revision uses old origin →
target, freshness uses latest → expected month, and the ingestion finding uses the run month.

Wording: no accuracy headline without its baseline, n and period ("Kesalahan model aktif … dibanding acuan
sederhana" instead of "Akurasi model aktif"); forecast intervals are never called calibrated.

## Rule catalogue

| Rule | Definition | Minimum data / threshold | Not generated when | Importance |
|---|---|---|---|---|
| `icp_mom` | ICP M vs M-1 | both months | M-1 missing; folded into `icp_extreme_move` for the same month | low |
| `icp_yoy` | ICP M vs M-12 | both months | M-12 missing | low |
| `icp_trend_run` | consecutive same-direction monthly changes ending at M | ≥ 3 moves, no gaps | fewer moves, a gap or a flat month | low |
| `icp_extreme_move` | M's change is the strictly largest of the consecutive changes in the last 24 months | ≥ 12 changes | not strictly largest | medium (uncertainty ranges from historical errors can understate risk) |
| `spread_vs_history` | ICP minus benchmark at M vs min/max of the previous 36 months | ≥ 24 paired months; outside the range | spread within range | medium if the production method uses that benchmark, else low |
| `fx_context` | ICP × monthly average JISDOR, M vs M-1 | FX and ICP for both months | missing FX | low |
| `forecast_outlook` | first point of the latest system forecast + backtest MAE, 80 % coverage and direction hit rate for the same step and mode | forecast anchored on the latest official month; stored metrics | stale forecast or no metrics | medium; a direction hit rate < 60 % is described as close to chance |
| `forecast_revision` | largest change for a shared month between the last two system forecasts | ≥ 0.5 × backtest MAE for that step | below threshold / no metrics | medium; lists recorded differences (model, origin, data versions, inputs), never causes; not emitted for a month that ESDM has already published |
| `data_freshness` | ICP behind its publication schedule (lag 1 month + 1 month grace) | – | fresh | low if lagging (normal early in a month), high if stale |
| `benchmark_freshness` | benchmark/FX stale | – | none stale | medium |
| `data_verification` | active ICP months not backed by two ESDM documents or decree text | – | all confirmed | medium if a conflict/OCR/no-evidence month is in the last 12 months, else low |
| `model_evaluation` | production hold-last h1 MAE vs repeating last month's ICP | stored metrics | no metrics | low |
| `ingestion_failure` | latest run of a dataset failed (no raw error text) | – | last run succeeded | high, operators only |

## Consistency

One finding per id; month-over-month is folded into the largest-move finding; the move direction of
`icp_mom` / `icp_trend_run` / `icp_extreme_move` must agree (contradictions are dropped and logged);
customers receive at most 8 findings ordered by importance, then kind.

## Tests

- `backend/tests/test_insight_engine.py`: rules on synthetic series, template key/slot parity, wording bans,
  consistency.
- `backend/tests/test_d_insight_quality.py`: validator matrix (10 rejections, 1 acceptance), drop-and-log,
  boundary tests for every threshold (trend 3, extreme window 24, spread edge, revision exactly 0.5 × MAE,
  direction 0.6, verification 12-month window, under-coverage 0.10, coverage n 20), **300 seeded random
  contexts** (one test item per seed: no contradictions in strict mode, unique ids, no causal or
  overclaiming words, no NaN/inf, complete evidence, nothing silently dropped) and an **overclaim sweep of
  322 API-provided texts** (one item per text). These two parametrised tests account for 622 of the 1,001
  collected Python test items.
- Access gating: `backend/tests/test_access_control.py`; the plan gate (insights off → `403
  FEATURE_NOT_IN_PLAN`) in `backend/tests/test_d_plan_matrix.py`. The `market_monthly` report embeds
  insights only when the plan includes them (`test_operations.py`).

Methodology context (forecast semantics, backtest metrics the outlook rule quotes):
[FORECASTING_METHODOLOGY.md](FORECASTING_METHODOLOGY.md).
