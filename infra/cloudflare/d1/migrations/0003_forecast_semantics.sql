-- Forecast semantics (2026-10): forecast origin vs issue date, method identity, scenario inputs and
-- the dataset versions every input came from. Existing forecast records are preserved unchanged
-- (new columns are nullable); they keep their original params/result JSON.
ALTER TABLE forecast_runs ADD COLUMN issue_date TEXT;             -- YYYY-MM-DD the forecast was produced
ALTER TABLE forecast_runs ADD COLUMN forecast_origin TEXT;        -- last official ICP month used (YYYY-MM)
ALTER TABLE forecast_runs ADD COLUMN method TEXT;                 -- forecasting method name of the model version
ALTER TABLE forecast_runs ADD COLUMN scenario_json TEXT;          -- user-supplied benchmark/FX assumptions
ALTER TABLE forecast_runs ADD COLUMN dataset_versions_json TEXT;  -- {dataset: version_id} for every input used
ALTER TABLE forecast_runs ADD COLUMN workflow TEXT;               -- api | scheduled | report

CREATE INDEX ix_forecast_runs_origin ON forecast_runs (forecast_origin, created_at DESC);
CREATE INDEX ix_forecast_runs_model ON forecast_runs (model_version, created_at DESC);

INSERT INTO app_schema (version, name, applied_at) VALUES (3, '0003_forecast_semantics', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
