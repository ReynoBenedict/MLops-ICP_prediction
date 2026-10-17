/**
 * Shared API contract. `schema.d.ts` is generated from `openapi.json`, which is exported from the
 * FastAPI backend (`npm run contracts`). Do not edit generated files by hand; CI fails when they drift
 * (`npm run contracts:check`).
 */
import type { components, paths } from "./schema";

export type { components, paths };
type Schemas = components["schemas"];

export type ErrorResponse = Schemas["ErrorResponse"];
export type ReadinessResponse = Schemas["ReadinessResponse"];
export type SystemStatus = Schemas["SystemStatus"];
export type DependencyStatus = Schemas["DependencyStatus"];
export type WhoAmI = Schemas["WhoAmI"];
export type DataSummary = Schemas["DataSummary"];
export type DatasetSummary = Schemas["DatasetSummary"];
export type HistoryResponse = Schemas["HistoryResponse"];
export type HistoryPoint = Schemas["HistoryPoint"];
export type QualityReport = Schemas["QualityReport"];
export type DataQualityResponse = Schemas["DataQualityResponse"];
export type ActiveDatasets = Schemas["ActiveDatasets"];
export type DatasetVersion = Schemas["DatasetVersion"];
export type DatasetVersionList = Schemas["DatasetVersionList"];
export type ImportResponse = Schemas["ImportResponse"];
export type ForecastRequest = Schemas["ForecastRequest"];
export type ForecastRun = Schemas["ForecastRun"];
export type ForecastRunList = Schemas["ForecastRunList"];
export type ForecastPoint = Schemas["ForecastPoint"];
export type ModelList = Schemas["ModelList"];
export type ModelVersion = Schemas["ModelVersion"];
export type ModelVersionDetail = Schemas["ModelVersionDetail"];
export type ExperimentRunList = Schemas["ExperimentRunList"];
export type ExperimentRun = Schemas["ExperimentRun"];
export type ValidationReport = Schemas["ValidationReport"];
export type PromotionResult = Schemas["PromotionResult"];
export type PromotionList = Schemas["PromotionList"];
export type DriftResponse = Schemas["DriftResponse"];
export type Job = Schemas["Job"];
export type JobList = Schemas["JobList"];
export type JobCreateRequest = Schemas["JobCreateRequest"];
export type AuditList = Schemas["AuditList"];
export type AuditEvent = Schemas["AuditEvent"];

/** Forecast horizon limits enforced by the backend (ForecastRequest.horizon). */
export const MAX_FORECAST_HORIZON = 6;
