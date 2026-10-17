/**
 * Typed aliases for contract schemas not re-exported by `@icp/contracts`' index. Everything comes
 * from the generated `components` type, so the web app can never drift from the OpenAPI document.
 */
import type { components } from "@icp/contracts";

type S = components["schemas"];

export type {
  ActiveDatasets, AuditList, DataQualityResponse, DataSummary, DatasetVersionList, DriftResponse, ExperimentRunList, ForecastPoint,
  ForecastRun, ForecastRunList, HistoryPoint, HistoryResponse, ImportResponse, Job, JobList, ModelList, ModelVersion, ModelVersionDetail,
  PromotionList, PromotionResult, QualityReport, SystemStatus, ValidationReport, WhoAmI,
} from "@icp/contracts";

export type AuthResponse = S["AuthResponse"];
export type MeResponse = S["MeResponse"];
export type UserPublic = S["UserPublic"];
export type OrganizationDetail = S["OrganizationDetail"];
export type OrganizationList = S["OrganizationList"];
export type OrganizationPublic = S["OrganizationPublic"];
export type PlanInfo = S["PlanInfo"];
export type InviteResponse = S["InviteResponse"];
export type ApiKeyCreated = S["ApiKeyCreated"];
export type ApiKeyInfo = S["ApiKeyInfo"];
export type MarketSnapshot = S["MarketSnapshot"];
export type InsightsResponse = S["InsightsResponse"];
export type InsightItem = S["InsightItem"];
export type Scenario = S["Scenario"];
export type ScenarioList = S["ScenarioList"];
export type ScenarioCompareResponse = S["ScenarioCompareResponse"];
export type AlertKind = S["AlertKind"];
export type AlertRule = S["AlertRule"];
export type AlertEvent = S["AlertEvent"];
export type AlertEventList = S["AlertEventList"];
export type ReportRun = S["ReportRun"];
export type ReportList = S["ReportList"];
export type EvidenceResponse = S["EvidenceResponse"];
export type SourceEvidence = S["SourceEvidence"];
export type IngestionRunList = S["IngestionRunList"];
export type IngestionRun = S["IngestionRun"];
export type DatasetProvenance = S["DatasetProvenance"];
export type HorizonMetrics = S["HorizonMetrics"];
export type BacktestMetricRow = S["BacktestMetricRow"];
