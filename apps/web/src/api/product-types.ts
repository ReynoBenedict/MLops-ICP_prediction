/**
 * Product API shapes beyond the generated index (verification, forecast read-time fields, insight
 * feed, plan requests, localized alerts), typed from the backend contract note (schema fixed
 * 2026-10-19). Shapes that the contracts now generate are aliased to them; every field stays optional on the
 * read side because older servers do not send it yet: the UI must
 * render honestly with or without it (e.g. "Bukti belum tercatat" when no verification data
 * exists). Once `packages/contracts` is regenerated these shapes should match the generated ones.
 *
 * Text arrives already localized: either via `?lang=` (insights) or as `{ id, en }` objects.
 * The frontend picks a language; it never composes a business sentence itself.
 */
import type { components } from "@icp/contracts";
import type { Locale } from "../i18n";
import { localizePeriods } from "../format";

type S = components["schemas"];

/** Generated from the contracts (schema name `Localized`). */
export type LocalizedText = S["Localized"];

/** Pick the active language from a server-localized text; `null` when the server sent nothing. */
export function loc(text: LocalizedText | string | null | undefined, locale: Locale): string | null {
  if (text == null) return null;
  const picked = typeof text === "string" ? text : (locale === "en" ? text.en : text.id) || text.id || text.en;
  // Month codes in server sentences ("2026-11") read as month labels ("Nov 2026").
  return picked ? localizePeriods(picked, locale) : null;
}

/* ---------- /data/verification ------------------------------------------------------------- */
export type Basis = "two_documents" | "decree_text" | "document_plus_legacy" | "single_document" | "ocr_single"
  | "conflict" | "no_evidence" | "provider_published";

export type VerificationMonth = S["VerificationMonth"];
export type VerificationResponse = S["VerificationResponse"];
/** Bases that mean "confirmed by official documents" (the only ones that may look confirmed). */
export const CONFIRMED_BASES: ReadonlySet<Basis> = new Set(["two_documents", "decree_text"]);

/* ---------- forecast read-time fields ----------------------------------------------------------- */
/**
 * Evaluation disclosure added by the backend (ux6/API-v6.md D1): test period, baseline on the same cases,
 * measured interval coverage and its warning, and a ready-made plain sentence. All optional: older
 * servers (and older stored runs) do not send them, and the UI then shows only what it has.
 */
export interface EvaluationDisclosure {
  origin_start?: string | null;
  origin_end?: string | null;
  baseline?: { method: string; label: LocalizedText; mae: number | null; n: number | null } | null;
  beats_baseline?: boolean | null;
  coverage_period?: { origin_start: string; origin_end: string } | null;
  under_coverage?: string[];
  coverage_warning?: LocalizedText | null;
  summary?: LocalizedText | null;
}
export type PointEvaluation = S["PointEvaluation"] & EvaluationDisclosure;
export type PointActual = S["PointActual"];
export interface PointReadFields {
  evaluation?: PointEvaluation | null;
  actual?: PointActual | null;
  kind_now?: string;
}
export type InputUsage = S["InputUsage"];
export interface ResultReadFields {
  notes?: { method?: LocalizedText; interval?: LocalizedText; evaluation?: LocalizedText; coverage_warning?: LocalizedText | null; ignored_inputs?: LocalizedText | null } | null;
  input_usage?: (InputUsage & { effect?: "used" | "ignored" | "rupiah_only" })[] | null;
  ignored_inputs?: string[] | null;
}
export interface RunPlanClip { plan_clip?: { horizon_max: number; stored_horizon: number; clipped: boolean } | null }

/* ---------- insights v2 ------------------------------------------------------------------------ */
export interface Fact { key: string; label: string; value: number | string | null; unit: string; display: string }
export interface InsightEvidence {
  datasets: Array<{ dataset: string; label: string; provider: string; version_id: string | null; period_end: string | null }>;
  periods: { subject: string | null; window_start: string | null; window_end: string | null; n: number | null };
  inputs: Fact[];
  outputs: Fact[];
  method: { key: string; text: string };
  model: { version: string; method: string } | null;
  forecast_ids: string[];
}
export interface FeedInsight {
  id: string;
  rule_id?: string;
  rule_version?: number;
  kind: string;
  importance?: "high" | "medium" | "low";
  severity: "warning" | "notice" | "info";
  audience?: "customer" | "operator";
  period: string | null;
  title: string;
  body: string;
  next_step: string | null;
  facts?: Fact[];
  evidence?: InsightEvidence | null;
  metrics: Array<{ label: string; value: number | string | null; unit: string }>;
  sources: Array<{ dataset?: string; label: string; provider?: string; version_id?: string | null; period_end?: string | null }>;
}
export interface InsightFeed {
  generated_at: string;
  lang?: "id" | "en";
  engine_version?: string;
  basis: { icp_period?: string | null } & Record<string, unknown>;
  insights: FeedInsight[];
  disclaimer: string;
}
/** Importance from the v2 field, or from the compatible severity of an older server. */
export function importanceOf(i: Pick<FeedInsight, "importance" | "severity">): "high" | "medium" | "low" {
  return i.importance ?? (i.severity === "warning" ? "high" : i.severity === "notice" ? "medium" : "low");
}

/* ---------- plan change requests ----------------------------------------------------------------- */
export type PlanCode = "trial" | "starter" | "professional" | "enterprise";
/** Generated from the contracts. */
export type PlanChangeRequest = S["PlanChangeRequest"];

/* ---------- alerts / plans ------------------------------------------------------------------------ */
export interface AlertLink { kind: "period" | "forecast" | "dataset"; period?: string; forecast_id?: string; dataset?: string }
export interface AlertEventLocalized { title_en?: string | null; body_en?: string | null; period?: string | null; link?: AlertLink | null }
export interface LabelEn { label_en?: string | null; price_note_en?: string | null }

/** Pick label_id / label_en by locale (falls back to the Indonesian label an older server sends). */
export function labelFor(o: { label_id?: string | null } & LabelEn, locale: Locale): string {
  return (locale === "en" ? o.label_en : null) ?? o.label_id ?? "";
}
