/**
 * Plan-aware loading. Some endpoints need a plan feature (insights); asking without it only
 * produces a 403 in the console. Customers' plan features come from the public /plans list.
 */
import type { InsightsResponse, PlanInfo } from "../api/types";
import type { InsightFeed } from "../api/product-types";
import { api, publicGet } from "../api/client";
import type { Principal } from "../auth";
import type { Locale } from "../i18n";

export const NOT_IN_PLAN = "not_in_plan" as const;

/** v1 contract shape (what older callers read) plus the optional v2 fields (importance, facts, evidence). */
export type InsightsPayload = InsightsResponse & InsightFeed;

/**
 * Insights in the requested language (`?lang=`; the server writes every sentence), or NOT_IN_PLAN
 * when the customer's plan does not include them (operators always load). An older server ignores
 * `lang` and answers in Indonesian with the v1 shape; the v2 fields are then simply absent.
 */
export async function loadInsights(principal: Principal, signal: AbortSignal, lang: Locale = "id"): Promise<InsightsPayload | typeof NOT_IN_PLAN> {
  if (principal.kind === "user" && principal.plan) {
    // The public plan catalogue is kept in memory for a few minutes (publicGet), so only the first
    // page of a visit waits for it before asking for insights.
    const plans = await publicGet<PlanInfo[]>("/plans", { signal });
    const plan = plans.find((p) => p.code === principal.plan);
    if (plan && !plan.insights) return NOT_IN_PLAN;
  }
  return api<InsightsPayload>("/insights", { signal, query: { lang } });
}
