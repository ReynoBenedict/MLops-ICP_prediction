/**
 * Plan-aware loading. Some endpoints need a plan feature (insights); asking without it only
 * produces a 403 in the console. Customers' plan features come from the public /plans list.
 */
import type { InsightsResponse, PlanInfo } from "../api/types";
import { api } from "../api/client";
import type { Principal } from "../auth";

export const NOT_IN_PLAN = "not_in_plan" as const;

/** Insights, or NOT_IN_PLAN when the customer's plan does not include them (operators always load). */
export async function loadInsights(principal: Principal, signal: AbortSignal): Promise<InsightsResponse | typeof NOT_IN_PLAN> {
  if (principal.kind === "user" && principal.plan) {
    const plans = await api<PlanInfo[]>("/plans", { signal, anonymous: true });
    const plan = plans.find((p) => p.code === principal.plan);
    if (plan && !plan.insights) return NOT_IN_PLAN;
  }
  return api<InsightsResponse>("/insights", { signal });
}
