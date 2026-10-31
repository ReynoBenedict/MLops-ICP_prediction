/**
 * The organisation's saved scenarios, all of them. Paid plans allow 100 or 1,000 saved scenarios and
 * the server returns at most 100 per page, so the list is read page by page (`next_cursor`) instead of
 * stopping at the first page (W2-R5: older scenarios could not be seen or run, and a compared scenario
 * beyond the first page was called "archived").
 *
 * `complete` is false only when the page budget ran out before the last page: then a scenario missing
 * from `items` is unknown, not archived, and callers must not claim otherwise.
 */
import type { Scenario, ScenarioList } from "../api/types";
import { api } from "../api/client";

/** The server's maximum page size for lists (`limit` le=100). */
export const SCENARIO_PAGE_SIZE = 100;
/** Enough pages for the largest plan (1,000 saved scenarios) with room to spare. */
export const SCENARIO_MAX_PAGES = 20;

export interface ScenarioCatalogue {
  items: Scenario[];
  /** True when every page was read: a scenario that is not listed is archived (or never existed). */
  complete: boolean;
}

export async function fetchAllScenarios(signal?: AbortSignal): Promise<ScenarioCatalogue> {
  const byId = new Map<string, Scenario>();
  let cursor: string | null = null;
  for (let page = 0; page < SCENARIO_MAX_PAGES; page++) {
    const res: ScenarioList = await api<ScenarioList>("/scenarios", { query: { limit: SCENARIO_PAGE_SIZE, cursor }, signal });
    for (const s of res.items) if (!byId.has(s.id)) byId.set(s.id, s);
    cursor = res.next_cursor ?? null;
    if (!cursor) return { items: [...byId.values()], complete: true };
  }
  return { items: [...byId.values()], complete: false };
}
