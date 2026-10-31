/**
 * Is a scenario result still describing the scenario as it is saved now? (B-P1-1)
 *
 * A result is compared with the scenario's *assumptions* (inputs per month and horizon), not with its
 * `updated_at`: the server also bumps `updated_at` when it records a run or a comparison
 * (`last_forecast_id`), so a timestamp test would mark every result stale the moment it is made.
 * Renaming a scenario or changing its description does not change a result, so it is not "stale".
 *
 * Everything here is pure (no React), so the rules are unit-tested on their own.
 */
import type { ForecastRun, Scenario, ScenarioCompareResponse } from "../api/types";

type Inputs = Partial<Record<string, Partial<Record<string, number>> | null | undefined>> | null | undefined;

/** Canonical text of a scenario's assumptions: inputs and months sorted, empty inputs dropped, numbers normalised. */
export function inputsFingerprint(inputs: Inputs): string {
  const parts: string[] = [];
  for (const k of Object.keys(inputs ?? {}).sort()) {
    const months = inputs?.[k] ?? {};
    for (const p of Object.keys(months).sort()) {
      const v = months[p];
      if (typeof v === "number" && Number.isFinite(v)) parts.push(`${k}:${p}:${Number(v.toFixed(6))}`);
    }
  }
  return parts.join("|");
}

/** Fingerprint of everything that changes a scenario run: horizon and inputs. */
export function scenarioFingerprint(horizon: number, inputs: Inputs): string {
  return `h${horizon}#${inputsFingerprint(inputs)}`;
}

export type RunStale = { stale: false } | { stale: true; reason: "changed" | "archived" };

/**
 * A scenario run shown on the page against the scenario as currently listed. `current` is the
 * scenario from the latest list (`null` when a complete list no longer has it, i.e. archived),
 * `undefined` while the list is loading or unknown (then nothing is claimed either way).
 */
export function runStaleness(run: Pick<ForecastRun, "horizon" | "scenario">, current: Pick<Scenario, "horizon" | "inputs"> | null | undefined): RunStale {
  if (current === undefined) return { stale: false };
  if (current === null) return { stale: true, reason: "archived" };
  return scenarioFingerprint(run.horizon, run.scenario) === scenarioFingerprint(current.horizon, current.inputs)
    ? { stale: false } : { stale: true, reason: "changed" };
}

/** A compared scenario as the comparison response carries it. */
export interface ComparedScenario { id: string; name: string; inputs?: Inputs }

export interface CompareStale {
  stale: boolean;
  /** Compared scenarios whose inputs differ from the saved ones. */
  changed: ComparedScenario[];
  /** Compared scenarios no longer in the active list (archived). Only claimed from a complete list. */
  archived: ComparedScenario[];
  /** Compared scenarios missing from a list known to be partial: nothing is claimed about them. */
  unknown: ComparedScenario[];
  /** The comparison horizon (the longest of the compared scenarios) no longer matches. */
  horizonChanged: boolean;
  /** A newer official ICP month than the comparison's origin: the comparison predates the latest data. */
  newerOrigin: string | null;
  /** Ids still available for a new comparison, in the original order (unknown ones included). */
  activeIds: string[];
}

export interface CompareContext {
  /** False when the list could not be read to the end: a missing id is then unknown, not archived. */
  complete?: boolean;
  /** Latest official ICP month ("YYYY-MM"), when known. */
  latestOrigin?: string | null;
}

/**
 * A comparison against the active scenario list. The server compares every scenario on one horizon,
 * the longest of them (`cmp.horizon`), so a horizon edit matters only when it changes that maximum.
 * A comparison anchored on an older official month than the latest one is stale as well (it predates
 * the data). `list` undefined (still loading or unavailable): nothing is claimed.
 */
export function compareStaleness(
  cmp: Pick<ScenarioCompareResponse, "horizon" | "scenarios"> & { forecast_origin?: string | null },
  list: ReadonlyArray<Pick<Scenario, "id" | "horizon" | "inputs">> | undefined,
  ctx: CompareContext = {},
): CompareStale {
  const scenarios = (cmp.scenarios ?? []) as unknown as ComparedScenario[];
  const none: CompareStale = { stale: false, changed: [], archived: [], unknown: [], horizonChanged: false, newerOrigin: null, activeIds: scenarios.map((s) => s.id) };
  if (!list) return none;
  const complete = ctx.complete ?? true;
  const byId = new Map(list.map((s) => [s.id, s]));
  const changed: ComparedScenario[] = [];
  const archived: ComparedScenario[] = [];
  const unknown: ComparedScenario[] = [];
  for (const s of scenarios) {
    const cur = byId.get(s.id);
    if (!cur) (complete ? archived : unknown).push(s);
    // Older cached comparisons always carry inputs; without them nothing can be claimed for this one.
    else if (s.inputs !== undefined && inputsFingerprint(s.inputs) !== inputsFingerprint(cur.inputs)) changed.push(s);
  }
  const active = scenarios.map((s) => byId.get(s.id)).filter((s): s is NonNullable<typeof s> => !!s);
  // The comparison horizon is the maximum over every compared scenario: checkable only when all are known.
  const horizonChanged = !archived.length && !unknown.length && active.length > 0 && typeof cmp.horizon === "number"
    && Math.max(...active.map((s) => s.horizon)) !== cmp.horizon;
  const newerOrigin = ctx.latestOrigin && cmp.forecast_origin && ctx.latestOrigin > cmp.forecast_origin ? ctx.latestOrigin : null;
  return {
    stale: changed.length > 0 || archived.length > 0 || horizonChanged || !!newerOrigin,
    changed, archived, unknown, horizonChanged, newerOrigin,
    activeIds: scenarios.filter((s) => !archived.includes(s)).map((s) => s.id),
  };
}
