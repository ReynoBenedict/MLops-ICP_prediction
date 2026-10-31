/**
 * Workspace strings (market pages, forecast, scenarios, insights, reports and notifications, account):
 * [Indonesian, English]. Not part of the eager DICT: the signed-in shell (components/layout.tsx) and
 * every lazily loaded workspace page import this module, which registers the strings when their chunk
 * loads, so the public site never downloads them. Their keys are still part of the `Key` type
 * (dictionary.ts). Strings that public pages need as well live in eager modules (provenance.ts,
 * methodology.ts); src/i18n/lazy-strings.test.ts fails when a module uses a key before it is registered.
 */
import { ACCOUNT } from "./account";
import { FORECAST } from "./forecast";
import { registerStrings } from "./index";
import { INSIGHTS } from "./insights";
import { MARKET } from "./market";
import type { Pair } from "./pair";
import { REPORTS } from "./reports";
import { SCENARIO } from "./scenario";

/** The workspace domain modules, in merge order (exported for the duplicate-key test). */
export const WORKSPACE_MODULES = { MARKET, FORECAST, SCENARIO, INSIGHTS, REPORTS, ACCOUNT } as const;

export const WORKSPACE = {
  ...MARKET, ...FORECAST, ...SCENARIO, ...INSIGHTS, ...REPORTS, ...ACCOUNT,
} as const satisfies Record<string, Pair>;

registerStrings(WORKSPACE);
