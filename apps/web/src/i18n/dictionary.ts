/**
 * UI strings, split by domain (shell, common, market, forecast, ...). Each module maps keys to
 * [Indonesian, English]; this file merges the eagerly loaded ones into one typed DICT so `t("…")` keys
 * stay stable. Keeping both languages in one tuple guarantees the dictionaries can never drift apart,
 * and the i18n test checks that no key is defined in two modules. Placeholders use {name}.
 *
 * Only what the public site needs is merged here (shell, common, value provenance, legal, auth,
 * landing, methodology). Two groups register lazily instead, so the public site never downloads them;
 * their keys are still part of the `Key` type:
 * - workspace strings (market, forecast, scenario, insights, reports, account): `i18n/workspace.ts`,
 *   imported by the lazily loaded shell and by every lazily loaded workspace page;
 * - operator-console strings (OPS): `i18n/ops.ts`, imported by the /operasi pages.
 * `src/i18n/lazy-strings.test.ts` walks the static import graph and fails when a module uses a key
 * whose dictionary is not loaded with it.
 *
 * Narrative text produced from data (insights, alert messages, verification labels) is NOT kept
 * here: the backend returns it already localized.
 */
import { AUTH } from "./auth";
import { COMMON } from "./common";
import { LANDING } from "./landing";
import { LEGAL } from "./legal";
import { METHODOLOGY } from "./methodology";
import type { OPS } from "./ops";
import type { Pair } from "./pair";
import { PROVENANCE } from "./provenance";
import { SHELL } from "./shell";
import type { WORKSPACE } from "./workspace";

export type { Pair };

/** Every eagerly loaded domain module, in merge order (exported for the duplicate-key test). */
export const MODULES = { SHELL, COMMON, PROVENANCE, LEGAL, AUTH, LANDING, METHODOLOGY } as const;

export const DICT = {
  ...SHELL, ...COMMON, ...PROVENANCE, ...LEGAL, ...AUTH, ...LANDING, ...METHODOLOGY,
} as const satisfies Record<string, Pair>;

/** Every key, including the lazily registered workspace and operator-console strings. */
export type Key = keyof typeof DICT | keyof typeof WORKSPACE | keyof typeof OPS;
