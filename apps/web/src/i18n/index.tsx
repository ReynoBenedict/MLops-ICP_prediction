/**
 * Minimal i18n: Indonesian by default, English toggle, persisted per browser (localStorage
 * "icp.locale"). `t(key, vars)` interpolates `{name}` placeholders. Formatting helpers use the
 * matching Intl locale so numbers read "112,81" in Indonesian and "112.81" in English.
 */
import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from "react";
import { DICT, type Key, type Pair } from "./dictionary";

export type Locale = "id" | "en";
export const LOCALE_KEY = "icp.locale";

export function readLocale(): Locale {
  try {
    const v = localStorage.getItem(LOCALE_KEY);
    return v === "en" ? "en" : "id";
  } catch {
    return "id";
  }
}

/** Strings registered after start-up by lazily loaded areas (i18n/workspace.ts, i18n/ops.ts). */
const LAZY: Record<string, Pair> = {};
export function registerStrings(strings: Record<string, Pair>): void { Object.assign(LAZY, strings); }
const lookup = (key: string): Pair | undefined => (DICT as Record<string, Pair | undefined>)[key] ?? LAZY[key];

/**
 * Development builds name a missing string once in the console (usually a module that renders a
 * workspace or operator key without importing the module that registers it). Production builds drop
 * this branch (`import.meta.env.DEV` is replaced by `false`); it never throws.
 */
const warned = new Set<string>();
function warnMissing(key: string): void {
  if (!import.meta.env.DEV || warned.has(key)) return;
  warned.add(key);
  console.warn(`[i18n] missing string "${key}": no loaded dictionary defines it (lazy strings are registered by i18n/workspace.ts and i18n/ops.ts)`);
}

/** Plural choice inside a string: "{n|month|months}" is the first form when `n` is 1, else the second
 * (English needs it; Indonesian nouns do not change). Applied before the placeholders. */
const PLURAL = /\{(\w+)\|([^{}|]*)\|([^{}|]*)\}/g;

export function translate(locale: Locale, key: Key, vars?: Record<string, string | number | null | undefined>): string {
  const pair = lookup(key);
  if (!pair) {
    warnMissing(String(key));
    return String(key).split(".").pop()?.replaceAll("_", " ") ?? String(key);
  }
  let text: string = locale === "en" ? pair[1] : pair[0];
  if (text.includes("|")) text = text.replace(PLURAL, (_, k: string, one: string, other: string) => (Number(vars?.[k]) === 1 ? one : other));
  if (vars) {
    for (const [k, v] of Object.entries(vars)) text = text.replaceAll(`{${k}}`, v === null || v === undefined ? "-" : String(v));
  }
  return text;
}

/** Keys that exist for a dynamic suffix, e.g. status.<x>; falls back to the raw value. */
export function translateDynamic(locale: Locale, prefix: string, value: string | null | undefined, fallback?: string): string {
  if (!value) return fallback ?? "-";
  const key = `${prefix}.${value}` as Key;
  return lookup(key) ? translate(locale, key) : (fallback ?? value.replaceAll("_", " "));
}

interface I18n {
  locale: Locale;
  setLocale: (l: Locale) => void;
  t: (key: Key, vars?: Record<string, string | number | null | undefined>) => string;
  td: (prefix: string, value: string | null | undefined, fallback?: string) => string;
  intl: string;
}

const I18nContext = createContext<I18n>({
  locale: "id", setLocale: () => undefined, t: (k, v) => translate("id", k, v), td: (p, v, f) => translateDynamic("id", p, v, f), intl: "id-ID",
});

export function I18nProvider({ children, initial }: { children: ReactNode; initial?: Locale }) {
  const [locale, setLocaleState] = useState<Locale>(initial ?? readLocale());
  const setLocale = useCallback((l: Locale) => {
    setLocaleState(l);
    try { localStorage.setItem(LOCALE_KEY, l); } catch { /* per-browser convenience only */ }
    document.documentElement.lang = l;
  }, []);
  const value = useMemo<I18n>(() => ({
    locale, setLocale,
    t: (key, vars) => translate(locale, key, vars),
    td: (prefix, v, fallback) => translateDynamic(locale, prefix, v, fallback),
    intl: locale === "en" ? "en-US" : "id-ID",
  }), [locale, setLocale]);
  return <I18nContext.Provider value={value}>{children}</I18nContext.Provider>;
}

export function useT(): I18n {
  return useContext(I18nContext);
}

export type { Key };
