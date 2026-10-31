import { describe, expect, it, vi } from "vitest";
import { DICT as CORE, MODULES as CORE_MODULES } from "./dictionary";
import { translate, translateDynamic, type Key } from "./index";
import { OPS } from "./ops";
import { WORKSPACE, WORKSPACE_MODULES } from "./workspace";

// The workspace and operator-console strings are registered lazily; the checks below cover every string.
const DICT = { ...CORE, ...WORKSPACE, ...OPS };
const MODULES = { ...CORE_MODULES, ...WORKSPACE_MODULES, OPS };

const JARGON = /\b(RMSE|PSI|MLflow|artifact registry|baseline naive|pipeline execution)\b/i;

describe("i18n dictionary", () => {
  it("has a non-empty Indonesian and English string for every key", () => {
    for (const [key, pair] of Object.entries(DICT)) {
      expect(pair, key).toHaveLength(2);
      expect(pair[0].trim(), `${key} (id)`).not.toBe("");
      expect(pair[1].trim(), `${key} (en)`).not.toBe("");
    }
  });

  it("uses the same placeholders in both languages", () => {
    const ph = (s: string) => [...s.matchAll(/\{(\w+)\}/g)].map((m) => m[1]).sort();
    for (const [key, pair] of Object.entries(DICT)) expect(ph(pair[0]), key).toEqual(ph(pair[1]));
  });

  it("keeps ML jargon out of the customer-facing namespaces", () => {
    const business = Object.entries(DICT).filter(([k]) => /^(nav\.(?!ops)|overview|explorer|forecast|scen|ins|rep|alert|acct|landing)\./.test(k));
    expect(business.length).toBeGreaterThan(100);
    for (const [key, pair] of business) expect(pair[0], key).not.toMatch(JARGON);
  });

  it("uses no em dashes or spaced en dashes in customer-facing copy (either language)", () => {
    const business = Object.entries(DICT).filter(([k]) => /^(nav\.(?!ops)|shell|common|error|kind|src|fx|overview|explorer|forecast|scen|ins|rep|alert|acct|landing|auth|legal|ds)\./.test(k));
    expect(business.length).toBeGreaterThan(300);
    for (const [key, pair] of business) {
      expect(pair[0], `${key} (id)`).not.toMatch(/—| – /);
      expect(pair[1], `${key} (en)`).not.toMatch(/—| – /);
    }
  });

  it("interpolates placeholders and falls back for dynamic keys", () => {
    expect(translate("id", "forecast.horizonMonths", { n: 3 })).toBe("3 bulan");
    expect(translate("en", "forecast.horizonMonths", { n: 3 })).toBe("3 months");
    expect(translateDynamic("id", "status", "fresh")).toBe("Mutakhir");
    expect(translateDynamic("en", "status", "not_a_status")).toBe("not a status");
  });

  it("picks English plural forms instead of writing \"(s)\" (UX-11)", () => {
    expect(translate("en", "shell.status.behind", { n: 1 })).toBe("1 month behind");
    expect(translate("en", "shell.status.behind", { n: 3 })).toBe("3 months behind");
    expect(translate("en", "overview.qualityPending", { n: 1, list: "Mar 2024" })).toBe("1 month in your plan range is not confirmed by two ESDM documents: Mar 2024");
    expect(translate("en", "overview.qualityPending", { n: 7, list: "…" })).toBe("7 months in your plan range are not confirmed by two ESDM documents: …");
    expect(translate("en", "form.fixFields", { n: "1" })).toBe("Fix 1 highlighted field.");
    expect(translate("id", "shell.status.behind", { n: 3 })).toBe("tertinggal 3 bulan");
    for (const [key, pair] of Object.entries(DICT)) expect(pair[1], key).not.toContain("(s)");
  });

  it("is split into domain modules that never define the same key twice", () => {
    const counts = new Map<string, number>();
    for (const mod of Object.values(MODULES)) for (const k of Object.keys(mod)) counts.set(k, (counts.get(k) ?? 0) + 1);
    const dupes = [...counts].filter(([, n]) => n > 1).map(([k]) => k);
    expect(dupes).toEqual([]);
    expect(Object.keys(DICT).length).toBe([...counts.keys()].length);
    expect(Object.keys(MODULES).length).toBeGreaterThanOrEqual(10);
  });

  it("keeps each key prefix in one module (the domain that owns it)", () => {
    const owner = new Map<string, string>();
    for (const [name, mod] of Object.entries(MODULES)) {
      for (const k of Object.keys(mod)) {
        const prefix = k.split(".")[0]!;
        const prev = owner.get(prefix);
        expect(prev === undefined || prev === name, `${k} in ${name}, prefix owned by ${prev}`).toBe(true);
        owner.set(prefix, name);
      }
    }
  });

  it("holds no insight narratives (insight text comes localized from the server)", () => {
    for (const [key, pair] of Object.entries(DICT)) {
      if (!key.startsWith("ins.")) continue;
      expect(pair[0], key).not.toMatch(/\d+[,.]\d+ ?%|naik \+|turun −|melebar|menyempit/i);
    }
  });

  it("keeps operator-console strings out of the eager dictionary (customers never download them)", () => {
    expect(Object.keys(CORE).filter((k) => k.startsWith("ops."))).toEqual([]);
    expect(Object.keys(OPS).length).toBeGreaterThan(100);
    expect(translate("en", "ops.health.title")).toBe("System health");
  });

  it("keeps workspace strings out of the eager dictionary (the public site never downloads them)", () => {
    const lazy = new Set(Object.keys(WORKSPACE).map((k) => k.split(".")[0]));
    expect([...lazy].sort()).toEqual(["acct", "alert", "ds", "explorer", "forecast", "fx", "history", "ins", "interval", "onboard", "overview", "provider", "rep", "scen", "sess"]);
    expect(Object.keys(CORE).filter((k) => lazy.has(k.split(".")[0]))).toEqual([]);
    expect(Object.keys(WORKSPACE).length).toBeGreaterThan(500);
    expect(translate("en", "overview.title")).toBe("Market overview");
  });

  it("names a missing string once in development and still renders a fallback", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      const missing = "test.missing_string" as Key;
      expect(translate("id", missing)).toBe("missing string");
      expect(translate("en", missing)).toBe("missing string");
      expect(warn).toHaveBeenCalledTimes(1);
      expect(String(warn.mock.calls[0]?.[0])).toContain('"test.missing_string"');
      expect(translate("en", "common.retry")).toBe("Retry");
      expect(warn).toHaveBeenCalledTimes(1);
    } finally {
      warn.mockRestore();
    }
  });
});
