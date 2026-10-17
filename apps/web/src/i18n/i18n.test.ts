import { describe, expect, it } from "vitest";
import { DICT } from "./dictionary";
import { translate, translateDynamic } from "./index";

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
});
