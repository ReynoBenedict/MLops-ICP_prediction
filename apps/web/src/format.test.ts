import { describe, expect, it } from "vitest";
import { fmtIdr, fmtPct, fmtUsd, monthRange, nextPeriods, period, shiftPeriod, toCsv } from "./format";

describe("format helpers", () => {
  it("formats numbers per locale with explicit units", () => {
    expect(fmtUsd(112.81, "id-ID")).toBe("USD 112,81");
    expect(fmtUsd(112.81, "en-US")).toBe("USD 112.81");
    expect(fmtIdr(1730696, "id-ID")).toBe("Rp 1.730.696");
    expect(fmtPct(-3.456, "id-ID")).toBe("−3,5 %");
    expect(fmtUsd(null, "id-ID")).toBe("-");
  });

  it("names months in Indonesian by default and steps across year boundaries", () => {
    expect(period("2026-03")).toBe("Mar 2026");
    expect(period("2026-08", "id", true)).toBe("Agustus 2026");
    expect(period("2026-05", "en")).toBe("May 2026");
    expect(nextPeriods("2026-11", 3)).toEqual(["2026-12", "2027-01", "2027-02"]);
    expect(shiftPeriod("2026-01", -1)).toBe("2025-12");
    expect(monthRange("2025-11", "2026-02")).toEqual(["2025-11", "2025-12", "2026-01", "2026-02"]);
  });

  it("builds Excel-friendly CSV with escaping", () => {
    const csv = toCsv(["a", "b"], [["x,y", 1], [null, "q\"t"]]);
    expect(csv.startsWith("﻿")).toBe(true);
    expect(csv).toContain('"x,y",1');
    expect(csv).toContain(',"q""t"');
  });
});
