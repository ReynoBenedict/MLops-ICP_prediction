/**
 * Locale-tolerant number entry. Users type "95,5" (Indonesian) or "95.5" (English) and both mean
 * 95.5; a field that expects large values (exchange rates) also accepts thousands grouping such as
 * "16.500" or "16,500". The raw text is always kept by the caller, so a trailing separator typed
 * mid-number never disappears.
 */

/** Parse user text: `null` for an empty field, `NaN` for text that is not a number. */
export function parseDecimal(raw: string, opts: { grouping?: boolean } = {}): number | null {
  const s = raw.trim().replace(/\s/g, "");
  if (!s) return null;
  if (!/^[+-]?[\d.,]+$/.test(s) || !/\d/.test(s)) return NaN;
  const sign = s.startsWith("-") ? -1 : 1;
  const body = s.replace(/^[+-]/, "");
  const dots = body.split(".").length - 1;
  const commas = body.split(",").length - 1;
  let intPart: string;
  let frac = "";
  if (dots && commas) {
    // Both separators: the last one is the decimal separator, the other groups thousands.
    const last = Math.max(body.lastIndexOf("."), body.lastIndexOf(","));
    intPart = body.slice(0, last).replace(/[.,]/g, "");
    frac = body.slice(last + 1);
    if (/[.,]/.test(frac)) return NaN;
  } else if (!dots && !commas) {
    intPart = body;
  } else {
    const parts = body.split(dots ? "." : ",");
    const head = parts[0] ?? "";
    const tail = parts.slice(1);
    if (tail.length > 1) {
      // "1.234.567": only valid as thousands grouping.
      if (head.length < 1 || head.length > 3 || !tail.every((p) => p.length === 3)) return NaN;
      intPart = parts.join("");
    } else if (opts.grouping && head.length >= 1 && head.length <= 3 && tail[0]?.length === 3) {
      intPart = head + tail[0];
    } else {
      intPart = head;
      frac = tail[0] ?? "";
    }
  }
  if (!intPart && !frac) return NaN;
  const v = Number(`${intPart || "0"}.${frac || "0"}`);
  return Number.isFinite(v) ? sign * v : NaN;
}

/** Plain text for an input field (no grouping, locale decimal separator). */
export function formatForInput(v: number | null | undefined, intl: string): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return "";
  return new Intl.NumberFormat(intl, { useGrouping: false, maximumFractionDigits: 6 }).format(v);
}

export type NumberCheck = { value: number | null; error: null } | { value: null; error: "invalid" | "range" };

/** Validate text against a plausible range; empty text is valid and means "no value". */
export function checkNumber(raw: string, range: { min: number; max: number; grouping?: boolean }): NumberCheck {
  const v = parseDecimal(raw, { grouping: range.grouping });
  if (v === null) return { value: null, error: null };
  if (Number.isNaN(v)) return { value: null, error: "invalid" };
  if (v < range.min || v > range.max) return { value: null, error: "range" };
  return { value: v, error: null };
}
