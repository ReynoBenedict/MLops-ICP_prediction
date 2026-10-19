/**
 * Locale-aware formatting. Values are never invented: null/undefined render as the NA placeholder
 * (a plain hyphen, so no typographic dashes reach the UI).
 * All helpers take the Intl locale ("id-ID" | "en-US") from the i18n context.
 */
import type { Locale } from "./i18n";

/** Placeholder for a value that does not exist. */
export const NA = "-";

/** "Apr 2026 s.d. Jun 2026" (id) / "Apr 2026 to Jun 2026" (en). */
export function rangeText(a: string, b: string, locale: Locale = "id"): string {
  return `${a} ${locale === "en" ? "to" : "s.d."} ${b}`;
}

const MONTHS: Record<Locale, string[]> = {
  id: ["Jan", "Feb", "Mar", "Apr", "Mei", "Jun", "Jul", "Agu", "Sep", "Okt", "Nov", "Des"],
  en: ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
};
const MONTHS_LONG: Record<Locale, string[]> = {
  id: ["Januari", "Februari", "Maret", "April", "Mei", "Juni", "Juli", "Agustus", "September", "Oktober", "November", "Desember"],
  en: ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"],
};

const isNum = (v: unknown): v is number => typeof v === "number" && Number.isFinite(v);

export function fmtNum(v: number | null | undefined, intl: string, digits = 2): string {
  return isNum(v) ? new Intl.NumberFormat(intl, { minimumFractionDigits: digits, maximumFractionDigits: digits }).format(v) : NA;
}

/** "USD 112,81" (id) / "USD 112.81" (en). Unit kept as an explicit ISO code, never a bare "$". */
export function fmtUsd(v: number | null | undefined, intl: string, digits = 2): string {
  return isNum(v) ? `USD ${fmtNum(v, intl, digits)}` : NA;
}

/** "USD 103,32 s.d. 121,85" (id) / "USD 103.32 to 121.85" (en): a range always carries its unit. */
export function fmtUsdRange(lo: number | null | undefined, hi: number | null | undefined, intl: string, locale: Locale = "id"): string {
  return isNum(lo) && isNum(hi) ? `USD ${rangeText(fmtNum(lo, intl), fmtNum(hi, intl), locale)}` : NA;
}

export function fmtIdr(v: number | null | undefined, intl: string, digits = 0): string {
  return isNum(v) ? `Rp ${fmtNum(v, intl, digits)}` : NA;
}

export function fmtPct(v: number | null | undefined, intl: string, digits = 1, sign = true): string {
  if (!isNum(v)) return NA;
  const s = fmtNum(Math.abs(v), intl, digits);
  return `${sign ? (v > 0 ? "+" : v < 0 ? "−" : "") : ""}${s} %`;
}

export function fmtSigned(v: number | null | undefined, intl: string, digits = 2): string {
  if (!isNum(v)) return NA;
  return `${v > 0 ? "+" : v < 0 ? "−" : ""}${fmtNum(Math.abs(v), intl, digits)}`;
}

/** "2026-03" -> "Mar 2026" (short) or "Maret 2026" (long). */
export function period(p: string | null | undefined, locale: Locale = "id", long = false): string {
  if (!p) return NA;
  const [y, m] = p.split("-");
  const idx = Number(m) - 1;
  const names = long ? MONTHS_LONG[locale] : MONTHS[locale];
  return names[idx] ? `${names[idx]} ${y}` : p;
}

export function dateTime(iso: string | null | undefined, intl = "id-ID"): string {
  if (!iso) return NA;
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString(intl, { dateStyle: "medium", timeStyle: "short" });
}

export function dateOnly(iso: string | null | undefined, intl = "id-ID"): string {
  if (!iso) return NA;
  const d = new Date(iso.length === 10 ? `${iso}T00:00:00` : iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleDateString(intl, { dateStyle: "long" });
}

export function fromMs(ms: number | null | undefined, intl = "id-ID"): string {
  return ms ? dateTime(new Date(ms).toISOString(), intl) : NA;
}

export const shortHash = (h: string | null | undefined): string => (h ? h.slice(0, 12) : NA);

/** Next n monthly periods after `p` ("2026-03" -> ["2026-04", ...]). */
export function nextPeriods(p: string, n: number): string[] {
  const [y, m] = p.split("-").map(Number) as [number, number];
  const out: string[] = [];
  for (let i = 1; i <= n; i++) {
    const total = y * 12 + (m - 1) + i;
    out.push(`${Math.floor(total / 12)}-${String((total % 12) + 1).padStart(2, "0")}`);
  }
  return out;
}

export function shiftPeriod(p: string, months: number): string {
  const [y, m] = p.split("-").map(Number) as [number, number];
  const total = y * 12 + (m - 1) + months;
  return `${Math.floor(total / 12)}-${String((total % 12) + 1).padStart(2, "0")}`;
}

/** Every month from `start` to `end` inclusive (used to keep calendar gaps visible in charts). */
export function monthRange(start: string, end: string): string[] {
  const out = [start];
  while (out.length < 1200 && out[out.length - 1]! < end) out.push(shiftPeriod(out[out.length - 1]!, 1));
  return out;
}

/** Current calendar month as YYYY-MM (used to tell elapsed months from future ones client-side). */
export function currentPeriod(now = new Date()): string {
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
}

/** Build a CSV string (UTF-8 with BOM for Excel) from rows of plain values. */
export function toCsv(headers: string[], rows: Array<Array<string | number | null | undefined>>): string {
  const esc = (v: string | number | null | undefined) => {
    if (v === null || v === undefined) return "";
    const s = String(v);
    return /[",\n;]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
  };
  return "﻿" + [headers.map(esc).join(","), ...rows.map((r) => r.map(esc).join(","))].join("\n");
}

export function downloadText(filename: string, text: string, mime = "text/csv;charset=utf-8"): void {
  const blob = new Blob([text], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

const ISSUE_LABELS: Record<string, [string, string]> = {
  MISSING_PERIODS: ["Bulan hilang", "Missing months"],
  LARGE_MOM_CHANGE: ["Perubahan bulanan besar", "Large month-over-month moves"],
  VALUE_UNUSUAL: ["Nilai tidak biasa", "Unusual values"],
  VALUE_OUT_OF_RANGE: ["Nilai di luar rentang", "Values out of range"],
  UNSORTED: ["Baris tidak kronologis", "Rows not in chronological order"],
  DUPLICATE_PERIODS: ["Bulan ganda", "Duplicate months"],
  NULL_VALUES: ["Nilai kosong", "Missing values"],
  INVALID_PERIOD: ["Tanggal tidak valid", "Invalid dates"],
  SCHEMA_MISSING_COLUMNS: ["Kolom hilang", "Missing columns"],
  CSV_PARSE_ERROR: ["CSV tidak terbaca", "Unreadable CSV"],
  EMPTY: ["Dataset kosong", "Empty dataset"],
};

/** Readable title for a data-quality issue code returned by the API. */
export function issueLabel(code: string, locale: Locale = "id"): string {
  const pair = ISSUE_LABELS[code];
  if (pair) return locale === "en" ? pair[1] : pair[0];
  return code.replaceAll("_", " ").toLowerCase().replace(/^./, (c) => c.toUpperCase());
}
