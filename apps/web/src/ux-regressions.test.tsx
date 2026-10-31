/**
 * Mission v5 regressions: modal navigation drawer, accessible tabs, focus after navigation,
 * honest forecast presentation (assumed Brent, official actuals, conditional ranges), record-card
 * tables, verification labels that never overclaim, URL state, chart text alternatives, localized
 * rate-limit errors, the onboarding card and the unsaved-changes guard.
 */
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { useState } from "react";
import { MemoryRouter, useLocation } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import { ApiError } from "./api/client";
import type { ForecastRun, HistoryPoint } from "./api/types";
import { AuthProvider } from "./auth";
import { niceTicks, TimeSeriesChart } from "./components/charts";
import { AssumptionNote, buildForecastRows, ForecastTable } from "./components/forecast";
import { ToastProvider } from "./components/toast";
import { Button, Delta, ErrorState, tabPanelProps, Tabs } from "./components/ui";
import { I18nProvider } from "./i18n";

function json(body: unknown, status = 200, headers: Record<string, string> = {}) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", ...headers } });
}
const err = (status: number, code: string, message = code) => json({ error: { code, message, request_id: "req_1", details: {} } }, status);
const customer = { authenticated: true, name: "Penguji UX", role: "owner", kind: "user", org_id: "org_1", org_name: "PT Uji", plan: "trial", must_change_password: false, horizon_max: 3 };

let lastLocation = "";
function Probe() { const l = useLocation(); lastLocation = l.pathname + l.search; return null; }
const mount = (path: string) => render(
  <MemoryRouter initialEntries={[path]}>
    <I18nProvider initial="id"><ToastProvider><AuthProvider><App /><Probe /></AuthProvider></ToastProvider></I18nProvider>
  </MemoryRouter>,
);
function asCustomer(extra: (path: string, url: string, init?: RequestInit) => Response | null = () => null) {
  localStorage.setItem("icp.sessionToken", "icp_sess_test");
  const calls: string[] = [];
  vi.stubGlobal("fetch", async (url: string, init?: RequestInit) => {
    const path = url.split("?")[0] ?? "";
    calls.push(url);
    if (path === "/api/v1/auth/whoami") return json(customer);
    return extra(path, url, init) ?? err(404, "NOT_FOUND");
  });
  return calls;
}
function mockMedia(drawer: boolean) {
  vi.stubGlobal("matchMedia", (q: string) => ({ matches: drawer && q.includes("max-width: 900px"), media: q, addEventListener: () => undefined, removeEventListener: () => undefined }));
}

const history: HistoryPoint[] = [
  { period: "2026-07", icp_price: 88.1, brent_price: 89, usd_idr: 16300, icp_idr: 1436030 },
  { period: "2026-08", icp_price: 89.43, brent_price: 90.8, usd_idr: 16400, icp_idr: 1466652 },
  { period: "2026-09", icp_price: 112.81, brent_price: 114.16, usd_idr: 17749, icp_idr: 2002265 },
];
const pt = (period: string, step: number, extra: Record<string, unknown> = {}) => ({
  period, step, kind: "future_forecast", prediction: 112.9, intervals: { "0.8": [103.32, 121.85] }, interval_basis: "hold_last", conditional_on_scenario: false,
  benchmarks: { brent: { value: 114.16, source: "assumed_hold_last", used_by_model: true }, dubai: { value: 110, source: "assumed_hold_last", used_by_model: false }, wti: { value: 100, source: "assumed_hold_last", used_by_model: false } },
  recursive: false, prediction_idr: null, fx_rate: null, fx_source: null, ...extra,
});
const run = (points: unknown[], origin = "2026-09"): ForecastRun => ({
  id: "fc_1", created_at: "2026-10-16T10:00:00Z", request_id: null, requested_by: "system", status: "succeeded", horizon: points.length, workflow: "system",
  issue_date: "2026-10-16", forecast_origin: origin, method: "spread_brent_6m", model_name: "M", model_version: "5", model_run_id: "r", icp_dataset_version_id: null,
  wti_dataset_version_id: null, dataset_versions: {}, last_observed_period: origin, params: {}, error_code: null, error_message: null, duration_ms: 1, export_available: false,
  result: { forecast_origin: origin, issue_date: "2026-10-16", horizon: points.length, method: "spread_brent_6m", warnings: [], interval_levels: [0.8], interval_note: "", points: points as never },
});

beforeEach(() => { lastLocation = ""; });
afterEach(() => { cleanup(); vi.unstubAllGlobals(); localStorage.clear(); sessionStorage.clear(); document.documentElement.style.overflow = ""; });

describe("navigation drawer (≤ 900 px) is a real modal", () => {
  it("traps focus, makes the page inert, locks scrolling, closes on Escape and restores focus", async () => {
    mockMedia(true);
    asCustomer();
    mount("/akun");
    const menu = await screen.findByRole("button", { name: "Buka navigasi" });
    menu.focus();
    fireEvent.click(menu);
    const dialog = await screen.findByRole("dialog", { name: "Navigasi ruang kerja" });
    expect(dialog).toHaveAttribute("aria-modal", "true");
    expect(document.getElementById("main")).toHaveAttribute("inert");
    expect(document.querySelector(".topbar")).toHaveAttribute("inert");
    expect(document.documentElement.style.overflow).toBe("hidden");
    expect(dialog.contains(document.activeElement)).toBe(true);
    // Tab from the last control wraps to the first; 25 presses never leave the drawer.
    for (let i = 0; i < 25; i++) {
      const items = [...dialog.querySelectorAll<HTMLElement>("a[href], button:not([disabled])")];
      const active = document.activeElement as HTMLElement;
      if (active === items[items.length - 1]) { fireEvent.keyDown(document, { key: "Tab" }); expect(document.activeElement).toBe(items[0]); }
      else items[Math.min(items.indexOf(active) + 1, items.length - 1)]?.focus();
      expect(dialog.contains(document.activeElement)).toBe(true);
    }
    fireEvent.keyDown(document, { key: "Escape" });
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "Navigasi ruang kerja" })).toBeNull());
    expect(document.getElementById("main")).not.toHaveAttribute("inert");
    expect(document.documentElement.style.overflow).toBe("");
    expect(menu).toHaveFocus();
  });

  it("is a plain navigation landmark on desktop (no dialog role, nothing inert)", async () => {
    mockMedia(false);
    asCustomer();
    mount("/akun");
    await screen.findByRole("navigation", { name: "Navigasi ruang kerja" });
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(document.getElementById("main")).not.toHaveAttribute("inert");
  });
});

describe("focus after navigation", () => {
  it("moves focus to the new page heading, not the clicked link; help is one click away", async () => {
    asCustomer((p) => p === "/api/v1/data/history" ? json({ points: history, history_years: 3, earliest_period: "2023-10" }) : null);
    mount("/akun");
    await screen.findByRole("heading", { level: 1, name: "Pengaturan akun" });
    const nav = screen.getByRole("navigation", { name: "Navigasi ruang kerja" });
    fireEvent.click(within(nav).getByRole("link", { name: "Riwayat harga" }));
    const h1 = await screen.findByRole("heading", { level: 1, name: /Riwayat harga/ });
    await waitFor(() => expect(h1).toHaveFocus());
    expect(within(screen.getByRole("navigation", { name: "Akun dan bantuan" })).getByRole("link", { name: "Data & metodologi" })).toHaveAttribute("href", "/metodologi");
  });
});

describe("tabs", () => {
  function Harness() {
    const [v, setV] = useState<"a" | "b" | "c">("a");
    return (
      <I18nProvider initial="id">
        <Tabs idBase="t" label="Contoh" value={v} onChange={setV} tabs={[{ value: "a", label: "Satu" }, { value: "b", label: "Dua" }, { value: "c", label: "Tiga" }]} />
        <div {...tabPanelProps("t", v)}>Panel {v}</div>
      </I18nProvider>
    );
  }
  it("uses roving tabindex, arrow/Home/End keys and tab ↔ panel ids", () => {
    render(<Harness />);
    const [a, b, c] = screen.getAllByRole("tab");
    expect(a).toHaveAttribute("tabindex", "0");
    expect(b).toHaveAttribute("tabindex", "-1");
    expect(a).toHaveAttribute("aria-controls", "t-panel-a");
    expect(screen.getByRole("tabpanel")).toHaveAttribute("aria-labelledby", "t-tab-a");
    a!.focus();
    fireEvent.keyDown(a!, { key: "ArrowRight" });
    expect(b).toHaveAttribute("aria-selected", "true");
    expect(b).toHaveFocus();
    fireEvent.keyDown(b!, { key: "End" });
    expect(c).toHaveFocus();
    fireEvent.keyDown(c!, { key: "ArrowRight" });
    expect(a).toHaveFocus();
    fireEvent.keyDown(a!, { key: "ArrowLeft" });
    expect(c).toHaveAttribute("aria-selected", "true");
    expect(screen.getByRole("tabpanel")).toHaveTextContent("Panel c");
  });
});

describe("honest forecast presentation", () => {
  it("draws assumed Brent as its own series and never extends the observed Brent line", () => {
    const { rows } = buildForecastRows(history, run([pt("2026-10", 1), pt("2026-11", 2)]), "0.8");
    const oct = rows.find((r) => r.period === "2026-10")!;
    expect(oct.brent).toBeUndefined();
    expect(oct.brent_assumed).toBe(114.16);
    // continuity point at the last observed month
    expect(rows.find((r) => r.period === "2026-09")!.brent_assumed).toBe(114.16);
  });

  it("explains a flat hold-last forecast and links to scenarios", () => {
    render(<MemoryRouter><I18nProvider initial="id"><AssumptionNote points={run([pt("2026-10", 1), pt("2026-11", 2)]).result!.points} history={history} /></I18nProvider></MemoryRouter>);
    expect(screen.getByText(/Brent dipertahankan pada USD 114,16 \(nilai teramati terakhir, Sep 2026\) untuk 2 bulan mendatang/)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Uji asumsi Brent Anda/ })).toHaveAttribute("href", "/skenario");
  });

  it("shows the official value for months published since the issue, and labels scenario ranges as conditional", () => {
    const pts = [pt("2026-08", 1, { kind: "estimate_awaiting_publication", prediction: 90.1 }), pt("2026-10", 2, { conditional_on_scenario: true })];
    const { container } = render(<MemoryRouter><I18nProvider initial="id"><ForecastTable points={run(pts, "2026-07").result!.points} level="0.8" history={history} /></I18nProvider></MemoryRouter>);
    expect(screen.getByText("Sudah resmi")).toBeInTheDocument();
    expect(container.textContent).toContain("Resmi USD 89,43");
    expect(container.textContent).not.toContain("Estimasi (menunggu publikasi)Estimasi");
    expect(screen.getAllByText("jika asumsi tepat").length).toBeGreaterThan(0);
    // record-card structure: the central value is the main cell of each card
    const table = container.querySelector("table")!;
    expect(table).toHaveClass("rc");
    expect(table.querySelectorAll("td.rc-main")[1]).toHaveTextContent("USD 112,90");
    expect(table.querySelectorAll("td.rc-main")[1]).toHaveAttribute("data-label", "Nilai tengah (USD/bbl)");
  });

  it("opens the forecast issue named in the URL", async () => {
    const calls = asCustomer((p) => p === "/api/v1/forecasts/fc_old" ? json({ ...run([pt("2026-10", 1)]), id: "fc_old" })
      : p === "/api/v1/data/history" ? json({ points: history }) : p === "/api/v1/forecasts" ? json({ items: [], next_cursor: null }) : p === "/api/v1/models" ? json({ versions: [], production_version: null, alias: "production" }) : null);
    mount("/proyeksi?proyeksi=fc_old");
    // The page title is "Proyeksi ICP" since the wave B redesign (was "Proyeksi Harga Minyak Mentah Indonesia").
    await screen.findByRole("heading", { level: 1, name: "Proyeksi ICP" });
    await waitFor(() => expect(calls.some((c) => c.startsWith("/api/v1/forecasts/fc_old"))).toBe(true));
    expect(await screen.findByText(/Brent dipertahankan/)).toBeInTheDocument();
  });
});

describe("verification never overclaims", () => {
  const L = (id: string, en: string) => ({ id, en });
  const month = (period: string, basis: string, label: ReturnType<typeof L>) => ({ period, value: 1, status: basis === "two_documents" ? "verified" : "single_source", basis, documents: basis === "two_documents" ? 2 : 1, kinds: [], reference: null, label, explanation: L("x", "x") });
  const verification = { dataset: "icp", history_years: 3, earliest_period: "2023-10", counts: { two_documents: 1, single_document: 1 },
    legend: { no_evidence: { label: L("Bukti belum tercatat", "No evidence recorded"), explanation: L("x", "x") } },
    months: [month("2026-09", "two_documents", L("Dua dokumen ESDM sepakat", "Two ESDM documents agree")), month("2026-08", "single_document", L("Satu dokumen ESDM", "One ESDM document"))] };
  it("shows 'Bukti belum tercatat' for months without evidence and keeps view state in the URL", async () => {
    asCustomer((p) => p === "/api/v1/data/history" ? json({ points: history, history_years: 3, earliest_period: "2023-10" })
      : p === "/api/v1/data/verification" ? json(verification)
      : p === "/api/v1/datasets/active" ? json({ datasets: {} }) : null);
    mount("/harga?rentang=1");
    const table = await screen.findByRole("table", { name: /Tabel bulanan/ });
    await waitFor(() => expect(within(table).getAllByText("Bukti belum tercatat")).toHaveLength(1));
    expect(within(table).getByText("Dua dokumen ESDM sepakat")).toBeInTheDocument();
    expect(within(table).getByText("Satu dokumen ESDM")).toBeInTheDocument();
    expect(table.querySelector(".evidence-dot.confirmed")).toHaveTextContent("Dua dokumen ESDM sepakat");
    expect(within(table).queryByText("Teramati")).toBeNull();
    expect(screen.getByRole("button", { name: "1 thn" })).toHaveAttribute("aria-pressed", "true");
    fireEvent.click(screen.getByRole("button", { name: "Dubai" }));
    await waitFor(() => expect(lastLocation).toBe("/harga?rentang=1&acuan=dubai"));
  });
});

describe("charts and states", () => {
  it("uses evenly spaced nice ticks", () => {
    for (const [lo, hi] of [[55, 125], [-6, 4], [970000, 2063000], [88.1, 112.81]] as const) {
      const ticks = niceTicks(lo, hi);
      expect(ticks.length).toBeGreaterThanOrEqual(3);
      expect(ticks.length).toBeLessThanOrEqual(6);
      const steps = new Set(ticks.slice(1).map((v, i) => Number((v - ticks[i]!).toFixed(6))));
      expect(steps.size).toBe(1);
      expect(ticks[0]).toBeLessThanOrEqual(lo);
      expect(ticks.at(-1)).toBeGreaterThanOrEqual(hi);
    }
  });

  it("gives every chart a summary sentence and a keyboard-reachable table view with units", () => {
    render(<I18nProvider initial="id"><TimeSeriesChart ariaLabel="Harga bulanan" data={history as unknown as Array<Record<string, unknown>>} series={[{ key: "icp_price", label: "ICP", color: "#1d6fb8" }]} /></I18nProvider>);
    const img = screen.getByRole("img", { name: "Harga bulanan (USD/bbl)" });
    const desc = document.getElementById(img.getAttribute("aria-describedby")!);
    expect(desc).toHaveTextContent("ICP: USD 88,10 pada Jul 2026, USD 112,81 pada Sep 2026.");
    fireEvent.click(screen.getByRole("button", { name: "Tabel" }));
    const table = screen.getByRole("table", { name: /Harga bulanan/ });
    expect(within(table).getByText("USD 112,81")).toBeInTheDocument();
  });

  // Direction words are lower case since the 2026-10 design system (sentence case everywhere, no capitals).
  it("writes direction as arrow + sign + word with a stabil threshold, in neutral ink", () => {
    const { container } = render(<I18nProvider initial="id"><Delta value={26.1} /> <Delta value={-0.3} /></I18nProvider>);
    expect(container.textContent).toContain("+26,1%naik");
    expect(container.textContent).toContain("−0,3%stabil");
    expect(container.querySelector(".delta.up")).not.toHaveClass("good");
  });

  it("explains a rate limit in the user's language with the wait time and never shows server English in Indonesian", () => {
    render(<MemoryRouter><I18nProvider initial="id"><ErrorState error={new ApiError(429, "RATE_LIMITED", "Too many requests. Slow down and retry later.", null, { retry_after_seconds: 42 })} /></I18nProvider></MemoryRouter>);
    expect(screen.getByText("Terlalu banyak permintaan, coba lagi dalam 42 detik.")).toBeInTheDocument();
    expect(screen.queryByText(/Slow down/)).toBeNull();
    cleanup();
    render(<MemoryRouter><I18nProvider initial="id"><ErrorState error={new ApiError(409, "CONFLICT", "A request is already pending.", null)} /></I18nProvider></MemoryRouter>);
    expect(screen.queryByText(/already pending/)).toBeNull();
  });

  it("reads the Retry-After header of a gateway 429", async () => {
    vi.stubGlobal("fetch", async () => json({ error: { code: "RATE_LIMITED", message: "Too many requests.", request_id: null, details: {} } }, 429, { "retry-after": "60" }));
    const { api } = await import("./api/client");
    const e = await api("/x").catch((x: unknown) => x);
    expect(e).toBeInstanceOf(ApiError);
    expect((e as ApiError).details.retry_after_seconds).toBe(60);
  });
});

describe("onboarding and unsaved changes", () => {
  it("shows 'Langkah pertama' for a new organisation, ticks finished steps, and stays dismissed", async () => {
    const org = { saved_scenarios: 1, alert_rules: 0 };
    asCustomer((p) => p === "/api/v1/org" ? json(org) : p === "/api/v1/reports" ? json({ items: [], next_cursor: null }) : null);
    const view = mount("/ringkasan");
    const card = await screen.findByRole("region", { name: "Langkah pertama" });
    expect(within(card).getAllByRole("link")).toHaveLength(4);
    expect(within(card).getByText("selesai")).toBeInTheDocument();
    fireEvent.click(within(card).getByRole("button", { name: "Tutup panduan" }));
    expect(screen.queryByRole("region", { name: "Langkah pertama" })).toBeNull();
    view.unmount();
    mount("/ringkasan");
    await screen.findByRole("heading", { level: 1 });
    await act(async () => { await new Promise((r) => setTimeout(r, 50)); });
    expect(screen.queryByRole("region", { name: "Langkah pertama" })).toBeNull();
  });

  it("asks before leaving a scenario form with unsaved changes", async () => {
    asCustomer((p) => p === "/api/v1/scenarios" ? json({ items: [], next_cursor: null }) : p === "/api/v1/org" ? json({ entitlements: { scenarios: true, saved_scenarios_max: 2, label_id: "Uji coba" }, saved_scenarios: 0 })
      : p === "/api/v1/data/history" ? json({ points: history }) : p === "/api/v1/forecasts" ? json({ items: [], next_cursor: null }) : null);
    mount("/skenario");
    const newBtn = (await screen.findAllByRole("button", { name: "Skenario baru" }))[0]!;
    await waitFor(() => expect(newBtn).toBeEnabled());
    fireEvent.click(newBtn);
    fireEvent.change(await screen.findByLabelText("Nama skenario"), { target: { value: "Brent 80" } });
    const nav = screen.getByRole("navigation", { name: "Navigasi ruang kerja" });
    fireEvent.click(within(nav).getByRole("link", { name: "Ringkasan" }));
    const confirm = await screen.findByRole("alertdialog", { name: "Tinggalkan halaman tanpa menyimpan?" });
    expect(lastLocation).toBe("/skenario");
    fireEvent.click(within(confirm).getByRole("button", { name: "Batal" }));
    await waitFor(() => expect(screen.queryByRole("alertdialog")).toBeNull());
    fireEvent.click(within(nav).getByRole("link", { name: "Ringkasan" }));
    fireEvent.click(await screen.findByRole("button", { name: "Ya, tinggalkan" }));
    await waitFor(() => expect(lastLocation).toBe("/ringkasan"));
  });
});

describe("buttons inside forms", () => {
  it("never submit the form unless they say type=submit (a download click re-generated the report)", () => {
    const onSubmit = vi.fn((e: { preventDefault: () => void }) => e.preventDefault());
    render(<form onSubmit={onSubmit}><Button>Unduh PDF</Button><Button type="submit">Buat laporan</Button></form>);
    fireEvent.click(screen.getByRole("button", { name: "Unduh PDF" }));
    expect(onSubmit).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "Buat laporan" }));
    expect(onSubmit).toHaveBeenCalledTimes(1);
  });
});
