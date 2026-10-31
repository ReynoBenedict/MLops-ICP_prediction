/**
 * Workspace core (wave B, W1): the top bar (location, data status popover from one shared
 * /data/summary request, keyboard behaviour, operator variant), the overview range in the URL, the
 * forecast tabs in the URL with Back, the forecast stale guard, the #bukti deep link, and the rule
 * that no skeleton survives a failed request. The review fixes add: partial and total failures on
 * the overview, data status recovery, the stale guard right after a re-run / after an origin change /
 * on value-only edits, the price history table frame, and the home page's short session wait.
 */
import { act, cleanup, configure, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter, useLocation, useNavigate, type NavigateFunction } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import type { ForecastRun, HistoryPoint } from "./api/types";
import { AuthProvider } from "./auth";
import { ToastProvider } from "./components/toast";
import { AssumptionTable, ForecastTable } from "./components/forecast";
import { SlowNotice } from "./components/ui";
import { I18nProvider } from "./i18n";

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}
const err = (status: number, code: string) => json({ error: { code, message: code, request_id: "req_1", details: {} } }, status);
const customer = { authenticated: true, name: "Penguji UX", role: "owner", kind: "user", org_id: "org_1", org_name: "PT Uji", plan: "trial", must_change_password: false, horizon_max: 3 };
const operator = { authenticated: true, name: "Operator", role: "operator", kind: "operator", org_id: null, org_name: null, plan: null, must_change_password: false, horizon_max: 6 };

let lastLocation = "";
let navigate: NavigateFunction | null = null;
function Probe() {
  const l = useLocation();
  navigate = useNavigate();
  lastLocation = l.pathname + l.search + l.hash;
  return null;
}
const mount = (path: string) => render(
  <MemoryRouter initialEntries={[path]}>
    <I18nProvider initial="id"><ToastProvider><AuthProvider><App /><Probe /></AuthProvider></ToastProvider></I18nProvider>
  </MemoryRouter>,
);

type Handler = (path: string, init?: RequestInit) => Response | null;
function server(who: unknown, extra: Handler = () => null) {
  const calls: Array<{ path: string; method: string; body: unknown }> = [];
  vi.stubGlobal("fetch", async (url: string, init?: RequestInit) => {
    const path = url.split("?")[0] ?? "";
    calls.push({ path, method: init?.method ?? "GET", body: init?.body ? JSON.parse(String(init.body)) : null });
    if (path === "/api/v1/auth/whoami") return json(who);
    return extra(path, init) ?? err(404, "NOT_FOUND");
  });
  return calls;
}
const asCustomer = (extra?: Handler) => { localStorage.setItem("icp.sessionToken", "icp_sess_test"); return server(customer, extra); };

/* ---------- fixtures ------------------------------------------------------------------------- */
const fresh = (p: string) => ({ status: "fresh", latest_period: p, expected_period: p, months_behind: 0 });
const ds = (period_end: string, latest_value: number, extra: Record<string, unknown> = {}) => ({
  available: true, label_id: "Label ID", unit: "USD/bbl", latest_value, period_end, freshness: fresh(period_end), updated_at: "2026-10-16T22:07:10Z", missing_periods: [], ...extra,
});
const summary = {
  datasets: {
    icp: ds("2026-09", 112.81, { previous_value: 89.43, mom_change_pct: 26.14 }), brent: ds("2026-09", 114.16), dubai: ds("2026-09", 93.3),
    wti: ds("2026-08", 97.31, { freshness: { status: "lagging", latest_period: "2026-08", expected_period: "2026-09", months_behind: 1 } }), usd_idr: ds("2026-09", 17749, { unit: "IDR/USD" }),
  },
  relationships: { brent: { correlation: 0.99, spread_mean: -2.36, spread_latest: -1.35, spread_latest_period: "2026-09", months: 93 } },
  icp_idr: { period: "2026-09", icp_usd: 112.81, usd_idr: 17749, icp_idr: 2002265, fx_definition: "x", note: "x" },
};
const months = Array.from({ length: 36 }, (_, i) => { const t = 2023 * 12 + 9 + i; return `${Math.floor(t / 12)}-${String((t % 12) + 1).padStart(2, "0")}`; });
const points: HistoryPoint[] = months.map((period, i) => ({ period, icp_price: i === 35 ? 112.81 : i === 34 ? 89.43 : 70 + (i % 7), brent_price: 72 + (i % 7), usd_idr: 16000 }));
const history = { points, history_years: 3, earliest_period: "2023-10" };
const pt = (period: string, step: number) => ({
  period, step, kind: "future_forecast", prediction: 112.9, intervals: { "0.8": [103.32, 121.85] }, interval_basis: "hold_last", conditional_on_scenario: false,
  benchmarks: { brent: { value: 114.16, source: "assumed_hold_last", used_by_model: true }, dubai: { value: 93.3, source: "assumed_hold_last", used_by_model: false }, wti: { value: 97.31, source: "assumed_hold_last", used_by_model: false } },
  recursive: false, prediction_idr: null, fx_rate: null, fx_source: null,
});
const run = (id: string, n: number): ForecastRun => ({
  id, created_at: "2026-10-17T00:00:00Z", request_id: null, requested_by: "system", status: "succeeded", horizon: n, workflow: "system",
  issue_date: "2026-10-17", forecast_origin: "2026-09", method: "spread_brent_6m", model_name: "M", model_version: "5", model_run_id: "r",
  icp_dataset_version_id: null, wti_dataset_version_id: null, dataset_versions: {}, last_observed_period: "2026-09", params: {}, error_code: null,
  error_message: null, duration_ms: 1, export_available: false, scenario: null,
  result: { forecast_origin: "2026-09", issue_date: "2026-10-17", horizon: n, method: "spread_brent_6m", warnings: [], interval_levels: [0.8], interval_note: "",
    points: ["2026-10", "2026-11", "2026-12"].slice(0, n).map((p, i) => pt(p, i + 1)) as never },
});
const L = (id: string, en: string) => ({ id, en });
const verification = {
  dataset: "icp", history_years: 3, earliest_period: "2023-10", counts: { two_documents: 36 }, legend: {},
  months: months.map((period) => ({ period, value: 1, status: "verified", basis: "two_documents", documents: 2, kinds: [], reference: period === "2026-09" ? "395.K/MG.03/MEM.M/2026" : null, label: L("Dua dokumen ESDM sepakat", "Two ESDM documents agree"), explanation: L("x", "x") })),
};
const models = { versions: [], production_version: null, alias: "production" };
/** A healthy workspace backend. */
const healthy: Handler = (p) => p === "/api/v1/data/summary" ? json(summary)
  : p === "/api/v1/data/history" ? json(history)
  : p === "/api/v1/data/verification" ? json(verification)
  : p === "/api/v1/forecasts/latest" ? json(run("fc_latest", 3))
  : p === "/api/v1/forecasts/fc_latest" ? json(run("fc_latest", 3))
  : p === "/api/v1/forecasts" ? json({ items: [run("fc_latest", 3)], next_cursor: null })
  : p === "/api/v1/models" ? json(models)
  : p === "/api/v1/datasets/active" ? json({ datasets: {} })
  : null;

// Pages are lazy routes: the first render of a page module in a busy parallel run can take a few
// seconds to transform, so waits allow for that (this file only; vitest isolates files).
configure({ asyncUtilTimeout: 8_000 });

beforeEach(() => { lastLocation = ""; navigate = null; });
afterEach(() => { cleanup(); vi.unstubAllGlobals(); localStorage.clear(); sessionStorage.clear(); });

describe("app top bar", () => {
  it("shows the location and the data status from one shared /data/summary request", async () => {
    const calls = asCustomer(healthy);
    mount("/ringkasan");
    const loc = await screen.findByRole("navigation", { name: "Lokasi" });
    expect(loc).toHaveTextContent("Pasar");
    expect(within(loc).getByText("Ringkasan")).toHaveAttribute("aria-current", "page");
    const status = await screen.findByRole("button", { name: /ICP resmi s\.d\. September 2026/ });
    expect(status).toHaveTextContent("Mutakhir");
    // the overview page reads the same summary: one request for both
    await screen.findByRole("heading", { level: 1, name: "Ringkasan pasar" });
    await screen.findByText("112,81");
    expect(calls.filter((c) => c.path === "/api/v1/data/summary")).toHaveLength(1);
  });

  it("opens a keyboard-accessible popover listing every source, and Escape returns focus to the button", async () => {
    asCustomer(healthy);
    mount("/proyeksi");
    expect(await screen.findByRole("navigation", { name: "Lokasi" })).toHaveTextContent("Analisis");
    const button = await screen.findByRole("button", { name: /ICP resmi s\.d\. September 2026/ });
    expect(button).toHaveAttribute("aria-expanded", "false");
    button.focus();
    fireEvent.click(button);
    expect(button).toHaveAttribute("aria-expanded", "true");
    const dialog = screen.getByRole("dialog", { name: "Status data" });
    expect(button).toHaveAttribute("aria-controls", dialog.id);
    await waitFor(() => expect(dialog).toHaveFocus());
    for (const name of ["ICP resmi", "Brent", "Dubai", "WTI", "Kurs JISDOR"]) expect(within(dialog).getByRole("rowheader", { name: new RegExp(name) })).toBeInTheDocument();
    // a lagging source says so, in words, once ("Tertinggal 1 bulan", not "Tertinggal · tertinggal 1 bulan")
    expect(within(dialog).getByText("Tertinggal 1 bulan")).toBeInTheDocument();
    expect(within(dialog).queryByText(/Tertinggal · tertinggal/)).toBeNull();
    expect(within(dialog).getByRole("link", { name: /Lihat bukti per bulan/ })).toHaveAttribute("href", "/harga#bukti");
    fireEvent.keyDown(dialog, { key: "Escape" });
    expect(screen.queryByRole("dialog", { name: "Status data" })).toBeNull();
    expect(button).toHaveFocus();
    expect(button).toHaveAttribute("aria-expanded", "false");
  });

  it("explains an unavailable data status instead of loading forever", async () => {
    asCustomer((p) => p === "/api/v1/data/summary" ? err(503, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE") : healthy(p));
    mount("/proyeksi");
    const button = await screen.findByRole("button", { name: /Status data tidak tersedia/ });
    fireEvent.click(button);
    const dialog = screen.getByRole("dialog", { name: "Status data" });
    expect(within(dialog).getByRole("alert")).toHaveTextContent("Layanan data sedang tidak tersedia");
    expect(dialog.querySelector(".skeleton")).toBeNull();
  });

  it("shows the environment to operators instead of the data status", async () => {
    sessionStorage.setItem("icp.operatorToken", "op_test");
    const calls = server(operator, (p) => p === "/api/v1/system/status" ? json({ status: "ready", environment: "local", version: "2.0.0", checks: [] }) : null);
    mount("/operasi");
    expect(await screen.findByRole("navigation", { name: "Lokasi" })).toHaveTextContent("Kesehatan sistem");
    expect(await screen.findByText("Lingkungan local · API 2.0.0")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Status data|ICP resmi s\.d\./ })).toBeNull();
    expect(calls.some((c) => c.path === "/api/v1/data/summary")).toBe(false);
  });
});

describe("overview", () => {
  it("keeps the chart range in the URL, capped by the plan's history", async () => {
    asCustomer(healthy);
    mount("/ringkasan");
    const three = await screen.findByRole("button", { name: "3 thn" });
    expect(three).toHaveAttribute("aria-pressed", "true");
    expect(screen.queryByRole("button", { name: "5 thn" })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "1 thn" }));
    await waitFor(() => expect(lastLocation).toBe("/ringkasan?rentang=1"));
    expect(screen.getByRole("heading", { name: "ICP resmi vs Brent, 12 bulan terakhir" })).toBeInTheDocument();
    cleanup();
    asCustomer(healthy);
    mount("/ringkasan?rentang=5");
    expect(await screen.findByRole("button", { name: "3 thn" })).toHaveAttribute("aria-pressed", "true");
  });

  it("states the lead number with its period, evidence, changes, rupiah basis and source", async () => {
    asCustomer(healthy);
    mount("/ringkasan");
    const lead = await screen.findByRole("region", { name: "ICP resmi September 2026" });
    await waitFor(() => expect(within(lead).getByText("Dua dokumen ESDM sepakat")).toBeInTheDocument());
    expect(lead).toHaveTextContent("112,81");
    expect(lead).toHaveTextContent("Dibanding Agustus 2026");
    expect(lead).toHaveTextContent("dari USD 89,43");
    expect(lead).toHaveTextContent("Rp 2.002.265");
    expect(lead).toHaveTextContent("ICP × kurs JISDOR rata-rata September 2026 (Rp 17.749 per USD)");
    expect(lead).toHaveTextContent("Keputusan Menteri ESDM 395.K/MG.03/MEM.M/2026");
  });

  it("shows dataset names in English without Indonesian leftovers", async () => {
    localStorage.setItem("icp.locale", "en");
    asCustomer(healthy);
    render(<MemoryRouter initialEntries={["/ringkasan"]}><I18nProvider initial="en"><ToastProvider><AuthProvider><App /></AuthProvider></ToastProvider></I18nProvider></MemoryRouter>);
    await screen.findByRole("heading", { level: 1, name: "Market overview" });
    expect((await screen.findAllByText("JISDOR rate")).length).toBeGreaterThan(0);
    expect(document.body.textContent).not.toContain("Label ID");
    expect(document.body.textContent).not.toMatch(/rata-rata bulanan|Harga Minyak Mentah/);
  });

  it("leaves no skeleton behind when every request fails", async () => {
    asCustomer((p) => p.startsWith("/api/v1/auth/") ? null : err(500, "INTERNAL_ERROR"));
    mount("/ringkasan");
    await screen.findByRole("heading", { level: 1, name: "Ringkasan pasar" });
    await waitFor(() => expect(document.querySelectorAll(".skeleton")).toHaveLength(0));
    expect(screen.getAllByRole("alert").length).toBeGreaterThan(0);
    expect(screen.getAllByRole("button", { name: "Coba lagi" }).length).toBeGreaterThan(0);
  });
});

describe("forecast page", () => {
  it("keeps the tab in the URL and Back returns to the previous tab", async () => {
    asCustomer(healthy);
    mount("/proyeksi");
    const hasil = await screen.findByRole("tab", { name: "Hasil" });
    expect(hasil).toHaveAttribute("aria-selected", "true");
    fireEvent.click(screen.getByRole("tab", { name: "Asumsi" }));
    await waitFor(() => expect(lastLocation).toBe("/proyeksi?tab=asumsi"));
    expect(await screen.findByText("Asumsi input per bulan")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("tab", { name: "Riwayat terbitan" }));
    await waitFor(() => expect(lastLocation).toBe("/proyeksi?tab=riwayat"));
    expect(screen.getByRole("tabpanel")).toHaveAttribute("aria-labelledby", "fc-tab-riwayat");
    act(() => { navigate!(-1); });
    await waitFor(() => expect(screen.getByRole("tab", { name: "Asumsi" })).toHaveAttribute("aria-selected", "true"));
    expect(lastLocation).toBe("/proyeksi?tab=asumsi");
  });

  it("keeps the interval level in the URL", async () => {
    asCustomer(healthy);
    mount("/proyeksi?tingkat=95");
    expect(await screen.findByRole("button", { name: "95%" })).toHaveAttribute("aria-pressed", "true");
    fireEvent.click(screen.getByRole("button", { name: "50%" }));
    await waitFor(() => expect(lastLocation).toBe("/proyeksi?tingkat=50"));
  });

  it("marks results as made with the previous inputs after an edit, and clears the notice after re-running", async () => {
    const calls = asCustomer((p, init) => {
      if (p === "/api/v1/forecasts" && init?.method === "POST") return json(run("fc_new", 2));
      if (p === "/api/v1/forecasts/fc_new") return json(run("fc_new", 2));
      return healthy(p);
    });
    mount("/proyeksi");
    await screen.findByText("Hasil proyeksi");
    await screen.findAllByText("USD 112,90");
    expect(screen.queryByText("Hasil di bawah dibuat dengan asumsi sebelumnya.")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "2 bulan" }));
    const notice = (await screen.findByText("Hasil di bawah dibuat dengan asumsi sebelumnya.")).closest(".callout") as HTMLElement;
    expect(notice).toHaveTextContent("Hasil: horizon 3 bulan, tanpa skenario. Formulir: horizon 2 bulan, tanpa skenario.");
    fireEvent.click(within(notice).getByRole("button", { name: "Jalankan ulang" }));
    await waitFor(() => expect(calls.some((c) => c.method === "POST" && c.path === "/api/v1/forecasts")).toBe(true));
    expect(calls.find((c) => c.method === "POST")!.body).toEqual({ horizon: 2 });
    await waitFor(() => expect(lastLocation).toContain("proyeksi=fc_new"));
    await waitFor(() => expect(screen.queryByText("Hasil di bawah dibuat dengan asumsi sebelumnya.")).toBeNull());
    // going back to the old horizon makes the form differ from the new result again
    fireEvent.click(screen.getByRole("button", { name: "3 bulan" }));
    expect(await screen.findByText("Hasil di bawah dibuat dengan asumsi sebelumnya.")).toBeInTheDocument();
  });

  it("leaves no skeleton behind when the forecast requests fail, on every tab", async () => {
    asCustomer((p) => p.startsWith("/api/v1/auth/") ? null : p === "/api/v1/data/summary" ? json(summary) : err(500, "INTERNAL_ERROR"));
    mount("/proyeksi");
    await screen.findByRole("heading", { level: 1, name: "Proyeksi ICP" });
    for (const tab of ["Hasil", "Asumsi", "Akurasi & metode", "Riwayat terbitan"]) {
      fireEvent.click(screen.getByRole("tab", { name: tab }));
      await waitFor(() => expect(within(screen.getByRole("tabpanel")).getAllByRole("alert").length).toBeGreaterThan(0));
      expect(document.querySelectorAll(".skeleton")).toHaveLength(0);
    }
  });
});

describe("price history evidence deep link", () => {
  it("moves focus to the verification panel (#bukti) once the evidence has loaded", async () => {
    asCustomer(healthy);
    mount("/harga#bukti");
    const heading = await screen.findByRole("heading", { level: 2, name: /Verifikasi ICP per bulan/ });
    expect(heading.closest("section")).toHaveAttribute("id", "bukti");
    await waitFor(() => expect(heading).toHaveFocus(), { timeout: 2000 });
  });
});

describe("loading and narrow layouts", () => {
  it("says so when the page data takes unusually long, and stays quiet before that", () => {
    vi.useFakeTimers();
    try {
      const { rerender } = render(<I18nProvider initial="id"><SlowNotice active /></I18nProvider>);
      expect(screen.queryByText("Data masih dimuat")).toBeNull();
      act(() => { vi.advanceTimersByTime(8_100); });
      expect(screen.getByText("Data masih dimuat")).toBeInTheDocument();
      expect(screen.getByRole("status")).toHaveTextContent("menunggu hingga 35 detik");
      rerender(<I18nProvider initial="id"><SlowNotice active={false} /></I18nProvider>);
      expect(screen.queryByText("Data masih dimuat")).toBeNull();
    } finally { vi.useRealTimers(); }
  });

  it("folds the value-kind column under the month when the table frame is narrow", () => {
    const pts = run("fc_x", 2).result!.points;
    const wide = render(<I18nProvider initial="id"><ForecastTable points={pts} level="0.8" /><AssumptionTable points={pts} /></I18nProvider>);
    expect(wide.getAllByRole("columnheader", { name: "Jenis nilai" })).toHaveLength(2);
    wide.unmount();
    // A 600 px frame (forecast page at 1024 px, scenario results pane): no kind column, chips under the month.
    const spy = vi.spyOn(HTMLElement.prototype, "clientWidth", "get").mockReturnValue(600);
    try {
      render(<I18nProvider initial="id"><ForecastTable points={pts} level="0.8" /><AssumptionTable points={pts} /></I18nProvider>);
      expect(screen.queryByRole("columnheader", { name: "Jenis nilai" })).toBeNull();
      const firstRow = screen.getAllByRole("row")[1]!;
      expect(within(firstRow).getAllByRole("cell")[0]).toHaveTextContent(/Oktober 2026\s*Proyeksi/);
      expect(within(firstRow).getAllByRole("cell")).toHaveLength(4);
    } finally { spy.mockRestore(); }
  });
});

/* ---------- Review fixes (wave B, W1-R1..R12 and the home boot wait) ------------------------------ */
/** Healthy backend plus an empty insight feed (the plan catalogue lists no plan, so insights are asked for). */
const full: Handler = (p, init) => p === "/api/v1/plans" ? json([]) : p === "/api/v1/insights" ? json({ insights: [] }) : healthy(p, init);
/** Alerts with content (the toast region is an empty alert region when no toast is shown). */
const pageAlerts = () => screen.queryAllByRole("alert").filter((a) => !a.classList.contains("toast-region"));
const scenarioIssue = (id: string): ForecastRun => ({ ...run(id, 3), workflow: "api", scenario: { brent: { "2026-10": 125, "2026-11": 120, "2026-12": 120 } } } as ForecastRun);

describe("overview failure states", () => {
  it("explains a history failure in its own block and leaves no skeleton in the status strip", async () => {
    asCustomer((p, i) => p === "/api/v1/data/history" ? err(500, "INTERNAL_ERROR") : full(p, i));
    mount("/ringkasan");
    await screen.findByText("112,81");
    const strip = (await screen.findByRole("heading", { level: 2, name: "Status data" })).closest("section") as HTMLElement;
    await waitFor(() => expect(within(strip).getByText("Tidak tersedia")).toBeInTheDocument());
    await waitFor(() => expect(document.querySelectorAll(".skeleton")).toHaveLength(0));
    // one failing block: its own explanation and retry, no page-level outage message
    expect(pageAlerts()).toHaveLength(1);
    expect(screen.getAllByRole("button", { name: "Coba lagi" })).toHaveLength(1);
    // without the history the held Brent value is not attributed to a month (never to a forecast month)
    expect(document.body.textContent).toContain("Brent dipertahankan pada USD 114,16 (nilai teramati terakhir) untuk 3 bulan mendatang");
    expect(document.body.textContent).not.toMatch(/nilai teramati terakhir, Okt 2026/);
  });

  it("shows one page-level message with one retry when the data service is down, and recovers", async () => {
    let up = false;
    asCustomer((p, i) => p.startsWith("/api/v1/auth/") ? null : up ? full(p, i) : err(503, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE"));
    mount("/ringkasan");
    await screen.findByRole("heading", { level: 1, name: "Ringkasan pasar" });
    await waitFor(() => expect(screen.getAllByText("Data belum dapat dimuat. Lihat pesan di atas lalu coba lagi.").length).toBeGreaterThanOrEqual(4));
    await waitFor(() => expect(document.querySelectorAll(".skeleton")).toHaveLength(0));
    expect(pageAlerts()).toHaveLength(1);
    expect(pageAlerts()[0]).toHaveTextContent("Layanan data sedang tidak tersedia");
    expect(pageAlerts()[0]).not.toHaveTextContent("Angka yang tampil tidak diperbarui");
    expect(screen.getAllByRole("button", { name: "Coba lagi" })).toHaveLength(1);
    up = true;
    fireEvent.click(screen.getByRole("button", { name: "Coba lagi" }));
    expect(await screen.findByText("112,81")).toBeInTheDocument();
    await waitFor(() => expect(pageAlerts()).toHaveLength(0));
    // the top bar's data status recovers with the page (no 15 s wait for its own retry)
    expect(await screen.findByRole("button", { name: /ICP resmi s\.d\. September 2026/ })).toBeInTheDocument();
  });

  it("gives every benchmark spread its unit", async () => {
    asCustomer(full);
    mount("/ringkasan");
    expect(await screen.findByText(/ICP − Brent \(USD\/bbl\): −1,35 · rata-rata 93 bulan −2,36/)).toBeInTheDocument();
  });
});

describe("data status recovery", () => {
  it("reads the status again on navigation after a failure", async () => {
    let up = false;
    asCustomer((p, i) => p === "/api/v1/data/summary" ? (up ? json(summary) : err(503, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE")) : full(p, i));
    mount("/proyeksi");
    await screen.findByRole("button", { name: /Status data tidak tersedia/ });
    up = true;
    act(() => { navigate!("/harga"); });
    expect(await screen.findByRole("button", { name: /ICP resmi s\.d\. September 2026/ })).toHaveTextContent("Mutakhir");
  });

  it("retries a failed status read by itself after a delay", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      let up = false;
      asCustomer((p, i) => p === "/api/v1/data/summary" ? (up ? json(summary) : err(503, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE")) : full(p, i));
      mount("/proyeksi");
      await screen.findByRole("button", { name: /Status data tidak tersedia/ });
      up = true;
      await act(async () => { vi.advanceTimersByTime(15_100); });
      expect(await screen.findByRole("button", { name: /ICP resmi s\.d\. September 2026/ })).toBeInTheDocument();
    } finally { vi.useRealTimers(); }
  });
});

describe("forecast stale guard edge cases", () => {
  it("shows a new run from the server's answer while its own read loads: no stale notice, no skeleton", async () => {
    localStorage.setItem("icp.sessionToken", "icp_sess_test");
    let release!: () => void;
    const held = new Promise<void>((r) => { release = r; });
    const posts: unknown[] = [];
    vi.stubGlobal("fetch", async (url: string, init?: RequestInit) => {
      const path = url.split("?")[0] ?? "";
      if (path === "/api/v1/auth/whoami") return json(customer);
      if (path === "/api/v1/forecasts" && init?.method === "POST") { posts.push(JSON.parse(String(init.body))); return json(run("fc_new", 2)); }
      if (path === "/api/v1/forecasts/fc_new") { await held; return json(run("fc_new", 2)); }
      return full(path, init) ?? err(404, "NOT_FOUND");
    });
    mount("/proyeksi");
    await screen.findAllByText("USD 112,90");
    fireEvent.click(screen.getByRole("button", { name: "2 bulan" }));
    const notice = (await screen.findByText("Hasil di bawah dibuat dengan asumsi sebelumnya.")).closest(".callout") as HTMLElement;
    fireEvent.click(within(notice).getByRole("button", { name: "Jalankan ulang" }));
    await waitFor(() => expect(lastLocation).toContain("proyeksi=fc_new"));
    // The read of the new run is still pending: the screen shows the server's answer, not the previous issue.
    await screen.findByText(/· horizon 2 bulan/);
    expect(screen.queryByText("Hasil di bawah dibuat dengan asumsi sebelumnya.")).toBeNull();
    expect(screen.queryByRole("button", { name: "Jalankan ulang" })).toBeNull();
    expect(document.querySelectorAll(".skeleton")).toHaveLength(0);
    expect(posts).toEqual([{ horizon: 2 }]);
    await act(async () => { release(); await held; });
    await waitFor(() => expect(screen.queryByText("Hasil di bawah dibuat dengan asumsi sebelumnya.")).toBeNull());
  });

  it("names the changed assumption values when only scenario values differ", async () => {
    asCustomer((p, i) => p === "/api/v1/forecasts/fc_scen" ? json(scenarioIssue("fc_scen")) : full(p, i));
    mount("/proyeksi?proyeksi=fc_scen");
    const oct = await screen.findByLabelText("Brent (USD/bbl) Okt 2026");
    await waitFor(() => expect(oct).toHaveValue("125"));
    expect(screen.queryByText("Hasil di bawah dibuat dengan asumsi sebelumnya.")).toBeNull();
    fireEvent.change(oct, { target: { value: "101" } });
    const notice = (await screen.findByText("Hasil di bawah dibuat dengan asumsi sebelumnya.")).closest(".callout") as HTMLElement;
    expect(notice).toHaveTextContent("Asumsi yang berubah: Brent Okt 2026 (USD/bbl): 125 → 101.");
  });

  it("does not compare an issue from an earlier origin with the form, and names the scenario months it cannot take over", async () => {
    const history10 = { ...history, points: [...points, { period: "2026-10", icp_price: 100.5, brent_price: 101, usd_idr: 16000 }] };
    const latest10 = { ...run("fc_oct", 3), forecast_origin: "2026-10", last_observed_period: "2026-10" } as ForecastRun;
    asCustomer((p, i) => p === "/api/v1/data/history" ? json(history10)
      : p === "/api/v1/forecasts/latest" ? json(latest10)
      : p === "/api/v1/forecasts/fc_scen" ? json(scenarioIssue("fc_scen"))
      : full(p, i));
    mount("/proyeksi?proyeksi=fc_scen");
    const info = (await screen.findByText("Proyeksi ini dibuat dari titik awal sebelumnya.")).closest(".callout") as HTMLElement;
    expect(info).toHaveTextContent("dimulai dari ICP resmi September 2026; proyeksi baru dimulai dari ICP resmi Oktober 2026");
    expect(info).toHaveTextContent("Asumsi skenario untuk Oktober 2026 tidak dibawa ke formulir");
    expect(screen.queryByText("Hasil di bawah dibuat dengan asumsi sebelumnya.")).toBeNull();
    // the months that a new run covers keep the issue's assumptions
    expect(screen.getByLabelText("Brent (USD/bbl) Nov 2026")).toHaveValue("120");
    expect(screen.getByLabelText("Brent (USD/bbl) Des 2026")).toHaveValue("120");
  });
});

describe("price history table frame", () => {
  it("folds the evidence column under the month in a narrow frame, and scrolls inside the frame when the table still does not fit", async () => {
    const width = vi.spyOn(HTMLElement.prototype, "clientWidth", "get").mockReturnValue(900);
    const rect = vi.spyOn(HTMLTableElement.prototype, "getBoundingClientRect").mockReturnValue({ width: 950, height: 400, top: 0, left: 0, right: 950, bottom: 400, x: 0, y: 0, toJSON: () => ({}) } as DOMRect);
    try {
      asCustomer(full);
      mount("/harga");
      const table = await screen.findByRole("table", { name: /Tabel bulanan/ });
      await waitFor(() => expect(table).toHaveClass("compact"));
      expect(within(table).queryByRole("columnheader", { name: "Bukti sumber" })).toBeNull();
      expect(within(table).getByRole("columnheader", { name: /Bulan · Bukti sumber/ })).toBeInTheDocument();
      await waitFor(() => expect(within(within(table).getAllByRole("row")[1]!).getAllByRole("cell")[0]).toHaveTextContent(/Sep 2026\s*Dua dokumen ESDM sepakat/));
      expect(within(within(table).getAllByRole("row")[1]!).getAllByRole("cell")).toHaveLength(8);
      // 950 px of table in a 900 px frame: the header does not stick to the page; the frame scrolls instead
      expect(table.parentElement).not.toHaveClass("fits");
    } finally { width.mockRestore(); rect.mockRestore(); }
  });
});

describe("home with a stored credential", () => {
  it("shows the public landing after a short wait while the session check hangs, and keeps it once the session is confirmed", async () => {
    localStorage.setItem("icp.sessionToken", "icp_sess_test");
    let answer!: (r: Response) => void;
    const who = new Promise<Response>((r) => { answer = r; });
    vi.stubGlobal("fetch", async (url: string) => {
      const path = url.split("?")[0] ?? "";
      if (path === "/api/v1/auth/whoami") return who;
      return err(404, "NOT_FOUND");
    });
    vi.useFakeTimers();
    try {
      mount("/");
      expect(document.querySelector(".boot")).not.toBeNull();
      act(() => { vi.advanceTimersByTime(1_000); });
      expect(screen.queryByRole("heading", { level: 1 })).toBeNull();
      act(() => { vi.advanceTimersByTime(1_100); });
      expect(screen.getByRole("heading", { level: 1 })).toHaveTextContent("ICP resmi, lengkap dengan dokumen sumbernya.");
      expect(document.querySelector(".boot")).toBeNull();
    } finally { vi.useRealTimers(); }
    // The check succeeds later: the reader stays on the landing and the header offers the workspace.
    await act(async () => { answer(json(customer)); });
    // (found by text: a role query over the whole landing page is slow in jsdom; more than one place
    // on the page offers the link)
    const links = await screen.findAllByText("Buka ruang kerja");
    expect(links.every((a) => a.closest("a")?.getAttribute("href") === "/ringkasan")).toBe(true);
    expect(lastLocation).toBe("/");
    expect(screen.getByRole("heading", { level: 1 })).toHaveTextContent("ICP resmi, lengkap dengan dokumen sumbernya.");
  });
});
