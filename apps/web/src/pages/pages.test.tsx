import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "../App";
import { AuthProvider } from "../auth";
import { ForecastRunView } from "../components/forecast";
import { I18nProvider } from "../i18n";
import type { ForecastRun } from "../api/types";

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}
const notFound = () => json({ error: { code: "NOT_FOUND", message: "nope", request_id: null, details: {} } }, 404);

const snapshot = {
  icp_period: "2026-03", icp_usd: 102.26, previous_period: "2026-02", previous_usd: 68.79, mom_change_pct: 48.66, brent_usd: 114.16, brent_period: "2026-09",
  usd_idr: 17749, icp_idr: 1730696, freshness: { status: "stale", latest_period: "2026-03", expected_period: "2026-09", months_behind: 6 },
  source: "Kementerian ESDM; EIA; World Bank; Bank Indonesia", updated_at: "2026-10-16T10:00:00Z", latest_forecast: null,
};
const plan = { code: "trial", label_id: "Uji coba", label_en: "Trial", price_note_id: "Gratis, fitur terbatas", seats: 2, forecast_horizon_max: 3, scenarios: true,
  saved_scenarios_max: 2, reports_pdf: true, reports_excel: false, alerts: true, alert_rules_max: 2, api_access: false, api_requests_per_month: 0, history_years: 3, insights: true };

const run: ForecastRun = {
  id: "fc_test", created_at: "2026-10-16T10:00:00Z", request_id: null, requested_by: "system", status: "succeeded", horizon: 2, workflow: "system",
  issue_date: "2026-10-16", forecast_origin: "2026-03", method: "spread_brent_12m", model_name: "M", model_version: "4", model_run_id: "r",
  icp_dataset_version_id: "dsv_1", wti_dataset_version_id: "dsv_2", dataset_versions: { icp: "dsv_1" }, last_observed_period: "2026-03", params: {}, error_code: null,
  error_message: null, duration_ms: 10, export_available: false,
  result: {
    forecast_origin: "2026-03", issue_date: "2026-10-16", horizon: 2, method: "spread_brent_12m", warnings: [], interval_levels: [0.8], interval_note: "",
    points: [
      { period: "2026-04", step: 1, kind: "estimate_awaiting_publication", prediction: 95.5, intervals: { "0.8": [93, 98] }, interval_basis: "observed", conditional_on_scenario: false,
        benchmarks: { brent: { value: 94, source: "observed", used_by_model: true }, dubai: { value: 92, source: "observed", used_by_model: false }, wti: { value: 90, source: "observed", used_by_model: false } },
        recursive: false, prediction_idr: 1700000, fx_rate: 17800, fx_source: "observed" },
      { period: "2026-12", step: 2, kind: "future_forecast", prediction: 97.1, intervals: {}, interval_basis: null, conditional_on_scenario: true,
        benchmarks: { brent: { value: 99, source: "scenario", used_by_model: true }, dubai: { value: null, source: "assumed_hold_last", used_by_model: false }, wti: { value: 90, source: "assumed_hold_last", used_by_model: false } },
        recursive: false, prediction_idr: null, fx_rate: null, fx_source: null },
    ],
  },
};

const wrap = (ui: React.ReactNode, path = "/") => render(<MemoryRouter initialEntries={[path]}><I18nProvider initial="id"><AuthProvider>{ui}</AuthProvider></I18nProvider></MemoryRouter>);

afterEach(() => { cleanup(); vi.unstubAllGlobals(); localStorage.clear(); sessionStorage.clear(); });

const customer = { authenticated: true, name: "Penguji UX", role: "owner", kind: "user", org_id: "org_1", org_name: "PT Uji", plan: "trial", must_change_password: false, horizon_max: 3 };
/** Fake API for a signed-in customer: whoami answers, everything else 404 unless overridden. */
function signedIn(extra: (path: string) => Response | null = () => null) {
  localStorage.setItem("icp.sessionToken", "icp_sess_test");
  vi.stubGlobal("fetch", async (url: string) => {
    const path = url.split("?")[0] ?? "";
    if (path === "/api/v1/auth/whoami") return json(customer);
    return extra(path) ?? notFound();
  });
}

describe("routes", () => {
  it("shows the landing page with the public snapshot and a sign-in teaser instead of forecast values", async () => {
    vi.stubGlobal("fetch", async (url: string) => {
      if (url.startsWith("/api/v1/public/market-snapshot")) return json({ ...snapshot, latest_forecast: { available: true, issue_date: "2026-10-17", forecast_origin: "2026-09", horizon: 6 } });
      if (url.startsWith("/api/v1/plans")) return json([plan]);
      return notFound();
    });
    wrap(<App />);
    expect(await screen.findByRole("heading", { level: 1 })).toHaveTextContent(/Harga Minyak Mentah Indonesia/);
    expect(await screen.findByText("USD 102,26")).toBeInTheDocument();
    expect(screen.getByText("Rp 1.730.696")).toBeInTheDocument();
    expect(await screen.findByText("Proyeksi terbit 17 Oktober 2026 tersedia setelah masuk")).toBeInTheDocument();
    expect(screen.getByText(/Proyeksi 6 bulan dari ICP resmi September 2026/)).toBeInTheDocument();
    expect(screen.getAllByRole("link", { name: "Masuk" }).some((a) => a.getAttribute("href") === "/masuk?next=%2Fproyeksi")).toBe(true);
    expect(await screen.findByText("Gratis, fitur terbatas")).toBeInTheDocument();
    expect(screen.getByText(/Kredit foto/)).toBeInTheDocument();
    expect(screen.getByRole("main")).toBeInTheDocument();
    expect(screen.getAllByRole("link", { name: "Metodologi & Data" }).every((a) => a.getAttribute("href") === "/metodologi")).toBe(true);
    expect(screen.queryByText(/testimoni/i)).toBeNull();
  });

  it("never renders forecast values on the landing page, even from an older snapshot that still sends them", async () => {
    vi.stubGlobal("fetch", async (url: string) => {
      if (url.startsWith("/api/v1/public/market-snapshot")) return json({ ...snapshot, latest_forecast: { id: "fc_1", issue_date: "2026-10-17", forecast_origin: "2026-09", method: "spread_brent_6m",
        points: [{ period: "2026-10", kind: "future_forecast", prediction: 112.8958, interval_80: [103.3, 121.9] }] } });
      if (url.startsWith("/api/v1/plans")) return json([plan]);
      return notFound();
    });
    const { container } = wrap(<App />);
    expect(await screen.findByText("Proyeksi terbit 17 Oktober 2026 tersedia setelah masuk")).toBeInTheDocument();
    expect(container.textContent).not.toMatch(/112,90|112\.90|spread_brent|fc_1/);
  });

  it("validates the login form before calling the API and switches language; no operator tab unless asked for", async () => {
    const fetchMock = vi.fn(async () => notFound());
    vi.stubGlobal("fetch", fetchMock);
    wrap(<App />, "/masuk");
    expect(await screen.findByRole("heading", { level: 1 })).toHaveTextContent("Masuk ke ICP Intelligence");
    expect(screen.queryByRole("button", { name: /Operator platform/ })).toBeNull();
    fireEvent.change(await screen.findByLabelText("Email"), { target: { value: "bukan-email" } });
    fireEvent.change(screen.getByLabelText("Kata sandi"), { target: { value: "x" } });
    fireEvent.click(screen.getByRole("button", { name: "Masuk" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Alamat email tidak valid.");
    expect(fetchMock).not.toHaveBeenCalledWith(expect.stringContaining("/auth/login"), expect.anything());
    const en = screen.getAllByRole("button", { name: "EN" });
    fireEvent.click(en[en.length - 1]!);
    expect(await screen.findByText("Sign in to ICP Intelligence", {}, { timeout: 4000 })).toBeInTheDocument();
    expect(screen.getByLabelText("Password")).toBeInTheDocument();
  });

  it("puts a space before the terms link on the registration form", async () => {
    vi.stubGlobal("fetch", async () => notFound());
    const { container } = wrap(<App />, "/daftar");
    await screen.findByRole("heading", { level: 1 });
    expect(container.textContent).toContain("Saya menyetujui Syarat Penggunaan dan Kebijakan Privasi.");
  });
});

describe("navigation and deep links", () => {
  it("redirects the legacy /forecast route to /proyeksi for a signed-in customer and hides internal cards", async () => {
    signedIn();
    wrap(<App />, "/forecast");
    expect(await screen.findByRole("heading", { level: 1, name: /Proyeksi Harga Minyak Mentah Indonesia/ })).toBeInTheDocument();
    expect(screen.queryByText("Sistem")).toBeNull();
    expect(screen.queryByText("Masuk sebagai operator")).toBeNull();
    expect(screen.getByRole("link", { name: "Syarat Penggunaan" })).toHaveAttribute("href", "/syarat");
  });

  it("opens the notifications tab from the query string and marks it in the sidebar", async () => {
    signedIn((path) => path === "/api/v1/alerts/events" ? json({ items: [], next_cursor: null, unread: 0 }) : path === "/api/v1/reports" ? json({ items: [], next_cursor: null }) : null);
    wrap(<App />, "/laporan?tab=pemberitahuan");
    expect(await screen.findByRole("heading", { level: 1 })).toHaveTextContent("Laporan siap pakai dan pemberitahuan");
    expect(screen.getByRole("tab", { name: "Pemberitahuan" })).toHaveAttribute("aria-selected", "true");
    const nav = screen.getByRole("navigation", { name: "Navigasi ruang kerja" });
    expect(within(nav).getByRole("link", { name: "Pemberitahuan" })).toHaveAttribute("aria-current", "page");
    expect(within(nav).getByRole("link", { name: "Laporan" })).not.toHaveAttribute("aria-current");
    await waitFor(() => expect(document.title).toBe("Pemberitahuan · ICP Intelligence"));
  });
});

describe("forecast presentation", () => {
  it("never shows internal identifiers (run ids, method ids, dataset version ids) to end users", () => {
    const { container } = wrap(<ForecastRunView run={run} history={[{ period: "2026-03", icp_price: 102.26 }]} level="0.8" onLevel={() => undefined} />);
    const text = container.textContent ?? "";
    for (const leak of ["fc_test", "spread_brent_12m", "dsv_1", "dsv_2"]) expect(text, leak).not.toContain(leak);
    expect(text).toContain("Brent + rata-rata selisih 12 bulan terakhir");
    expect(text).toContain("Data yang digunakan: ICP resmi (ESDM).");
    expect(text).not.toMatch(/[—–]/);
    for (const jargon of ["versi model", "Langkah", "kuantil", "sistem"]) expect(text, jargon).not.toContain(jargon);
  });

  it("distinguishes estimates awaiting publication from future forecasts and labels every input source", () => {
    wrap(<ForecastRunView run={run} history={[{ period: "2026-03", icp_price: 102.26 }]} level="0.8" onLevel={() => undefined} />);
    expect(screen.getAllByText("Estimasi (menunggu publikasi)").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Proyeksi").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Teramati").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Skenario Anda").length).toBeGreaterThan(0);
    expect(screen.getAllByText(/Nilai terakhir dipertahankan/).length).toBeGreaterThan(0);
    expect(screen.getAllByText("bersyarat pada skenario").length).toBeGreaterThan(0);
    expect(screen.getByText("USD 95,50")).toBeInTheDocument();
    expect(screen.getByText("Rp 1.700.000")).toBeInTheDocument();
    // ranges carry their unit
    expect(screen.getByText("USD 93,00 s.d. 98,00")).toBeInTheDocument();
  });
});
