/**
 * Insight feed: editorial layout (lead, compact items, grouped rows), importance shown without
 * alarm colours, the detail sheet as an accessible dialog deep-linked by ?insight=, Back/Escape
 * closing it with focus restored, the v1 payload fallback, `?lang=` and the plan gate.
 */
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter, useLocation, useNavigate } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "../App";
import { AuthProvider } from "../auth";
import { ToastProvider } from "../components/toast";
import { I18nProvider, type Locale } from "../i18n";

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}
const err = (status: number, code: string) => json({ error: { code, message: code, request_id: "req_1", details: {} } }, status);
const customer = { authenticated: true, name: "Penguji UX", role: "owner", kind: "user", org_id: "org_1", org_name: "PT Uji", plan: "trial", must_change_password: false, horizon_max: 3 };
const plan = (insights: boolean) => [{ code: "trial", label_id: "Uji coba", label_en: "Trial", price_note_id: "Gratis", seats: 2, forecast_horizon_max: 3, scenarios: true,
  saved_scenarios_max: 2, reports_pdf: true, reports_excel: false, alerts: true, alert_rules_max: 2, api_access: false, api_requests_per_month: 0, history_years: 3, insights }];

const fact = (key: string, label: string, display: string) => ({ key, label, value: 1, unit: "", display });
function item(id: string, kind: string, importance: "high" | "medium" | "low", title: string, extra: Record<string, unknown> = {}) {
  return {
    id, rule_id: `${kind}_rule`, rule_version: 1, kind, importance, severity: importance === "high" ? "warning" : importance === "medium" ? "notice" : "info",
    audience: "customer", period: "2026-09", title, body: `Ringkasan ${title}.`, next_step: "Periksa Riwayat Harga.", generated: true,
    facts: [fact("v", "ICP Sep 2026", "112,81 USD/bbl"), fact("p", "ICP Agu 2026", "89,43 USD/bbl")],
    evidence: {
      datasets: [{ dataset: "icp", label: "ICP resmi", provider: "Kementerian ESDM", version_id: "dsv_icp_9", period_end: "2026-09" }],
      periods: { subject: "2026-09", window_start: "2023-09", window_end: "2026-08", n: 36 },
      inputs: [fact("i1", "ICP Sep 2026", "112,81 USD/bbl")], outputs: [fact("o1", "Perubahan", "+26,1 %")],
      method: { key: "mom", text: "Perubahan dibanding bulan sebelumnya." }, model: null, forecast_ids: [],
    },
    metrics: [], sources: [], ...extra,
  };
}
const v2 = {
  generated_at: "2026-10-19T13:53:00Z", lang: "id", engine_version: "2026-10.1", basis: { icp_period: "2026-09", datasets: {} },
  disclaimer: "Dihasilkan dengan aturan tetap.",
  insights: [
    item("ins_fresh", "freshness", "high", "Data Brent tertinggal satu bulan"),
    item("ins_spread", "benchmark_divergence", "medium", "Selisih ICP dan Dubai di luar rentang 36 bulan", {
      evidence: { ...item("x", "x", "low", "x").evidence, datasets: [{ dataset: "icp", label: "ICP", provider: "ESDM", version_id: null, period_end: "2026-09" }, { dataset: "dubai", label: "Dubai", provider: "Bank Dunia", version_id: null, period_end: "2026-09" }] },
    }),
    item("ins_fc", "forecast_outlook", "medium", "Proyeksi Oktober 2026", { evidence: { ...item("x", "x", "low", "x").evidence, forecast_ids: ["fc_77"] } }),
    item("ins_mom", "material_change", "low", "ICP September 2026 naik 26,1 %"),
    item("ins_yoy", "annual_comparison", "low", "Dibanding September 2025"),
    item("ins_fx", "fx_context", "low", "Dalam rupiah per barel"),
  ],
};
const v1 = {
  generated_at: "2026-10-19T13:53:00Z", basis: { icp_period: "2026-09" }, disclaimer: "Catatan lama.",
  insights: [{ id: "old_1", kind: "material_change", severity: "warning", period: "2026-09", title: "Judul lama", body: "Isi lama.", next_step: null, generated: true,
    metrics: [{ label: "ICP", value: 112.81, unit: "USD/bbl" }], sources: [{ dataset: "icp", label: "ICP resmi", provider: "ESDM", version_id: null, period_end: "2026-09" }] }],
};

let lastLocation = "";
let goBack: (() => void) | null = null;
function Probe() {
  const loc = useLocation();
  const navigate = useNavigate();
  lastLocation = loc.pathname + loc.search;
  goBack = () => navigate(-1);
  return null;
}
const mount = (path: string, locale: Locale = "id") => render(
  <MemoryRouter initialEntries={[path]}>
    <I18nProvider initial={locale}><ToastProvider><AuthProvider><App /><Probe /></AuthProvider></ToastProvider></I18nProvider>
  </MemoryRouter>,
);
function server(insights: () => Response, opts: { inPlan?: boolean } = {}) {
  const urls: string[] = [];
  localStorage.setItem("icp.sessionToken", "icp_sess_test");
  vi.stubGlobal("fetch", async (url: string) => {
    urls.push(url);
    const path = url.split("?")[0] ?? "";
    if (path === "/api/v1/auth/whoami") return json(customer);
    if (path === "/api/v1/plans") return json(plan(opts.inPlan ?? true));
    if (path === "/api/v1/insights") return insights();
    return err(404, "NOT_FOUND");
  });
  return urls;
}

afterEach(() => { cleanup(); vi.unstubAllGlobals(); localStorage.clear(); sessionStorage.clear(); lastLocation = ""; goBack = null; });

describe("insight feed", () => {
  it("leads with the most important item, then compact items and rows grouped by category, without alarm colours", async () => {
    const urls = server(() => json(v2));
    const { container } = mount("/insight");
    expect(await screen.findByRole("heading", { level: 2, name: "Data Brent tertinggal satu bulan" })).toBeInTheDocument();
    const side = screen.getByRole("complementary", { name: "Ringkas" });
    expect(within(side).getAllByRole("listitem")).toHaveLength(3);
    // items 5+ are one-line rows under their category heading
    expect(screen.getByRole("heading", { level: 2, name: "Perbandingan tahunan" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { level: 2, name: "Kurs" })).toBeInTheDocument();
    // the value strip renders server text with units
    expect(screen.getAllByText("112,81 USD/bbl").length).toBeGreaterThan(0);
    // importance by marker, never warn/bad tones
    expect(container.querySelectorAll(".imp-high").length).toBeGreaterThan(0);
    expect(container.querySelectorAll(".insight-feed .warn, .ins-feed .warn, .ins-feed .bad, .ins-feed .badge.warn, .ins-feed .badge.bad")).toHaveLength(0);
    // the generated note appears once, not per item
    expect(screen.getAllByText(/Dihasilkan otomatis dengan aturan tetap/)).toHaveLength(1);
    expect(urls.some((u) => u.startsWith("/api/v1/insights?lang=id"))).toBe(true);
  });

  it("opens the detail as a dialog with ?insight=, closes on Escape and Back, and returns focus to the trigger", async () => {
    server(() => json(v2));
    mount("/insight");
    const row = await screen.findByRole("button", { name: /Dalam rupiah per barel/ });
    row.focus();
    fireEvent.click(row);
    const dialog = await screen.findByRole("dialog", { name: "Dalam rupiah per barel" });
    expect(lastLocation).toBe("/insight?insight=ins_fx");
    expect(within(dialog).getByRole("heading", { level: 2, name: "Dalam rupiah per barel" })).toHaveFocus();
    for (const s of ["Apa yang terjadi", "Mengapa ditampilkan", "Angka pendukung", "Periode pembanding", "Cara menghitung", "Yang perlu diperiksa berikutnya", "Batasan"]) {
      expect(within(dialog).getByRole("heading", { level: 3, name: s })).toBeInTheDocument();
    }
    expect(within(dialog).getByText("Detail teknis")).toBeInTheDocument();
    fireEvent.keyDown(document, { key: "Escape" });
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(lastLocation).toBe("/insight");
    await waitFor(() => expect(row).toHaveFocus());

    // Browser Back closes it as well.
    fireEvent.click(row);
    await screen.findByRole("dialog");
    act(() => goBack?.());
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(lastLocation).toBe("/insight");
  }, 30_000);

  it("deep links straight into an insight and offers context links", async () => {
    server(() => json(v2));
    mount("/insight?insight=ins_spread");
    const dialog = await screen.findByRole("dialog", { name: /Selisih ICP dan Dubai/ });
    expect(within(dialog).getByRole("link", { name: /Buka Riwayat harga/ })).toHaveAttribute("href", "/harga?rentang=3&acuan=dubai");
    fireEvent.keyDown(document, { key: "Escape" });
    await waitFor(() => expect(lastLocation).toBe("/insight"));
    // a forecast insight links to its forecast issue
    cleanup();
    server(() => json(v2));
    mount("/insight?insight=ins_fc");
    const d2 = await screen.findByRole("dialog", { name: /Proyeksi Oktober 2026/ });
    expect(within(d2).getByRole("link", { name: /Buka proyeksi/ })).toHaveAttribute("href", "/proyeksi?proyeksi=fc_77");
  }, 30_000);

  it("filters by category through the URL", async () => {
    server(() => json(v2));
    mount("/insight");
    fireEvent.click(await screen.findByRole("button", { name: "Kurs" }));
    await waitFor(() => expect(lastLocation).toBe("/insight?kategori=fx_context"));
    expect(screen.getByRole("heading", { level: 2, name: "Dalam rupiah per barel" })).toBeInTheDocument();
    expect(screen.queryByText("Data Brent tertinggal satu bulan")).toBeNull();
  });

  it("still renders an older (v1) payload: severity becomes importance, metrics become figures, evidence is marked missing", async () => {
    server(() => json(v1));
    mount("/insight");
    expect(await screen.findByRole("heading", { level: 2, name: "Judul lama" })).toBeInTheDocument();
    expect(screen.getByText("112,81 USD/bbl")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /Buka insight: Judul lama/ }));
    const dialog = await screen.findByRole("dialog", { name: "Judul lama" });
    expect(within(dialog).getByText(/Bukti rinci belum tersedia/)).toBeInTheDocument();
  });

  it("asks the server for English text in English", async () => {
    const urls = server(() => json({ ...v2, lang: "en" }));
    mount("/insight", "en");
    await screen.findByRole("heading", { level: 2, name: "Data Brent tertinggal satu bulan" });
    expect(urls.some((u) => u.startsWith("/api/v1/insights?lang=en"))).toBe(true);
    expect(screen.getByText(/Generated automatically with fixed rules/)).toBeInTheDocument();
  });

  it("shows the plan gate without calling /insights when the plan has no insights", async () => {
    const urls = server(() => json(v2), { inPlan: false });
    mount("/insight");
    expect(await screen.findByText("Insight tidak termasuk dalam paket Anda")).toBeInTheDocument();
    expect(urls.some((u) => u.startsWith("/api/v1/insights"))).toBe(false);
  });

  it("has defined empty and error states", async () => {
    server(() => json({ ...v2, insights: [] }));
    mount("/insight");
    expect(await screen.findByText("Belum ada insight untuk periode ini")).toBeInTheDocument();
    expect(within(screen.getByRole("main")).getByRole("link", { name: "Riwayat harga" })).toHaveAttribute("href", "/harga");
    cleanup();
    server(() => err(503, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE"));
    mount("/insight");
    expect(await screen.findByText("Layanan data sedang tidak tersedia")).toBeInTheDocument();
    // one error notice (the toast region is an empty, separate live region)
    expect(screen.getAllByText("Layanan data sedang tidak tersedia")).toHaveLength(1);
  });
});
