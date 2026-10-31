/**
 * Public landing (wave B, P): the live market panel and its states, the 24-month chart and table,
 * the plan comparison from /plans and the calls to action. Guests never see forecast values
 * (policy v4), a stale gateway copy never reads as fresh (FE-02), and no skeleton survives a
 * failed request.
 */
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "../App";
import { clearSharedGets } from "../api/client";
import { AuthProvider } from "../auth";
import { niceTicks } from "../components/public/MarketChart";
import { I18nProvider } from "../i18n";

const json = (body: unknown, headers: Record<string, string> = {}, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", ...headers } });
/** A copy of the snapshot without some (optional) fields, as an older or partial API would send it. */
const without = (o: Record<string, unknown>, ...keys: string[]) => Object.fromEntries(Object.entries(o).filter(([k]) => !keys.includes(k)));
const html530 = () => new Response("<html>error 530</html>", { status: 530, headers: { "content-type": "text/html" } });
const notFound = () => json({ error: { code: "NOT_FOUND", message: "nope", request_id: null, details: {} } }, {}, 404);

const months = (from: [number, number], values: Array<[number | null, number | null]>) => values.map(([icp, brent], i) => {
  const total = from[0] * 12 + (from[1] - 1) + i;
  return { period: `${Math.floor(total / 12)}-${String((total % 12) + 1).padStart(2, "0")}`, icp_usd: icp, brent_usd: brent };
});

const SNAPSHOT = {
  icp_period: "2026-09", icp_usd: 112.81, previous_period: "2026-08", previous_usd: 89.43, mom_change_pct: 26.14,
  brent_usd: 114.1591, brent_period: "2026-09", usd_idr: 17749, icp_idr: 2002265,
  freshness: { status: "fresh", latest_period: "2026-09", expected_period: "2026-09", months_behind: 0 },
  source: "Kementerian ESDM (ICP resmi); EIA/World Bank (acuan); Bank Indonesia JISDOR (kurs)", updated_at: "2026-10-16T22:07:10.949Z",
  latest_forecast: { available: true, issue_date: "2026-10-17", forecast_origin: "2026-09", horizon: 6 },
  recent: months([2026, 4], [[117.31, 117.2875], [106.56, 107.1395], [83.45, 85.3991], [81.68, 83.7587], [89.43, 91.076], [112.81, 114.1591]]),
  coverage: { first_period: "2019-01", last_period: "2026-09", months: 93, confirmed_months: 86 },
  icp_evidence: { period: "2026-09", basis: "two_documents", label: { id: "Dua dokumen ESDM sepakat", en: "Two ESDM documents agree" }, documents: [
    { kind: "decree", reference: "395.K/MG.03/MEM.M/2026", published_on: null, url: "https://migas.esdm.go.id/cms/uploads/Kepmen_ICP_September_2026.pdf" },
    { kind: "announcement", reference: null, published_on: "2026-10-13", url: "https://www.migas.esdm.go.id/post/icp-september-2026" },
  ] },
  generated_at: "2026-10-25T11:10:15.404Z",
};
const PLANS = [
  { code: "trial", label_id: "Uji coba", label_en: "Trial", price_note_id: "Gratis, fitur terbatas", price_note_en: "Free, limited features", seats: 2, forecast_horizon_max: 3,
    scenarios: true, saved_scenarios_max: 2, reports_pdf: true, reports_excel: false, alerts: true, alert_rules_max: 2, api_access: false, api_requests_per_month: 0, history_years: 3, insights: true },
  { code: "professional", label_id: "Profesional", label_en: "Professional", price_note_id: "Untuk tim analis", price_note_en: "For analyst teams", seats: 20, forecast_horizon_max: 6,
    scenarios: true, saved_scenarios_max: 100, reports_pdf: true, reports_excel: true, alerts: true, alert_rules_max: 50, api_access: true, api_requests_per_month: 20000, history_years: 0, insights: true },
];

/** Fake public API: the snapshot and plan handlers can be swapped per test. */
function server(snapshot: () => Response | Promise<Response>, plans: () => Response | Promise<Response> = () => json(PLANS)) {
  const calls: string[] = [];
  vi.stubGlobal("fetch", (url: string, init: RequestInit = {}) => {
    const path = String(url).split("?")[0] ?? "";
    calls.push(path);
    if (init.signal?.aborted) return Promise.reject(new DOMException("aborted", "AbortError"));
    if (path === "/api/v1/public/market-snapshot") return Promise.resolve(snapshot());
    if (path === "/api/v1/plans") return Promise.resolve(plans());
    return Promise.resolve(notFound());
  });
  return calls;
}

const mount = (path = "/") => render(<MemoryRouter initialEntries={[path]}><I18nProvider initial="id"><AuthProvider><App /></AuthProvider></I18nProvider></MemoryRouter>);
/** A pointer event with a pointer type (jsdom has no PointerEvent constructor); React reads both fields from it. */
const pointer = (el: Element, type: "pointerdown" | "pointerout", pointerType: "touch" | "mouse", clientX = 0) => {
  const ev = new MouseEvent(type, { bubbles: true, cancelable: true, clientX });
  Object.defineProperty(ev, "pointerType", { value: pointerType });
  fireEvent(el, ev);
};
const panel = () => screen.getByRole("region", { name: "ICP resmi terbaru dan riwayat bulanannya" });
/** The headline number is rendered (the chart repeats it as an end label, so query the headline itself). */
const loaded = () => waitFor(() => expect(panel().querySelector(".mkt-num")).toHaveTextContent("112,81"));

afterEach(() => { cleanup(); vi.unstubAllGlobals(); vi.useRealTimers(); localStorage.clear(); sessionStorage.clear(); });

describe("live market panel", () => {
  it("shows the official value with its unit, period, change, Brent, the rupiah rate and the source documents", async () => {
    server(() => json(SNAPSHOT));
    mount();
    expect(await screen.findByRole("heading", { level: 1 })).toHaveTextContent("ICP resmi, lengkap dengan dokumen sumbernya.");
    await loaded();
    const p = panel();
    expect(within(p).getByText("USD/bbl")).toBeInTheDocument();
    expect(within(p).getByText("September 2026")).toBeInTheDocument();
    expect(within(p).getByText("Dua dokumen ESDM sepakat")).toBeInTheDocument();
    expect(within(p).getByText("dari USD 89,43 pada Agustus 2026")).toBeInTheDocument();
    expect(within(p).getByText("USD 114,16")).toBeInTheDocument();
    expect(within(p).getByText("Rp 2.002.265")).toBeInTheDocument();
    expect(within(p).getByText("dengan kurs JISDOR rata-rata Rp 17.749")).toBeInTheDocument();
    // Fresh, with document evidence: the evidence chip speaks, not a second "Mutakhir" badge.
    expect(p.textContent).not.toContain("Mutakhir");
    // Source documents open in a new tab without opener or referrer; the decree number is the API's own.
    const decree = within(p).getByRole("link", { name: /Kepmen ESDM 395\.K\/MG\.03\/MEM\.M\/2026/ });
    expect(decree).toHaveAttribute("href", SNAPSHOT.icp_evidence.documents[0]!.url);
    expect(decree).toHaveAttribute("target", "_blank");
    expect(decree).toHaveAttribute("rel", "noopener noreferrer");
    expect(within(p).getByRole("link", { name: /Pengumuman Ditjen Migas, 13 Oktober 2026/ })).toHaveAttribute("href", SNAPSHOT.icp_evidence.documents[1]!.url);
    // The first step names the latest month's own documents (decree number only as the API gives it).
    expect(screen.getByText("Dokumen untuk September 2026")).toBeInTheDocument();
    expect(screen.getByText("Kepmen ESDM 395.K/MG.03/MEM.M/2026 dan Pengumuman Ditjen Migas, 13 Oktober 2026")).toBeInTheDocument();
    // The verification step names the real counts from `coverage`.
    expect(screen.getByText("86 dari 93 bulan terkonfirmasi")).toBeInTheDocument();
    expect(screen.getByText(/Jan 2019 s\.d\. Sep 2026/)).toBeInTheDocument();
  });

  it("locks the forecast window for guests: horizon and issue date only, sign-in link, no values", async () => {
    server(() => json(SNAPSHOT));
    const { container } = mount();
    const lock = await screen.findByRole("link", { name: "Proyeksi 6 bulan · terbit 17 Oktober 2026 · masuk untuk melihat" });
    expect(lock).toHaveAttribute("href", "/masuk?next=%2Fproyeksi");
    // The hatched window exists in the chart, but carries no number.
    expect(container.querySelector(".mkt-zone")).not.toBeNull();
    expect(container.querySelector(".mkt-zone")?.textContent ?? "").toBe("");
  });

  it("keeps the forecast window free of text: the latest values sit in their own gutter before it", async () => {
    server(() => json(SNAPSHOT));
    const { container } = mount();
    await loaded();
    const zoneX = Number(container.querySelector(".mkt-zone-fill")?.getAttribute("x"));
    const labels = [...container.querySelectorAll(".mkt-end-label")];
    expect(labels.map((l) => l.textContent)).toEqual(["112,81", "114,16"]);
    for (const l of labels) {
      // Same advance the chart uses for placement (7.1 px per character at 12.5 px).
      const x = Number(l.getAttribute("x"));
      const right = l.getAttribute("text-anchor") === "end" ? x : x + (l.textContent ?? "").length * 7.1;
      expect(right).toBeLessThanOrEqual(zoneX);
    }
  });

  it("does not show a lock line for a forecast whose months are all official by now", async () => {
    server(() => json({ ...SNAPSHOT, latest_forecast: { available: true, issue_date: "2026-04-17", forecast_origin: "2026-03", horizon: 6 } }));
    const { container } = mount();
    await loaded();
    expect(screen.queryByRole("link", { name: /masuk untuk melihat/ })).toBeNull();
    expect(container.querySelector(".mkt-zone")).toBeNull();
    // A forecast was published, so the panel does not claim there is none.
    expect(panel().textContent).not.toContain("Belum ada proyeksi yang diterbitkan");
  });

  it("never renders forecast values, even when an older API still sends points", async () => {
    server(() => json({ ...SNAPSHOT, latest_forecast: { id: "fc_1", issue_date: "2026-10-17", forecast_origin: "2026-09", method: "spread_brent_6m",
      points: [{ period: "2026-10", kind: "future_forecast", prediction: 112.8958, interval_80: [103.3, 121.9] }] } }));
    const { container } = mount();
    expect(await screen.findByRole("link", { name: /Proyeksi 1 bulan · terbit 17 Oktober 2026/ })).toBeInTheDocument();
    fireEvent.click(within(panel()).getByRole("button", { name: "Tabel" }));
    expect(container.textContent).not.toMatch(/112,90|112\.90|112,89|103,3|121,9|spread_brent|fc_1/);
  });

  it("says when the figures are the gateway's stored copy and never calls them up to date", async () => {
    server(() => json(without(SNAPSHOT, "icp_evidence"), { "X-ICP-Cache": "stale", "X-ICP-Cached-At": "2026-10-24T08:15:00.000Z" }));
    mount();
    await loaded();
    expect(within(panel()).getByText(/^Layanan analitik belum menjawab saat ini\. Angka ini salinan publik terakhir yang tersimpan pada 24 Okt 2026, 15[.:]15 WIB\.$/)).toBeInTheDocument();
    expect(panel().textContent).not.toContain("Mutakhir");
    expect(within(panel()).getByText("Salinan tersimpan")).toBeInTheDocument();
  });

  it("keeps the frame on an outage: one alert with a retry and the official ESDM page, no skeleton left; retry recovers", async () => {
    let up = false;
    server(() => (up ? json(SNAPSHOT) : html530()), () => (up ? json(PLANS) : html530()));
    const { container } = mount();
    const alert = await within(panel()).findByRole("alert");
    expect(alert).toHaveTextContent("Data pasar belum dapat dimuat");
    expect(alert).toHaveTextContent("belum ada salinan publik tersimpan");
    expect(alert.textContent).not.toMatch(/Angka yang tampil tidak diperbarui/);
    const esdm = within(alert).getByRole("link", { name: /Lihat ICP resmi di situs Ditjen Migas ESDM/ });
    expect(esdm).toHaveAttribute("href", "https://www.migas.esdm.go.id/post/harga-minyak-mentah");
    expect(esdm).toHaveAttribute("rel", "noopener noreferrer");
    await screen.findByText("Perbandingan paket belum dapat dimuat");
    expect(container.querySelectorAll(".skeleton")).toHaveLength(0);
    // One page-level alert: the plans section explains itself without a second alert.
    expect(screen.getAllByRole("alert").filter((a) => a.textContent)).toHaveLength(1);
    up = true;
    fireEvent.click(within(alert).getByRole("button", { name: "Coba lagi" }));
    await loaded();
  });

  it("waits at most the public timeout: a 'still waiting' line after 3 s, then an explained error with no skeleton", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("fetch", (_url: string, init: RequestInit = {}) => new Promise<Response>((_res, rej) => {
      init.signal?.addEventListener("abort", () => rej(new DOMException("aborted", "AbortError")));
    }));
    const { container } = mount();
    await act(async () => { await vi.advanceTimersByTimeAsync(100); });
    expect(within(panel()).getByRole("status", { name: "Memuat data pasar publik" })).toBeInTheDocument();
    expect(panel().textContent).not.toContain("Server belum menjawab");
    await act(async () => { await vi.advanceTimersByTimeAsync(3100); });
    expect(panel()).toHaveTextContent("Server belum menjawab. Halaman ini menunggu paling lama 10 detik.");
    await act(async () => { await vi.advanceTimersByTimeAsync(7000); });
    expect(within(panel()).getByRole("alert")).toHaveTextContent(/tidak menjawab dalam 10 detik/);
    expect(container.querySelectorAll(".skeleton")).toHaveLength(0);
  });

  it("shows the facts without a chart when an older backend sends no history", async () => {
    server(() => json(without(SNAPSHOT, "recent", "coverage", "icp_evidence", "generated_at")));
    const { container } = mount();
    await loaded();
    expect(within(panel()).getByText(/Riwayat bulanan belum dikirim oleh server ini/)).toBeInTheDocument();
    expect(container.querySelector(".mkt-chart")).toBeNull();
    expect(screen.queryByText(/bulan terkonfirmasi/)).toBeNull();
  });

  it("says the value is withheld when the official documents of the latest month disagree, with no stray '-' values", async () => {
    const conflict = {
      ...SNAPSHOT, icp_usd: null, mom_change_pct: null, icp_idr: null,
      recent: SNAPSHOT.recent.map((m, i, all) => (i === all.length - 1 ? { ...m, icp_usd: null } : m)),
      icp_evidence: { period: "2026-09", basis: "conflict", label: { id: "Sumber tidak sepakat", en: "Sources disagree" }, documents: [] },
    };
    server(() => json(conflict));
    mount();
    expect(await within(await screen.findByRole("region", { name: "ICP resmi terbaru dan riwayat bulanannya" }))
      .findByText("nilai ditahan: dokumen resmi belum sepakat")).toBeInTheDocument();
    expect(within(panel()).getByText("Sumber tidak sepakat")).toBeInTheDocument();
    expect(panel().querySelector(".mkt-num")).toBeNull();
    // The rupiah equivalent is withheld with it: it says so instead of "- per barel" or "no data".
    expect(panel().textContent).not.toMatch(/-\s*per barel/);
    expect(within(panel()).getByText("ditahan sampai dokumen resmi ICP sepakat")).toBeInTheDocument();
    expect(panel().textContent).not.toContain("112,81");
    // Legend and summary separate the withheld month from months without an official value.
    expect(within(panel()).getByText("Bulan dengan nilai ditahan (dokumen resmi belum sepakat)")).toBeInTheDocument();
    expect(within(panel()).queryByText("Bulan tanpa nilai resmi")).toBeNull();
    const summary = [...panel().querySelectorAll(".sr-only")].map((e) => e.textContent ?? "").join(" ");
    expect(summary).toContain("Nilai September 2026 ditahan karena dokumen resmi belum sepakat");
    expect(summary).not.toContain("tanpa nilai resmi");
    // The chart labels only values of the newest month: August's ICP is not drawn in the latest position.
    expect([...panel().querySelectorAll(".mkt-end-label")].map((e) => e.textContent)).toEqual(["114,16"]);
    // The table names the month as withheld, not as a month without an official value.
    fireEvent.click(within(panel()).getByRole("button", { name: "Tabel" }));
    const rows = within(within(panel()).getByRole("table")).getAllByRole("row").slice(1);
    expect(rows[0]).toHaveTextContent("Sep 2026ditahan (dokumen resmi belum sepakat)114,16-");
    expect(panel().textContent).not.toContain("tidak ada nilai resmi");
  });
});

describe("24-month chart", () => {
  it("offers the same months as a table, newest first, with units and the ICP − Brent difference", async () => {
    server(() => json(SNAPSHOT));
    mount();
    await loaded();
    fireEvent.click(within(panel()).getByRole("button", { name: "Tabel" }));
    const table = within(panel()).getByRole("table", { name: /ICP resmi dan Brent per bulan, April 2026 s\.d\. September 2026/ });
    const rows = within(table).getAllByRole("row").slice(1);
    expect(rows).toHaveLength(SNAPSHOT.recent.length);
    expect(rows[0]).toHaveTextContent("Sep 2026112,81114,16−1,35");
    expect(rows[rows.length - 1]).toHaveTextContent("Apr 2026117,31117,29");
    fireEvent.click(within(panel()).getByRole("button", { name: "Grafik" }));
    expect(within(panel()).queryByRole("table")).toBeNull();
  });

  it("draws months without an official value as gaps: a break in the line, never a 0", async () => {
    const recent = SNAPSHOT.recent.map((m, i) => (i === 2 ? { ...m, icp_usd: null } : m));
    server(() => json({ ...SNAPSHOT, recent }));
    const { container } = mount();
    await loaded();
    const d = container.querySelector(".mkt-line.icp")?.getAttribute("d") ?? "";
    expect(d.match(/M/g)).toHaveLength(2);
    expect(container.querySelectorAll(".mkt-gap-mark")).toHaveLength(1);
    expect(within(panel()).getByText("Bulan tanpa nilai resmi")).toBeInTheDocument();
    fireEvent.click(within(panel()).getByRole("button", { name: "Tabel" }));
    const row = within(panel()).getByRole("row", { name: /Jun 2026/ });
    expect(row).toHaveTextContent("tidak ada nilai resmi");
    const cells = within(panel()).getAllByRole("cell").map((c) => c.textContent?.trim());
    expect(cells).not.toContain("0");
    expect(cells).not.toContain("0,00");
  });

  it("reads month by month from the keyboard (arrows), starting at the newest month", async () => {
    server(() => json(SNAPSHOT));
    mount();
    await loaded();
    const chart = within(panel()).getByRole("group", { name: /Grafik ICP resmi dan Brent, April 2026 s\.d\. September 2026/ });
    act(() => chart.focus());
    expect(chart).toHaveTextContent("September 2026: ICP USD 112,81, Brent USD 114,16, selisih −1,35");
    fireEvent.keyDown(chart, { key: "ArrowLeft" });
    expect(chart).toHaveTextContent("Agustus 2026: ICP USD 89,43, Brent USD 91,08, selisih −1,65");
    fireEvent.keyDown(chart, { key: "Home" });
    expect(chart).toHaveTextContent("April 2026: ICP USD 117,31");
  });

  it("keeps a tapped month on screen after the finger lifts; a mouse leaving clears it; never over the forecast window", async () => {
    server(() => json(SNAPSHOT));
    const { container } = mount();
    await loaded();
    const svg = container.querySelector(".mkt-chart svg")!;
    pointer(svg, "pointerdown", "touch", 120);
    expect(container.querySelector(".mkt-tip")).not.toBeNull();
    // A finger lifting fires pointerleave (React: pointerout); the tooltip stays.
    pointer(svg, "pointerout", "touch");
    expect(container.querySelector(".mkt-tip")).not.toBeNull();
    pointer(svg, "pointerout", "mouse");
    expect(container.querySelector(".mkt-tip")).toBeNull();
    // A tap over the hatched window shows nothing (it has no values to show).
    const zone = container.querySelector(".mkt-zone-fill")!;
    pointer(svg, "pointerdown", "touch", Number(zone.getAttribute("x")) + 4);
    expect(container.querySelector(".mkt-tip")).toBeNull();
  });

  it("uses clean tick steps that enclose every value", () => {
    expect(niceTicks([61.1, 117.31])).toEqual([60, 80, 100, 120]);
    expect(niceTicks([89.43, 91.08], 4)).toEqual([89, 89.5, 90, 90.5, 91, 91.5]);
  });
});

describe("product tour", () => {
  it("switches question and screenshot with WAI-ARIA tabs; screenshots are lazy, sized and captioned", async () => {
    server(() => json(SNAPSHOT));
    mount();
    const list = await screen.findByRole("tablist", { name: "Bagian ruang kerja" });
    const insight = within(list).getByRole("tab", { name: "Insight" });
    fireEvent.click(insight);
    expect(insight).toHaveAttribute("aria-selected", "true");
    const tabpanel = screen.getByRole("tabpanel", { name: "Insight" });
    expect(within(tabpanel).getByRole("heading", { name: "Apa yang berubah bulan ini, dan mengapa itu penting?" })).toBeInTheDocument();
    const img = within(tabpanel).getByRole("img", { name: /Tangkapan layar halaman Insight/ });
    expect(img).toHaveAttribute("src", "/img/product/insight.webp");
    expect(img).toHaveAttribute("loading", "lazy");
    expect(img).toHaveAttribute("width");
    expect(img).toHaveAttribute("height");
    expect(within(tabpanel).getByText("Tangkapan layar aplikasi dengan data resmi hingga September 2026.")).toBeInTheDocument();
  });
});

describe("plans and calls to action", () => {
  it("builds the plan comparison from /plans, with every entitlement and the operator-approval note", async () => {
    server(() => json(SNAPSHOT));
    mount();
    const table = await screen.findByRole("table", { name: "Perbandingan paket ICP Intelligence" });
    expect(within(table).getByRole("columnheader", { name: /Uji coba/ })).toHaveTextContent("Gratis, fitur terbatas");
    expect(within(table).getByRole("columnheader", { name: /Profesional/ })).toBeInTheDocument();
    expect(within(table).getByRole("row", { name: /Kursi pengguna/ })).toHaveTextContent("Kursi pengguna220");
    expect(within(table).getByRole("row", { name: /Format laporan/ })).toHaveTextContent("PDF, CSVPDF, Excel, CSV");
    expect(within(table).getByRole("row", { name: /Akses API/ })).toHaveTextContent("Tidak termasuk20.000 permintaan per bulan");
    expect(within(table).getByRole("row", { name: /Riwayat data/ })).toHaveTextContent("3 tahunRiwayat penuh");
    expect(screen.getByText(/disetujui oleh operator layanan/)).toHaveTextContent("Tidak ada pembayaran online.");
  });

  it("says when the plan list is the gateway's stored copy, and explains an empty list instead of an empty table", async () => {
    server(() => json(SNAPSHOT), () => json(PLANS, { "X-ICP-Cache": "stale", "X-ICP-Cached-At": "2026-10-24T08:15:00.000Z" }));
    mount();
    await screen.findByRole("table", { name: "Perbandingan paket ICP Intelligence" });
    expect(screen.getByText(/^Layanan analitik belum menjawab saat ini\. Daftar paket ini salinan publik terakhir yang tersimpan pada 24 Okt 2026, 15[.:]15 WIB\.$/)).toBeInTheDocument();
    cleanup();
    vi.unstubAllGlobals();
    clearSharedGets();
    server(() => json(SNAPSHOT), () => json([]));
    mount();
    expect(await screen.findByText("Daftar paket belum tersedia saat ini.")).toBeInTheDocument();
    expect(screen.queryByRole("table", { name: "Perbandingan paket ICP Intelligence" })).toBeNull();
  });

  it("sends every call to action where it says", async () => {
    server(() => json(SNAPSHOT));
    mount();
    await loaded();
    for (const a of screen.getAllByRole("link", { name: "Buat akun uji coba" })) expect(a).toHaveAttribute("href", "/daftar");
    expect(screen.getByRole("link", { name: "Lihat cara kerjanya" })).toHaveAttribute("href", "/#cara-kerja");
    const nav = screen.getByRole("navigation", { name: "Navigasi situs" });
    expect(within(nav).getByRole("link", { name: "Produk" })).toHaveAttribute("href", "/#produk");
    expect(within(nav).getByRole("link", { name: "Data & metodologi" })).toHaveAttribute("href", "/metodologi");
    expect(within(nav).getByRole("link", { name: "Paket" })).toHaveAttribute("href", "/#paket");
    expect(within(nav).getByRole("link", { name: "Masuk" })).toHaveAttribute("href", "/masuk");
    expect(within(nav).getByRole("link", { name: "Buat akun" })).toHaveAttribute("href", "/daftar");
    expect(screen.getByRole("link", { name: /Baca metodologi dan hasil uji mundurnya/ })).toHaveAttribute("href", "/metodologi#akurasi");
    // Sections the header links point to exist.
    for (const id of ["produk", "cara-kerja", "paket", "sumber"]) expect(document.getElementById(id)).not.toBeNull();
  });

  it("closes the phone menu on Escape (focus back on the button) and on an outside click", async () => {
    server(() => json(SNAPSHOT));
    mount();
    const button = await screen.findByRole("button", { name: "Menu" });
    fireEvent.click(button);
    expect(button).toHaveAttribute("aria-expanded", "true");
    act(() => within(screen.getByRole("navigation", { name: "Navigasi situs" })).getByRole("link", { name: "Produk" }).focus());
    fireEvent.keyDown(window, { key: "Escape" });
    expect(button).toHaveAttribute("aria-expanded", "false");
    await waitFor(() => expect(button).toHaveFocus());
    fireEvent.click(button);
    expect(button).toHaveAttribute("aria-expanded", "true");
    fireEvent.pointerDown(document.getElementById("cara-kerja")!);
    expect(button).toHaveAttribute("aria-expanded", "false");
  });
});

describe("registration page", () => {
  it("holds the trial facts' place while /plans loads, then states them; a stored copy without a time says so", async () => {
    let release: (r: Response) => void = () => undefined;
    server(() => json(SNAPSHOT), () => new Promise<Response>((res) => { release = res; }));
    mount("/daftar");
    expect(await screen.findByRole("status", { name: "Memuat rincian paket Uji coba" })).toBeInTheDocument();
    await act(async () => { release(json(PLANS, { "X-ICP-Cache": "stale" })); });
    expect(await screen.findByText("Paket Uji coba mencakup")).toBeInTheDocument();
    expect(screen.getByText("Horizon hingga 3 bulan")).toBeInTheDocument();
    expect(screen.queryByRole("status", { name: "Memuat rincian paket Uji coba" })).toBeNull();
    expect(screen.getByText("Salinan publik terakhir yang tersimpan")).toBeInTheDocument();
    expect(document.body.textContent).not.toMatch(/tersimpan -/);
  });
});
