/**
 * Product boundary (access policy v4): guests never get the app shell, customers never see
 * operations, operators get an operations-first console, a temporary password keeps the member on
 * /akun#keamanan, and scenario numbers are entered safely.
 */
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { useState } from "react";
import { MemoryRouter, useLocation } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import { api } from "./api/client";
import { AuthProvider } from "./auth";
import { ToastProvider } from "./components/toast";
import { I18nProvider } from "./i18n";
import { checkNumber, parseDecimal } from "./lib/number";
import { emptyScenario, ScenarioEditor, type ScenarioMap } from "./pages/ForecastPage";

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}
const err = (status: number, code: string) => json({ error: { code, message: code, request_id: "req_1", details: {} } }, status);
const customer = { authenticated: true, name: "Penguji UX", role: "owner", kind: "user", org_id: "org_1", org_name: "PT Uji", plan: "trial", must_change_password: false, horizon_max: 3 };
const viewer = { ...customer, role: "viewer" };
const operator = { authenticated: true, name: "Operator", role: "operator", kind: "operator", org_id: null, org_name: null, plan: null, must_change_password: false, horizon_max: 6 };

let lastLocation = "";
function Probe() {
  const loc = useLocation();
  lastLocation = loc.pathname + loc.search + loc.hash;
  return null;
}
const mount = (path: string) => render(
  <MemoryRouter initialEntries={[path]}>
    <I18nProvider initial="id"><ToastProvider><AuthProvider><App /><Probe /></AuthProvider></ToastProvider></I18nProvider>
  </MemoryRouter>,
);

/** Fake API: answers whoami with `who`, records every path, `extra` may answer others (default 404). */
function server(who: unknown, extra: (path: string) => Response | null = () => null) {
  const calls: string[] = [];
  vi.stubGlobal("fetch", async (url: string) => {
    const path = url.split("?")[0] ?? "";
    calls.push(path);
    if (path === "/api/v1/auth/whoami") return json(who);
    return extra(path) ?? err(404, "NOT_FOUND");
  });
  return calls;
}
const asCustomer = (who: unknown = customer, extra?: (path: string) => Response | null) => { localStorage.setItem("icp.sessionToken", "icp_sess_test"); return server(who, extra); };
const asOperator = (extra?: (path: string) => Response | null) => { sessionStorage.setItem("icp.operatorToken", "op_test"); return server(operator, extra); };

afterEach(() => { cleanup(); vi.unstubAllGlobals(); localStorage.clear(); sessionStorage.clear(); lastLocation = ""; });

describe("guests", () => {
  it("are sent to sign-in with the deep link before any app shell renders", async () => {
    const calls = server({ authenticated: false, kind: "public", role: "public", name: null, horizon_max: 0, must_change_password: false });
    mount("/skenario?x=1");
    await waitFor(() => expect(lastLocation).toBe("/masuk?next=%2Fskenario%3Fx%3D1"));
    expect(await screen.findByRole("heading", { level: 1, name: "Masuk ke ICP Intelligence" })).toBeInTheDocument();
    expect(screen.queryByRole("navigation", { name: "Navigasi ruang kerja" })).toBeNull();
    // no protected endpoint was called on the guest's behalf
    expect(calls.filter((c) => !c.startsWith("/api/v1/auth/"))).toEqual([]);
  });

  for (const path of ["/ringkasan", "/harga", "/proyeksi", "/insight", "/laporan", "/akun"]) {
    it(`cannot open ${path}`, async () => {
      server({ authenticated: false, kind: "public", role: "public", name: null, horizon_max: 0, must_change_password: false });
      mount(path);
      await waitFor(() => expect(lastLocation).toBe(`/masuk?next=${encodeURIComponent(path)}`));
    });
  }

  it("opening the operations console preselects the operator sign-in", async () => {
    server({ authenticated: false, kind: "public" });
    mount("/operasi/data");
    await waitFor(() => expect(lastLocation).toBe("/masuk?next=%2Foperasi%2Fdata&operator=1"));
    expect(await screen.findByLabelText("Token operator")).toBeInTheDocument();
  });

  it("can read the public methodology page without any API call", async () => {
    const calls = server({ authenticated: false, kind: "public" });
    const { container } = mount("/metodologi");
    expect(await screen.findByRole("heading", { level: 1, name: /Dari mana angka berasal/ })).toBeInTheDocument();
    for (const h of ["Apa itu ICP dan kapan terbit", "Estimasi dan proyeksi", "Sumber data dan lisensi", "Pencocokan dengan dokumen resmi", "Ketidakpastian dan akurasi", "Keterbatasan"]) {
      expect(screen.getByRole("heading", { level: 2, name: h })).toBeInTheDocument();
    }
    expect(container.textContent).toContain("Desember 2020 s.d. Agustus 2026");
    expect(container.textContent).not.toMatch(/versi model|dsv_|fc_|RBRTE|MLflow|baseline/i);
    expect(calls).toEqual([]);
    await waitFor(() => expect(document.title).toBe("Metodologi & Data · ICP Intelligence"));
  });
});

describe("customers", () => {
  it("get the workspace navigation (Pasar / Analisis / Keluaran) and never the operations console", async () => {
    asCustomer();
    mount("/ringkasan");
    const nav = await screen.findByRole("navigation", { name: "Navigasi ruang kerja" });
    for (const label of ["Pasar", "Analisis", "Keluaran", "Ringkasan", "Riwayat Harga", "Proyeksi", "Simulasi Skenario", "Insight", "Laporan", "Pemberitahuan"]) {
      expect(nav).toHaveTextContent(label);
    }
    expect(nav).not.toHaveTextContent("Kesehatan Sistem");
    expect(screen.getByRole("link", { name: "Akun & Organisasi" })).toHaveAttribute("href", "/akun");
    expect(screen.getByText("Paket Uji coba")).toBeInTheDocument();
    await waitFor(() => expect(document.title).toBe("Ringkasan · ICP Intelligence"));
  });

  it("see a plain 'not found' on operations URLs, without any operations request", async () => {
    const calls = asCustomer();
    mount("/operasi");
    expect(await screen.findByRole("heading", { name: "Halaman tidak ditemukan" })).toBeInTheDocument();
    expect(calls).not.toContain("/api/v1/system/status");
    expect(document.body.textContent).not.toMatch(/token operator/i);
  });

  it("with a temporary password are kept on /akun#keamanan and organisation endpoints are not called", async () => {
    const calls = asCustomer({ ...customer, must_change_password: true });
    mount("/ringkasan");
    await waitFor(() => expect(lastLocation).toBe("/akun#keamanan"));
    expect(await screen.findByText(/Ganti kata sandi sementara terlebih dahulu/)).toBeInTheDocument();
    expect(calls).not.toContain("/api/v1/org");
    expect(calls).not.toContain("/api/v1/alerts/events");
  });

  it("are routed to /akun#keamanan when the server answers 403 PASSWORD_CHANGE_REQUIRED", async () => {
    asCustomer(customer, (path) => path === "/api/v1/data/summary" ? err(403, "PASSWORD_CHANGE_REQUIRED") : null);
    mount("/harga");
    await screen.findByRole("navigation", { name: "Navigasi ruang kerja" });
    await act(async () => { await api("/data/summary").catch(() => undefined); });
    await waitFor(() => expect(lastLocation).toBe("/akun#keamanan"));
    expect((await screen.findAllByText(/Ganti kata sandi sementara/)).length).toBeGreaterThan(0);
  }, 30_000);

  it("with the Viewer role see why scenario controls are disabled", async () => {
    asCustomer(viewer, (path) => path === "/api/v1/scenarios" ? json({ items: [], next_cursor: null }) : null);
    mount("/skenario");
    expect(await screen.findByText(/Peran Pembaca dapat melihat dan membandingkan skenario/)).toBeInTheDocument();
  });
});

describe("operators", () => {
  it("get an operations-first console with a product preview and no customer-only items", async () => {
    asOperator((path) => path === "/api/v1/system/status" ? json({ status: "ready", environment: "local", version: "2.0.0", checks: [], metadata_backend: "sqlite", object_store_backend: "local", schema_version: 4, expected_schema_version: 4, model_source: "bundle", admin_enabled: true }) : null);
    mount("/operasi");
    const nav = await screen.findByRole("navigation", { name: "Navigasi konsol operasi" });
    for (const label of ["Operasi", "Kesehatan Sistem", "Data & Versi", "Model & Evaluasi", "Pekerjaan", "Audit", "Organisasi", "Pratinjau produk", "Ringkasan", "Riwayat Harga", "Proyeksi", "Insight"]) {
      expect(nav).toHaveTextContent(label);
    }
    for (const label of ["Simulasi Skenario", "Laporan", "Pemberitahuan"]) expect(within(nav).queryByRole("link", { name: label })).toBeNull();
    expect(screen.queryByRole("link", { name: "Akun & Organisasi" })).toBeNull();
    expect(await screen.findByText("Konsol operasi · lingkungan local · API 2.0.0")).toBeInTheDocument();
  });

  it("see a clear notice on customer-only pages instead of an error, and no customer endpoint is called", async () => {
    const calls = asOperator();
    mount("/skenario");
    expect(await screen.findByRole("heading", { level: 1, name: "Halaman ini untuk pelanggan" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Buka Organisasi/ })).toHaveAttribute("href", "/operasi/organisasi");
    expect(calls.some((c) => c.startsWith("/api/v1/scenarios") || c.startsWith("/api/v1/org") || c.startsWith("/api/v1/alerts"))).toBe(false);
  });
});

describe("number entry", () => {
  it("parses Indonesian and English decimals and thousands grouping where it is expected", () => {
    expect(parseDecimal("95,5")).toBe(95.5);
    expect(parseDecimal("95.5")).toBe(95.5);
    expect(parseDecimal("95,")).toBe(95);
    expect(parseDecimal("")).toBeNull();
    expect(parseDecimal("abc")).toBeNaN();
    expect(parseDecimal("1.234,5")).toBe(1234.5);
    expect(parseDecimal("1,234.5")).toBe(1234.5);
    expect(parseDecimal("16.500", { grouping: true })).toBe(16500);
    expect(parseDecimal("16,500", { grouping: true })).toBe(16500);
    expect(parseDecimal("95.500")).toBe(95.5);
    expect(checkNumber("9500", { min: 10, max: 250 })).toEqual({ value: null, error: "range" });
  });

  function Harness({ onValue, onValid }: { onValue: (v: ScenarioMap) => void; onValid: (v: boolean) => void }) {
    const [v, setV] = useState<ScenarioMap>(emptyScenario());
    return <I18nProvider initial="id"><ScenarioEditor periods={["2026-10"]} value={v} onChange={(n) => { setV(n); onValue(n); }} onValidity={onValid} /></I18nProvider>;
  }

  for (const typed of ["95.5", "95,5"]) {
    it(`keeps "${typed}" as typed, key by key, and stores 95.5`, () => {
      const onValue = vi.fn();
      render(<Harness onValue={onValue} onValid={() => undefined} />);
      const input = screen.getByLabelText("Brent (USD/bbl) Okt 2026") as HTMLInputElement;
      let text = "";
      for (const ch of typed) { text += ch; fireEvent.change(input, { target: { value: text } }); expect(input.value).toBe(text); }
      expect(onValue.mock.calls.at(-1)?.[0].brent).toEqual({ "2026-10": 95.5 });
    });
  }

  it("shows an inline error for text and for an out-of-range value, and reports the editor invalid", () => {
    const onValid = vi.fn();
    render(<Harness onValue={() => undefined} onValid={onValid} />);
    const input = screen.getByLabelText("Brent (USD/bbl) Okt 2026") as HTMLInputElement;
    fireEvent.change(input, { target: { value: "abc" } });
    expect(input.value).toBe("abc");
    expect(screen.getByText("Masukkan angka, misalnya 95,5.")).toBeInTheDocument();
    expect(input).toHaveAttribute("aria-invalid", "true");
    expect(onValid).toHaveBeenLastCalledWith(false);
    fireEvent.change(input, { target: { value: "9500" } });
    fireEvent.blur(input);
    expect(screen.getByText("Di luar rentang wajar: 10 s.d. 250 USD/bbl.")).toBeInTheDocument();
    expect(onValid).toHaveBeenLastCalledWith(false);
    fireEvent.change(input, { target: { value: "95,5" } });
    expect(onValid).toHaveBeenLastCalledWith(true);
  });
});
