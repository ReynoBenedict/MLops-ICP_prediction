/**
 * Customer workflows v5: complete plan cards, the plan-change request flow (no payment),
 * notifications (links, instant badge refresh) and reports (parameters per row, deep links).
 */
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "../App";
import { AuthProvider } from "../auth";
import { ToastProvider } from "../components/toast";
import { I18nProvider } from "../i18n";

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}
const err = (status: number, code: string) => json({ error: { code, message: code, request_id: "req_1", details: {} } }, status);

const trial = { code: "trial", label_id: "Uji coba", label_en: "Trial", price_note_id: "Gratis, fitur terbatas", price_note_en: "Free, limited", seats: 2, forecast_horizon_max: 3, scenarios: true,
  saved_scenarios_max: 2, reports_pdf: true, reports_excel: false, alerts: true, alert_rules_max: 2, api_access: false, api_requests_per_month: 0, history_years: 3, insights: true };
const pro = { ...trial, code: "professional", label_id: "Profesional", label_en: "Professional", price_note_id: "Harga sesuai kesepakatan", price_note_en: "Price by agreement", seats: 10,
  forecast_horizon_max: 6, saved_scenarios_max: 50, reports_excel: true, alert_rules_max: 50, api_access: true, api_requests_per_month: 20000, history_years: 0 };
const org = { id: "org_1", name: "PT Uji", slug: "pt-uji", plan: "trial", status: "active", created_at: "2026-10-01T00:00:00Z", entitlements: trial, plans: [trial, pro],
  members: 1, active_members: 1, saved_scenarios: 0, alert_rules: 0, usage_this_month: {} };
const me = { user: { id: "u1", name: "Penguji UX", email: "ux@test.id", role: "owner", status: "active", created_at: "2026-10-01T00:00:00Z", last_login_at: null, must_change_password: false }, organization: org, sessions: [] };
const who = (role: string) => ({ authenticated: true, name: "Penguji UX", role, kind: "user", org_id: "org_1", org_name: "PT Uji", plan: "trial", must_change_password: false, horizon_max: 3 });

type Call = { path: string; method: string; body: unknown; query: string };
function server(role: string, handler: (c: Call) => Response | null) {
  localStorage.setItem("icp.sessionToken", "icp_sess_test");
  const calls: Call[] = [];
  vi.stubGlobal("fetch", async (url: string, init: RequestInit = {}) => {
    const [path = "", query = ""] = url.split("?");
    const c = { path, method: init.method ?? "GET", body: init.body ? JSON.parse(String(init.body)) : null, query };
    calls.push(c);
    if (path === "/api/v1/auth/whoami") return json(who(role));
    return handler(c) ?? err(404, "NOT_FOUND");
  });
  return calls;
}
const mount = (path: string) => render(
  <MemoryRouter initialEntries={[path]}><I18nProvider initial="id"><ToastProvider><AuthProvider><App /></AuthProvider></ToastProvider></I18nProvider></MemoryRouter>,
);
const accountBase = (c: Call) => c.path === "/api/v1/auth/me" ? json({ ...me, user: { ...me.user, role: who("owner").role } }) : c.path === "/api/v1/org" ? json(org)
  : c.path === "/api/v1/org/members" ? json([me.user]) : null;

afterEach(() => { cleanup(); vi.unstubAllGlobals(); localStorage.clear(); sessionStorage.clear(); });

describe("account: plans and plan-change requests", () => {
  it("lists every entitlement on the plan cards, in the active language, with an honest API quota line", async () => {
    server("owner", (c) => accountBase(c) ?? (c.path === "/api/v1/org/plan-requests" ? json({ items: [] }) : null));
    mount("/akun");
    const heading = await screen.findByText("Isi setiap paket");
    const grid = heading.nextElementSibling as HTMLElement;
    const cards = within(grid).getAllByRole("heading", { level: 3 });
    expect(cards.map((h) => h.textContent)).toEqual(["Uji coba Paket saat ini", "Profesional"]);
    const proCard = cards[1]!.closest(".plan") as HTMLElement;
    for (const text of ["10 kursi", "Horizon hingga 6 bulan", "Riwayat penuh", "50 skenario tersimpan", "Insight pasar otomatis", "Laporan PDF", "Laporan Excel", "Laporan CSV", "50 aturan pemberitahuan"]) {
      expect(proCard).toHaveTextContent(text);
    }
    expect(proCard).toHaveTextContent(/kuota 20\.000 permintaan\/bulan; di atas kuota, kunci API ditolak sampai awal bulan berikutnya/);
    expect(cards[0]!.closest(".plan")).toHaveTextContent("Riwayat 3 tahun");
    // no price, checkout or invented contact
    expect(document.body.textContent).not.toMatch(/checkout|bayar sekarang|@icp|mailto/i);
  });

  it("lets the owner request a plan (no payment), shows the pending status and withdraws it via a dialog", async () => {
    let items: unknown[] = [];
    const calls = server("owner", (c) => {
      if (c.path === "/api/v1/org/plan-requests" && c.method === "GET") return json({ items });
      if (c.path === "/api/v1/org/plan-requests" && c.method === "POST") {
        items = [{ id: "pr_1", org_id: "org_1", org_name: "PT Uji", requested_by: "user:u1", requested_by_name: "Penguji UX", previous_plan: "trial",
          requested_plan: (c.body as { requested_plan: string }).requested_plan, note: (c.body as { note: string }).note, status: "pending", created_at: "2026-10-19T10:00:00Z",
          resolved_at: null, resolved_by: null, resolution_note: null }];
        return json(items[0], 201);
      }
      if (c.path === "/api/v1/org/plan-requests/pr_1/withdraw") { items = [{ ...(items[0] as object), status: "withdrawn", resolved_at: "2026-10-19T11:00:00Z" }]; return json(items[0]); }
      return accountBase(c);
    });
    mount("/akun");
    fireEvent.change(await screen.findByLabelText(/Catatan untuk operator/), { target: { value: "Butuh API" } });
    fireEvent.click(screen.getByRole("button", { name: "Kirim pengajuan" }));
    await waitFor(() => expect(calls.some((c) => c.method === "POST" && c.path === "/api/v1/org/plan-requests")).toBe(true));
    expect(calls.find((c) => c.method === "POST" && c.path === "/api/v1/org/plan-requests")!.body).toEqual({ requested_plan: "professional", note: "Butuh API" });
    expect(await screen.findByText(/paket Profesional, menunggu operator/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Kirim pengajuan" })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Tarik pengajuan" }));
    const dialog = await screen.findByRole("alertdialog", { name: "Tarik pengajuan perubahan paket?" });
    fireEvent.click(within(dialog).getByRole("button", { name: "Ya, tarik" }));
    expect(await screen.findByText(/Ditarik .*paket Profesional/)).toBeInTheDocument();
    expect(calls.some((c) => c.path === "/api/v1/org/plan-requests/pr_1/withdraw" && c.method === "POST")).toBe(true);
  });

  it("explains an already pending request on 409 instead of claiming success", async () => {
    server("owner", (c) => c.path === "/api/v1/org/plan-requests" ? (c.method === "POST" ? err(409, "CONFLICT") : json({ items: [] })) : accountBase(c));
    mount("/akun");
    fireEvent.click(await screen.findByRole("button", { name: "Kirim pengajuan" }));
    expect(await screen.findByText(/Sudah ada pengajuan yang menunggu operator/)).toBeInTheDocument();
    expect(screen.queryByText(/terkirim ke operator/)).toBeNull();
  });

  it("shows analysts the status read-only, without a request form", async () => {
    server("analyst", (c) => accountBase(c) ?? (c.path === "/api/v1/org/plan-requests" ? json({ items: [] }) : null));
    mount("/akun");
    expect(await screen.findByText("Hanya pemilik atau admin organisasi yang dapat mengajukan perubahan paket.")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Kirim pengajuan" })).toBeNull();
  });

  it("falls back to the explanation when the server has no plan requests (404): no button, no fake success", async () => {
    server("owner", accountBase);
    mount("/akun");
    expect(await screen.findByText("Cara mengganti paket")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Kirim pengajuan" })).toBeNull();
  });
});

describe("notifications", () => {
  const event = { id: "ev_1", org_id: "org_1", rule_id: "r1", severity: "info", title: "ICP September 2026 terdeteksi", body: "ICP resmi Sep 2026: USD 112,81.", title_en: "ICP September 2026 detected",
    body_en: "Official ICP Sep 2026: USD 112.81.", created_at: "2026-10-19T10:00:00Z", read_at: null, data: null, dedupe_key: "k", period: "2026-09", link: { kind: "period", period: "2026-09" } };
  it("links each event to its context and refreshes the sidebar badge right after marking it read", async () => {
    let read = false;
    const calls = server("owner", (c) => {
      if (c.path === "/api/v1/org") return json(org);
      if (c.path === "/api/v1/alerts/kinds") return json([{ kind: "icp_published", label_id: "ICP baru terbit", label_en: "New ICP published", params: {} }]);
      if (c.path === "/api/v1/alerts/rules") return json([]);
      if (c.path === "/api/v1/alerts/events") return json({ items: [{ ...event, read_at: read ? "2026-10-19T11:00:00Z" : null }], next_cursor: null, unread: read ? 0 : 1 });
      if (c.path === "/api/v1/alerts/events/read") { read = true; return new Response(null, { status: 204 }); }
      return null;
    });
    mount("/pemberitahuan");
    const link = await screen.findByRole("link", { name: /Lihat di Riwayat harga Sep 2026/ });
    expect(link).toHaveAttribute("href", "/harga?bulan=2026-09");
    const badgeCalls = () => calls.filter((c) => c.path === "/api/v1/alerts/events" && c.query.includes("unread_only")).length;
    await waitFor(() => expect(badgeCalls()).toBeGreaterThan(0));
    const before = badgeCalls();
    fireEvent.click(screen.getByRole("button", { name: "Tandai dibaca" }));
    await waitFor(() => expect(badgeCalls()).toBeGreaterThan(before));
    expect(screen.getByRole("button", { name: /Periksa aturan sekarang/ })).toBeInTheDocument();
  });

  it("disables adding a rule at the plan limit and says why", async () => {
    server("owner", (c) => c.path === "/api/v1/org" ? json({ ...org, alert_rules: 2 }) : c.path === "/api/v1/alerts/kinds" ? json([{ kind: "icp_published", label_id: "ICP baru terbit", params: {} }])
      : c.path === "/api/v1/alerts/rules" ? json([]) : c.path === "/api/v1/alerts/events" ? json({ items: [], next_cursor: null, unread: 0 }) : null);
    mount("/pemberitahuan");
    expect(await screen.findByText(/Batas 2 aturan pada paket Anda sudah tercapai/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Tambah aturan" })).toBeDisabled();
  });
});

describe("reports", () => {
  const rep = (id: string, scen: string[]) => ({ id, kind: "scenario_comparison", label: "x", created_at: "2026-10-19T10:00:00Z", created_by: "user:u1", org_id: "org_1", formats: ["pdf"],
    status: "succeeded", finished_at: null, error_message: null, params: { scenario_ids: scen }, summary: { scenarios: scen } });
  it("tells two reports of the same kind apart by their parameters and describes contents before generating", async () => {
    server("owner", (c) => c.path === "/api/v1/org" ? json(org) : c.path === "/api/v1/reports" ? json({ items: [rep("r1", ["s1", "s2"]), rep("r2", ["s3"])], next_cursor: null })
      : c.path === "/api/v1/scenarios" ? json({ items: [{ id: "s1", name: "Brent 80" }, { id: "s2", name: "Brent 120" }, { id: "s3", name: "Rupiah lemah" }], next_cursor: null })
      : c.path === "/api/v1/datasets/active" ? json({ datasets: { icp: { active_version: { period_start: "2019-01", period_end: "2026-09", created_at: "2026-10-13T00:00:00Z" } } } }) : null);
    mount("/laporan");
    expect(await screen.findByText("Brent 80 vs Brent 120")).toBeInTheDocument();
    expect(screen.getByText("Rupiah lemah")).toBeInTheDocument();
    const preview = screen.getByRole("region", { name: "Isi laporan" });
    expect(preview).toHaveTextContent("Data hingga ICP resmi September 2026");
    expect(preview).toHaveTextContent("Format yang dibuat: PDF, CSV.");
    expect(screen.getByText("tidak termasuk paket Anda").closest(".badge")).not.toHaveClass("warn");
  });

  it("preselects the forecast kind and issue from a deep link", async () => {
    server("owner", (c) => c.path === "/api/v1/org" ? json(org) : c.path === "/api/v1/reports" ? json({ items: [], next_cursor: null })
      : c.path === "/api/v1/forecasts" ? json({ items: [{ id: "fc_9", status: "succeeded", issue_date: "2026-10-16", created_at: "2026-10-16T00:00:00Z", forecast_origin: "2026-09", last_observed_period: "2026-09", horizon: 3 }], next_cursor: null }) : null);
    mount("/laporan?jenis=proyeksi&proyeksi=fc_9");
    expect(await screen.findByRole("radio", { name: /Laporan proyeksi ICP/ })).toBeChecked();
    await waitFor(() => expect((screen.getByLabelText(/Proyeksi yang dilaporkan/) as HTMLSelectElement).value).toBe("fc_9"));
    expect(screen.getByRole("region", { name: "Isi laporan" })).toHaveTextContent(/titik awal ICP resmi Sep 2026/);
  });
});
