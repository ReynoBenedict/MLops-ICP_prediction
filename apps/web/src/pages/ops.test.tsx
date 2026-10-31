/** Operator console v5: readable labels, separate plan/status actions, plan-change request queue. */
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import App from "../App";
import { AuthProvider } from "../auth";
import { ToastProvider } from "../components/toast";
import { I18nProvider } from "../i18n";

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}
const err = (status: number, code: string) => json({ error: { code, message: code, request_id: "req_1", details: {} } }, status);
const operator = { authenticated: true, name: "Operator", role: "operator", kind: "operator", org_id: null, org_name: null, plan: null, must_change_password: false, horizon_max: 6 };
type Call = { path: string; method: string; body: unknown };

function server(handler: (c: Call) => Response | null) {
  sessionStorage.setItem("icp.operatorToken", "op_test");
  const calls: Call[] = [];
  vi.stubGlobal("fetch", async (url: string, init: RequestInit = {}) => {
    const path = url.split("?")[0] ?? "";
    const c = { path, method: init.method ?? "GET", body: init.body ? JSON.parse(String(init.body)) : null };
    calls.push(c);
    if (path === "/api/v1/auth/whoami") return json(operator);
    return handler(c) ?? err(404, "NOT_FOUND");
  });
  return calls;
}
const mount = (path: string) => render(
  <MemoryRouter initialEntries={[path]}><I18nProvider initial="id"><ToastProvider><AuthProvider><App /></AuthProvider></ToastProvider></I18nProvider></MemoryRouter>,
);
afterEach(() => { cleanup(); vi.unstubAllGlobals(); localStorage.clear(); sessionStorage.clear(); });

// The operator pages are lazy chunks that also register their strings on load. Under a loaded
// machine (parallel test files) the first dynamic import plus transform used to exceed the
// default 1 s `findBy` window. Load them once up front, so each test measures the page, not the
// module loader; React.lazy then resolves from the module cache within a microtask.
beforeAll(async () => {
  await Promise.all([import("./ops/TenantsPage"), import("./ops/DataOpsPage"), import("../i18n/ops")]);
}, 60_000);
/**
 * Role queries walk the whole accessibility tree of the (large) operator console on every retry;
 * under parallel load one walk can take several hundred ms, so the first query after mount gets a
 * wider, explicit window. Assertions are unchanged.
 */
const FIRST_RENDER = { timeout: 5_000 };

const orgs = { items: [{ id: "org_1", name: "PT Uji", slug: "pt-uji", plan: "trial", status: "active", created_at: "2026-10-01T00:00:00Z" }], next_cursor: null };
const request = { id: "pr_1", org_id: "org_1", org_name: "PT Uji", requested_by: "user:u1", requested_by_name: "Penguji UX", previous_plan: "trial", requested_plan: "professional",
  note: "Butuh API", status: "pending", created_at: "2026-10-19T10:00:00Z", resolved_at: null, resolved_by: null, resolution_note: null };

describe("operator tenants", () => {
  it("lists pending plan requests and approves one through a confirmation dialog", async () => {
    let pending = [request];
    const calls = server((c) => {
      if (c.path === "/api/v1/admin/organizations") return json(orgs);
      if (c.path === "/api/v1/admin/plan-requests") return json({ items: pending, next_cursor: null });
      if (c.path === "/api/v1/admin/plan-requests/pr_1/resolve") { pending = []; return json({ ...request, status: "approved" }); }
      return null;
    });
    mount("/operasi/organisasi");
    const panel = (await screen.findByRole("heading", { name: "Permintaan perubahan paket" }, FIRST_RENDER)).closest("section") as HTMLElement;
    expect(await within(panel).findByText("Butuh API")).toBeInTheDocument();
    expect(panel).toHaveTextContent("Uji coba → Profesional");
    fireEvent.click(within(panel).getByRole("button", { name: "Setujui" }));
    const dialog = await screen.findByRole("alertdialog", { name: "Setujui paket Profesional untuk PT Uji?" });
    fireEvent.change(within(dialog).getByLabelText(/Catatan untuk pelanggan/), { target: { value: "Sesuai kontrak" } });
    fireEvent.click(within(dialog).getByRole("button", { name: "Setujui" }));
    await waitFor(() => expect(calls.find((c) => c.path === "/api/v1/admin/plan-requests/pr_1/resolve")?.body).toEqual({ decision: "approve", note: "Sesuai kontrak" }));
    expect(await screen.findByText("Tidak ada permintaan yang menunggu.")).toBeInTheDocument();
  });

  it("shows readable plan labels and keeps plan and status as separate actions, each needing a reason", async () => {
    const calls = server((c) => c.path === "/api/v1/admin/organizations" ? json(orgs) : c.path === "/api/v1/admin/plan-requests" ? err(404, "NOT_FOUND")
      : c.path === "/api/v1/admin/organizations/org_1/status" ? json({ ...orgs.items[0], status: "suspended", plan: "trial", active_members: 1 }) : null);
    mount("/operasi/organisasi");
    expect(await screen.findByText("Server ini belum menyediakan permintaan perubahan paket.", {}, FIRST_RENDER)).toBeInTheDocument();
    const row = (await screen.findByText("PT Uji")).closest("tr") as HTMLElement;
    expect(within(row).getByText("Uji coba")).toBeInTheDocument();
    expect(row).not.toHaveTextContent(/\btrial\b/);
    expect(within(row).getByRole("button", { name: "Ubah paket: PT Uji" })).toBeInTheDocument();
    fireEvent.click(within(row).getByRole("button", { name: "Ubah status: PT Uji" }));
    const dialog = await screen.findByRole("alertdialog", { name: "Ubah status PT Uji" });
    fireEvent.click(within(dialog).getByRole("button", { name: "Ubah status" }));
    expect(await within(dialog).findByText(/minimal 5 karakter/)).toBeInTheDocument();
    expect(calls.some((c) => c.path === "/api/v1/admin/organizations/org_1/status")).toBe(false);
    fireEvent.change(within(dialog).getByLabelText(/Alasan/), { target: { value: "Penyalahgunaan API" } });
    fireEvent.click(within(dialog).getByRole("button", { name: "Ubah status" }));
    await waitFor(() => expect(calls.find((c) => c.path === "/api/v1/admin/organizations/org_1/status")?.body).toEqual({ status: "suspended", reason: "Penyalahgunaan API" }));
    expect(calls.some((c) => c.path.endsWith("/plan"))).toBe(false);
  });
});

describe("operator data console", () => {
  it("names datasets instead of codes and shows 'Lolos · 1 peringatan' for passed checks with a warning", async () => {
    const report = { ok: true, errors: [], warnings: [{ code: "LARGE_MOM_CHANGE", message: "2 month-over-month change(s) larger than 40%", periods: ["2020-04"] }], missing_periods: [], stats: { latest: 112.81, min: 20, max: 120, mean: 70, std: 15 } };
    server((c) => c.path === "/api/v1/data/quality" ? json({ dataset: "icp", report }) : c.path === "/api/v1/datasets/active" ? json({ datasets: { icp: { dataset: "icp", label_id: "ICP", provider: "ESDM", definition: "d", freshness: { status: "fresh" },
        active_version: { id: "dsv_1", dataset: "icp", version: 3, period_start: "2024-01", period_end: "2026-09", row_count: 33, created_at: "2026-10-13T00:00:00Z", source: "connector", sha256: "abc", size_bytes: 100, created_by: "op", parent_version_id: null } } } })
      : c.path === "/api/v1/data/history" ? json({ points: [] })
      : c.path === "/api/v1/datasets/versions" ? json({ items: [], next_cursor: null }) : c.path === "/api/v1/datasets/ingestions" ? json({ items: [], next_cursor: null })
      : c.path === "/api/v1/data/evidence" ? json({ dataset: "icp", items: [] }) : null);
    mount("/operasi/data");
    const group = await screen.findByRole("group", { name: "Dataset" }, FIRST_RENDER);
    for (const name of ["ICP", "Kurs JISDOR", "JISDOR akhir bulan"]) expect(within(group).getByRole("button", { name })).toBeInTheDocument();
    expect(within(group).queryByRole("button", { name: "usd_idr_eom" })).toBeNull();
    expect((await screen.findAllByText("Lolos · 1 peringatan")).length).toBeGreaterThan(0);
    expect(screen.getByText("Perubahan bulanan besar")).toBeInTheDocument();
    // the English server message is not shown in the Indonesian console
    expect(screen.queryByText(/month-over-month change/)).toBeNull();
    expect(screen.getByText("Sebaran (std)")).toBeInTheDocument();
  });
});
