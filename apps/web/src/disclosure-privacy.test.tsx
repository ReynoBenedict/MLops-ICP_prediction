/**
 * Mission v6 regressions: shared GETs (one /system/status per page load), sign-in throttle and API
 * quota messages, the evaluation disclosure next to forecast accuracy, and the account "Data &
 * privacy" panel (owner export, deletion request through a confirmation dialog).
 */
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import { ApiError, sharedGet } from "./api/client";
import type { ForecastPoint } from "./api/types";
import { AuthProvider } from "./auth";
import { PointAccuracy } from "./components/forecast";
import { ToastProvider } from "./components/toast";
import { errorText } from "./components/ui";
import { I18nProvider, translate } from "./i18n";

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}
afterEach(() => { cleanup(); vi.unstubAllGlobals(); localStorage.clear(); sessionStorage.clear(); });

describe("sharedGet", () => {
  it("shares one request between concurrent callers and refetches on demand", async () => {
    const fetchMock = vi.fn(async () => json({ status: "ready" }));
    vi.stubGlobal("fetch", fetchMock);
    const [a, b] = await Promise.all([sharedGet("/system/status"), sharedGet("/system/status")]);
    expect(a).toEqual(b);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await sharedGet("/system/status", { fresh: true });
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("does not let one caller's abort cancel the shared request", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => json({ ok: 1 })));
    const c = new AbortController();
    const first = sharedGet("/x", { signal: c.signal });
    const second = sharedGet("/x");
    c.abort();
    await expect(first).rejects.toMatchObject({ code: "ABORTED" });
    await expect(second).resolves.toEqual({ ok: 1 });
  });
});

describe("error texts", () => {
  const t = (k: Parameters<typeof translate>[1], v?: Record<string, string | number | null | undefined>) => translate("id", k, v);
  it("explains the sign-in throttle by scope (this device vs this account)", () => {
    const client = errorText(new ApiError(429, "RATE_LIMITED", "x", null, { retry_after_seconds: 120, scope: "client" }), t, "id");
    expect(client.body).toMatch(/dari perangkat ini.*120 detik/);
    const account = errorText(new ApiError(429, "RATE_LIMITED", "x", null, { retry_after_seconds: 60, scope: "account" }), t, "id");
    expect(account.body).toMatch(/untuk akun ini/);
  });
  it("explains an exhausted API quota with its reset date", () => {
    const q = errorText(new ApiError(429, "QUOTA_EXCEEDED", "x", null, { reset_at: "2026-11-01T00:00:00Z", limit: 10, used: 10 }), t, "id");
    expect(q.title).toBe("Kuota API bulanan habis");
    expect(q.body).toContain("2026-11-01");
  });
});

describe("accuracy disclosure", () => {
  const point = (evaluation: unknown) => ({ period: "2026-10", step: 1, kind: "future_forecast", evaluation } as unknown as ForecastPoint);
  it("renders the server sentence, the baseline verdict, coverage with its own n and period, and the warning", () => {
    render(<I18nProvider initial="id"><PointAccuracy level="0.8" prod={null} points={[point({
      mode: "hold_last", horizon: 1, n: 69, mae: 6.1, mase: 1.1, dir_acc: 0.5, coverage: { "0.8": 0.857 }, coverage_n: 28,
      origin_start: "2020-12", origin_end: "2026-08", baseline: { method: "naive_last", label: { id: "Ulang ICP bulan lalu", en: "Repeat" }, mae: 6.0, n: 69 },
      beats_baseline: false, coverage_period: { origin_start: "2024-05", origin_end: "2026-08" }, under_coverage: ["0.95"],
      coverage_warning: { id: "Rentang 95% hanya mencakup 82% nilai resmi.", en: "x" },
      summary: { id: "Uji mundur 69 kasus (titik awal 2020-12 s.d. 2026-08): selisih USD 6,11 dibanding USD 6,06.", en: "x" },
    })]} /></I18nProvider>);
    expect(screen.getByText(/Uji mundur 69 kasus/)).toBeInTheDocument();
    expect(screen.getByText("Tidak lebih kecil dari pembanding")).toBeInTheDocument();
    expect(screen.getByText(/28 uji, Mei 2024/)).toBeInTheDocument();
    expect(screen.getByRole("note")).toHaveTextContent("Rentang 95% hanya mencakup 82% nilai resmi.");
  });
  it("falls back to the stored metrics when the server sends no disclosure", () => {
    render(<I18nProvider initial="id"><PointAccuracy level="0.8" prod={null} points={[point({ mode: "observed", horizon: 1, n: 12, mae: 1.5, mase: null, dir_acc: null, coverage: null, coverage_n: null })]} /></I18nProvider>);
    expect(screen.getByText(/rata-rata meleset/)).toBeInTheDocument();
    expect(screen.queryByRole("note")).toBeNull();
  });
});

describe("account: data & privacy", () => {
  const trial = { code: "trial", label_id: "Uji coba", label_en: "Trial", seats: 2, forecast_horizon_max: 3, scenarios: true, saved_scenarios_max: 2, reports_pdf: true, reports_excel: false,
    alerts: true, alert_rules_max: 2, api_access: false, api_requests_per_month: 0, history_years: 3, insights: true };
  const org = { id: "org_1", name: "PT Uji", slug: "pt-uji", plan: "trial", status: "active", created_at: "2026-10-01T00:00:00Z", entitlements: trial, plans: [trial],
    members: 1, active_members: 1, saved_scenarios: 0, alert_rules: 0, usage_this_month: {} };
  const user = { id: "u1", name: "Penguji", email: "ux@test.id", role: "owner", status: "active", created_at: "2026-10-01T00:00:00Z", last_login_at: null, must_change_password: false };

  it("lets the owner request organisation deletion only through a confirmation dialog, then shows it as pending", async () => {
    localStorage.setItem("icp.sessionToken", "icp_sess_test");
    let items: unknown[] = [];
    const posts: unknown[] = [];
    vi.stubGlobal("fetch", async (url: string, init: RequestInit = {}) => {
      const path = url.split("?")[0];
      if (path === "/api/v1/auth/whoami") return json({ authenticated: true, name: "Penguji", role: "owner", kind: "user", org_id: "org_1", org_name: "PT Uji", plan: "trial", must_change_password: false, horizon_max: 3 });
      if (path === "/api/v1/auth/me") return json({ user, organization: org, sessions: [] });
      if (path === "/api/v1/org") return json(org);
      if (path === "/api/v1/org/plan-requests") return json({ items: [] });
      if (path === "/api/v1/org/deletion-requests" && (init.method ?? "GET") === "POST") {
        posts.push(JSON.parse(String(init.body)));
        items = [{ id: "dr_1", scope: "organization", user_id: null, user_name: null, requested_by: "u1", requested_by_name: "Penguji", reason: "Tutup", status: "pending",
          created_at: "2026-10-20T08:00:00Z", resolved_at: null, resolution_note: null }];
        return json(items[0], 201);
      }
      if (path === "/api/v1/org/deletion-requests") return json({ items, next_cursor: null });
      return json({ error: { code: "NOT_FOUND", message: "nf", request_id: "r", details: {} } }, 404);
    });
    render(<MemoryRouter initialEntries={["/akun"]}><I18nProvider initial="id"><ToastProvider><AuthProvider><App /></AuthProvider></ToastProvider></I18nProvider></MemoryRouter>);
    expect(await screen.findByRole("button", { name: /Unduh ekspor \(ZIP\)/ })).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText(/^Alasan/), { target: { value: "Tutup" } });
    fireEvent.click(screen.getByRole("button", { name: "Ajukan penghapusan organisasi" }));
    const dialog = await screen.findByRole("alertdialog");
    expect(posts).toEqual([]);
    fireEvent.click(within(dialog).getByRole("button", { name: "Kirim pengajuan" }));
    await waitFor(() => expect(posts).toEqual([{ scope: "organization", reason: "Tutup" }]));
    expect(await screen.findByText(/menunggu operator/)).toBeInTheDocument();
  });
});
