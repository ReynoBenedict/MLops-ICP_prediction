/**
 * Workspace flows (wave B, W2): scenario results labelled stale after an edit or an archive (B-P1-1,
 * including a comparison restored from the tab cache), report outcomes (a failed report offers no
 * downloads; a client timeout says the report may still finish), the insight sheet's evidence
 * sections, and a valid definition list on the account page.
 */
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import App from "../App";
import { AuthProvider } from "../auth";
import { ToastProvider } from "../components/toast";
import { I18nProvider } from "../i18n";
import { compareStaleness, inputsFingerprint, runStaleness, scenarioFingerprint } from "../lib/scenario-stale";
import { ApiError } from "../api/client";
import { whyKey } from "../components/insight";
import { translate } from "../i18n";
import { IDEMPOTENCY_KEY_PATTERN, nextAttempt, reportBody, unknownOutcome } from "../lib/report-request";
import { sessionDevice } from "./AccountPage";

// The pages under test are lazy chunks of App. Load (and transform) them once before the tests, so a
// first render under parallel workers does not spend a findBy timeout on module loading (W2-R12, UX-25).
beforeAll(async () => {
  await Promise.all([import("./ScenariosPage"), import("./ReportsPage"), import("./InsightsPage"), import("./NotificationsPage"), import("./AccountPage")]);
}, 60_000);

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}
const err = (status: number, code: string) => json({ error: { code, message: code, request_id: "req_1", details: {} } }, status);
const who = (role = "owner") => ({ authenticated: true, name: "Penguji UX", role, kind: "user", org_id: "org_1", org_name: "PT Uji", plan: "trial", must_change_password: false, horizon_max: 3 });
const trial = { code: "trial", label_id: "Uji coba", label_en: "Trial", price_note_id: "Gratis", price_note_en: "Free", seats: 2, forecast_horizon_max: 3, scenarios: true,
  saved_scenarios_max: 5, reports_pdf: true, reports_excel: false, alerts: true, alert_rules_max: 2, api_access: false, api_requests_per_month: 0, history_years: 3, insights: true };
const org = { id: "org_1", name: "PT Uji", slug: "pt-uji", plan: "trial", status: "active", created_at: "2026-10-01T00:00:00Z", entitlements: trial, plans: [trial],
  members: 1, active_members: 1, saved_scenarios: 2, alert_rules: 0, usage_this_month: {} };

type Call = { path: string; method: string; body: unknown; query: URLSearchParams; key: string | null };
type Handler = (c: Call, init: RequestInit) => Response | Promise<Response> | null;
function server(handler: Handler, role = "owner") {
  localStorage.setItem("icp.sessionToken", "icp_sess_test");
  const calls: Call[] = [];
  vi.stubGlobal("fetch", async (url: string, init: RequestInit = {}) => {
    const path = url.split("?")[0] ?? "";
    const c = { path, method: init.method ?? "GET", body: init.body ? JSON.parse(String(init.body)) : null,
      query: new URLSearchParams(url.split("?")[1] ?? ""), key: new Headers(init.headers).get("Idempotency-Key") };
    calls.push(c);
    if (path === "/api/v1/auth/whoami") return json(who(role));
    return (await handler(c, init)) ?? err(404, "NOT_FOUND");
  });
  return calls;
}
const mount = (path: string) => render(
  <MemoryRouter initialEntries={[path]}><I18nProvider initial="id"><ToastProvider><AuthProvider><App /></AuthProvider></ToastProvider></I18nProvider></MemoryRouter>,
);
afterEach(() => { cleanup(); vi.unstubAllGlobals(); vi.useRealTimers(); localStorage.clear(); sessionStorage.clear(); });

/* ---------- pure rules ------------------------------------------------------------------------------ */
describe("scenario staleness rules", () => {
  it("compares assumptions, not timestamps: key order and empty inputs do not matter", () => {
    expect(inputsFingerprint({ wti: {}, brent: { "2026-11": 70, "2026-10": 70.0 } })).toBe(inputsFingerprint({ brent: { "2026-10": 70, "2026-11": 70 } }));
    expect(scenarioFingerprint(3, { brent: { "2026-10": 70 } })).not.toBe(scenarioFingerprint(2, { brent: { "2026-10": 70 } }));
  });
  it("labels a run changed, archived or current, and claims nothing while the list loads", () => {
    const run = { horizon: 3, scenario: { brent: { "2026-10": 70 } } };
    expect(runStaleness(run, undefined)).toEqual({ stale: false });
    expect(runStaleness(run, { horizon: 3, inputs: { brent: { "2026-10": 70 } } })).toEqual({ stale: false });
    expect(runStaleness(run, { horizon: 3, inputs: { brent: { "2026-10": 72 } } })).toEqual({ stale: true, reason: "changed" });
    expect(runStaleness(run, { horizon: 2, inputs: { brent: { "2026-10": 70 } } })).toEqual({ stale: true, reason: "changed" });
    expect(runStaleness(run, null)).toEqual({ stale: true, reason: "archived" });
  });
  it("labels a comparison stale per scenario, and only when the comparison horizon really changes", () => {
    const cmp = { horizon: 3, scenarios: [{ id: "a", name: "A", inputs: { brent: { "2026-10": 70 } } }, { id: "b", name: "B", inputs: { brent: { "2026-10": 120 } } }] };
    const a = { id: "a", horizon: 3, inputs: { brent: { "2026-10": 70 } } };
    const b = { id: "b", horizon: 3, inputs: { brent: { "2026-10": 120 } } };
    expect(compareStaleness(cmp, [a, b]).stale).toBe(false);
    expect(compareStaleness(cmp, [a, { ...b, horizon: 2 }]).stale).toBe(false); // the longest is still 3
    expect(compareStaleness(cmp, [{ ...a, horizon: 2 }, { ...b, horizon: 2 }]).horizonChanged).toBe(true);
    const edited = compareStaleness(cmp, [a, { ...b, inputs: { brent: { "2026-10": 125 } } }]);
    expect(edited.changed.map((s) => s.id)).toEqual(["b"]);
    const archived = compareStaleness(cmp, [a]);
    expect(archived.archived.map((s) => s.id)).toEqual(["b"]);
    expect(archived.activeIds).toEqual(["a"]);
    expect(compareStaleness(cmp, undefined).stale).toBe(false);
  });
  it("claims nothing about a scenario missing from a partial list, and flags a comparison older than the latest official month", () => {
    const cmp = { horizon: 3, forecast_origin: "2026-08", scenarios: [{ id: "a", name: "A", inputs: { brent: { "2026-10": 70 } } }, { id: "b", name: "B", inputs: {} }] };
    const a = { id: "a", horizon: 3, inputs: { brent: { "2026-10": 70 } } };
    const partial = compareStaleness(cmp, [a], { complete: false });
    expect(partial).toMatchObject({ stale: false, archived: [], horizonChanged: false, activeIds: ["a", "b"] });
    expect(partial.unknown.map((s) => s.id)).toEqual(["b"]);
    expect(compareStaleness(cmp, [a], { complete: true }).archived.map((s) => s.id)).toEqual(["b"]);
    const b = { id: "b", horizon: 3, inputs: {} };
    expect(compareStaleness(cmp, [a, b], { latestOrigin: "2026-08" }).stale).toBe(false);
    expect(compareStaleness(cmp, [a, b], { latestOrigin: "2026-09" })).toMatchObject({ stale: true, newerOrigin: "2026-09", changed: [], archived: [] });
  });
});

describe("report requests: idempotency and unknown outcomes", () => {
  it("reuses the key for the same body only, in canonical format order, with a key the server accepts", () => {
    const body = reportBody("market_monthly", {}, ["csv", "pdf"]);
    expect(body.formats).toEqual(["pdf", "csv"]);
    const first = nextAttempt(null, body);
    expect(first.key).toMatch(IDEMPOTENCY_KEY_PATTERN);
    expect(first.key.startsWith("rep_")).toBe(true);
    expect(nextAttempt(first, reportBody("market_monthly", {}, ["pdf", "csv"]))).toBe(first);
    const changed = nextAttempt(first, reportBody("market_monthly", {}, ["pdf"]));
    expect(changed.key).not.toBe(first.key);
    expect(nextAttempt(null, body).key).not.toBe(first.key);
  });
  it("classifies only answer-less endings as unknown", () => {
    expect(unknownOutcome(new ApiError(0, "TIMEOUT", "t", null, { timeout_seconds: 95 }))).toBe("client_timeout");
    expect(unknownOutcome(new ApiError(504, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE", "t", "r"))).toBe("gateway_timeout");
    expect(unknownOutcome(new ApiError(409, "IDEMPOTENCY_IN_PROGRESS", "t", "r", { retry_after_seconds: 15 }))).toBe("in_progress");
    expect(unknownOutcome(new ApiError(409, "IDEMPOTENCY_KEY_REUSED", "t", "r"))).toBeNull();
    expect(unknownOutcome(new ApiError(502, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE", "t", "r"))).toBe("connection");
    expect(unknownOutcome(new ApiError(0, "NETWORK_ERROR", "t", null))).toBe("connection");
    expect(unknownOutcome(new ApiError(0, "ABORTED", "t", null))).toBeNull();
    expect(unknownOutcome(new ApiError(500, "INTERNAL_ERROR", "t", "r"))).toBeNull();
    expect(unknownOutcome(new ApiError(429, "RATE_LIMITED", "t", "r"))).toBeNull();
    expect(unknownOutcome(new ApiError(403, "FEATURE_NOT_IN_PLAN", "t", "r"))).toBeNull();
  });
});

describe("small W2 rules", () => {
  it("explains importance with the rule's own reason, else the level's general text", () => {
    expect(whyKey("spread_vs_history", "medium")).toBe("ins.whyRule.spread_vs_history.medium");
    expect(whyKey("spread_vs_history", "low")).toBe("ins.whyRule.spread_vs_history.low");
    expect(whyKey("data_freshness", "high")).toBe("ins.whyRule.data_freshness.high");
    expect(whyKey("icp_mom", "high")).toBe("ins.why.high");
    expect(whyKey("some_future_rule", "medium")).toBe("ins.why.medium");
    expect(whyKey(null, "low")).toBe("ins.why.low");
  });
  it("words the server's session device labels in the reader's language", () => {
    const en: Parameters<typeof sessionDevice>[1] = (key, vars) => translate("en", key, vars);
    expect(sessionDevice("Chrome di Windows", en)).toBe("Chrome on Windows");
    expect(sessionDevice("Peramban di Linux", en)).toBe("Browser on Linux");
    expect(sessionDevice("Peramban", en)).toBe("Browser");
    expect(sessionDevice("Skrip atau klien API", en)).toBe("Automated access (script or app)");
    expect(sessionDevice("Perangkat tidak dikenal", en)).toBe("Unknown device");
    expect(sessionDevice("Something new", en)).toBe("Something new");
  });
});

/* ---------- scenarios page ----------------------------------------------------------------------- */
const scen = (id: string, name: string, brent: number, extra: Record<string, unknown> = {}) => ({
  id, org_id: "org_1", created_by: "u1", name, description: null, horizon: 3, inputs: { brent: { "2026-10": brent, "2026-11": brent, "2026-12": brent } },
  created_at: "2026-10-20T00:00:00Z", updated_at: "2026-10-20T00:00:00Z", last_forecast_id: null, archived: false, ...extra,
});
const failedRun = (scenario: unknown) => ({ id: "fc_1", status: "failed", horizon: 3, scenario, result: null, error_code: "X", error_message: null, created_at: "2026-10-25T00:00:00Z",
  issue_date: "2026-10-25", params: {}, requested_by: "u1", duration_ms: 1, export_available: false, icp_dataset_version_id: null, last_observed_period: "2026-09",
  model_name: null, model_run_id: null, model_version: null, request_id: null, wti_dataset_version_id: null });
const compareOf = (list: Array<ReturnType<typeof scen>>) => ({
  baseline_forecast_id: "fc_b", forecast_origin: "2026-09", horizon: 3, issue_date: "2026-10-25", method: "spread_brent_6m", note: "x",
  scenarios: list.map((s) => ({ id: s.id, name: s.name, forecast_id: `fc_${s.id}`, inputs: s.inputs })),
  rows: ["2026-10", "2026-11", "2026-12"].map((p) => ({ period: p, kind: "future_forecast", baseline: 112.9,
    scenarios: Object.fromEntries(list.map((s) => [s.id, { prediction: s.inputs.brent["2026-10"]! - 1.26, delta_vs_baseline: s.inputs.brent["2026-10"]! - 1.26 - 112.9, conditional_on_scenario: true }])) })),
});
function scenarioServer(state: { list: Array<ReturnType<typeof scen>> }) {
  return server((c) => {
    if (c.path === "/api/v1/org") return json(org);
    if (c.path === "/api/v1/scenarios" && c.method === "GET") return json({ items: state.list, next_cursor: null });
    if (c.path === "/api/v1/data/history") return json({ points: [{ period: "2026-08", icp_price: 89.43 }, { period: "2026-09", icp_price: 112.81 }] });
    if (c.path === "/api/v1/forecasts") return json({ items: [{ id: "fc_0", forecast_origin: "2026-09", status: "succeeded" }], next_cursor: null });
    if (c.path === "/api/v1/scenarios/compare") return json(compareOf(state.list.filter((s) => (c.body as { scenario_ids: string[] }).scenario_ids.includes(s.id))));
    const run = c.path.match(/^\/api\/v1\/scenarios\/([^/]+)\/run$/);
    if (run) return json(failedRun(state.list.find((s) => s.id === run[1])!.inputs), 201);
    const upd = c.path.match(/^\/api\/v1\/scenarios\/([^/]+)$/);
    if (upd && c.method === "POST") {
      state.list = state.list.map((s) => (s.id === upd[1] ? { ...s, ...(c.body as object), updated_at: "2026-10-25T01:00:00Z" } : s));
      return json(state.list.find((s) => s.id === upd[1]));
    }
    const arch = c.path.match(/^\/api\/v1\/scenarios\/([^/]+)\/archive$/);
    if (arch) { state.list = state.list.filter((s) => s.id !== arch[1]); return new Response(null, { status: 204 }); }
    return null;
  });
}

describe("scenarios: results are never silently stale (B-P1-1)", () => {
  it("labels a run stale after its scenario is edited, shows the assumptions it used, and clears the label after running again", async () => {
    const state = { list: [scen("s1", "Brent 80", 80)] };
    const calls = scenarioServer(state);
    mount("/skenario");
    // The first render of the whole app can take more than the default 1 s under parallel workers (W2-R12).
    fireEvent.click(await screen.findByRole("button", { name: "Jalankan: Brent 80" }, { timeout: 5000 }));
    const results = await screen.findByRole("region", { name: "Hasil skenario" });
    await within(results).findByRole("heading", { name: 'Hasil skenario "Brent 80"' });
    expect(within(results).getByText(/Brent 80,00 USD\/bbl/)).toBeInTheDocument();
    expect(screen.queryByText("Asumsi skenario diubah setelah hasil ini dibuat")).toBeNull();

    // Edit the scenario in the form: Brent October 80 -> 85.
    fireEvent.click(screen.getByRole("button", { name: "Ubah skenario: Brent 80" }));
    const oct = await screen.findByRole("textbox", { name: "Brent (USD/bbl) Okt 2026" });
    fireEvent.change(oct, { target: { value: "85" } });
    fireEvent.click(screen.getByRole("button", { name: "Simpan perubahan" }));
    const notice = (await screen.findByText("Asumsi skenario diubah setelah hasil ini dibuat")).closest(".callout") as HTMLElement;
    // The result still says which assumptions it used (the old ones).
    expect(within(results).getByText(/Brent 80,00 USD\/bbl/)).toBeInTheDocument();

    fireEvent.click(within(notice).getByRole("button", { name: "Jalankan ulang" }));
    await waitFor(() => expect(screen.queryByText("Asumsi skenario diubah setelah hasil ini dibuat")).toBeNull());
    expect(calls.filter((c) => c.path === "/api/v1/scenarios/s1/run")).toHaveLength(2);
  }, 30_000);

  it("labels a comparison stale when a compared scenario is archived and offers to compare the remaining ones", async () => {
    const state = { list: [scen("a", "Brent 70", 70), scen("b", "Brent 120", 120)] };
    scenarioServer(state);
    mount("/skenario");
    fireEvent.click(await screen.findByRole("checkbox", { name: 'Pilih "Brent 70" untuk dibandingkan' }, { timeout: 5000 }));
    fireEvent.click(screen.getByRole("checkbox", { name: 'Pilih "Brent 120" untuk dibandingkan' }));
    fireEvent.click(screen.getByRole("button", { name: /Bandingkan terpilih \(2\)/ }));
    await screen.findByRole("heading", { name: "Perbandingan terhadap proyeksi dasar" });
    expect(screen.queryByText("Perbandingan ini tidak lagi sesuai dengan skenario tersimpan")).toBeNull();
    expect(screen.getByRole("link", { name: /Buat laporan perbandingan/ })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Arsipkan: Brent 120" }));
    fireEvent.click(within(await screen.findByRole("alertdialog")).getByRole("button", { name: "Arsipkan" }));
    const notice = (await screen.findByText("Perbandingan ini tidak lagi sesuai dengan skenario tersimpan")).closest(".callout") as HTMLElement;
    expect(notice).toHaveTextContent('Sudah diarsipkan: "Brent 120".');
    // A report link would ask the server to compare an archived scenario: it is withdrawn while stale.
    expect(screen.queryByRole("link", { name: /Buat laporan perbandingan/ })).toBeNull();
    expect(within(notice).getByRole("button", { name: "Bandingkan ulang yang tersisa (1)" })).toBeInTheDocument();
  }, 30_000);

  it("labels a comparison restored from the tab cache stale when the scenario changed since, without comparing again", async () => {
    const old = [scen("a", "Brent 70", 70), scen("b", "Brent 130", 130)];
    sessionStorage.setItem("icp.compare.a,b", JSON.stringify(compareOf(old)));
    const state = { list: [old[0]!, scen("b", "Brent 130", 120)] };
    const calls = scenarioServer(state);
    mount("/skenario?bandingkan=a,b");
    const notice = (await screen.findByText("Perbandingan ini tidak lagi sesuai dengan skenario tersimpan", {}, { timeout: 5000 })).closest(".callout") as HTMLElement;
    expect(notice).toHaveTextContent('Diubah setelah perbandingan dibuat: "Brent 130".');
    expect(calls.some((c) => c.path === "/api/v1/scenarios/compare")).toBe(false);
    fireEvent.click(within(notice).getByRole("button", { name: "Bandingkan ulang" }));
    await waitFor(() => expect(screen.queryByText("Perbandingan ini tidak lagi sesuai dengan skenario tersimpan")).toBeNull());
    expect(calls.filter((c) => c.path === "/api/v1/scenarios/compare")).toHaveLength(1);
  }, 30_000);

  it("shows the plan limit before it blocks a new scenario, and keeps compare away from viewers", async () => {
    const state = { list: [scen("a", "Brent 70", 70), scen("b", "Brent 120", 120)] };
    server((c) => c.path === "/api/v1/org" ? json({ ...org, entitlements: { ...trial, saved_scenarios_max: 2 } })
      : c.path === "/api/v1/scenarios" ? json({ items: state.list, next_cursor: null })
      : c.path === "/api/v1/forecasts" ? json({ items: [{ id: "fc_0", forecast_origin: "2026-09" }], next_cursor: null }) : null, "viewer");
    mount("/skenario");
    expect(await screen.findByText(/Batas skenario paket Anda sudah tercapai/, {}, { timeout: 5000 })).toBeInTheDocument();
    expect(screen.getByText("2 dari 2 skenario tersimpan terpakai · paket Uji coba")).toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: "Skenario baru" })[0]).toBeDisabled();
    fireEvent.click(screen.getByRole("checkbox", { name: 'Pilih "Brent 70" untuk dibandingkan' }));
    expect(screen.getByRole("button", { name: /Bandingkan terpilih \(1\)/ })).toBeDisabled();
    expect(screen.getByText(/Peran Pembaca dapat melihat skenario tersimpan/)).toBeInTheDocument();
  }, 30_000);

  it("drops an archived scenario named in the URL from the selection: compare counts and sends only active ones (W2-R2)", async () => {
    const both = [scen("a", "Brent 70", 70), scen("b", "Brent 120", 120)];
    sessionStorage.setItem("icp.compare.a,b", JSON.stringify(compareOf(both)));
    const calls = scenarioServer({ list: [both[0]!] }); // "b" was archived after the comparison
    mount("/skenario?bandingkan=a,b");
    const notice = (await screen.findByText("Perbandingan ini tidak lagi sesuai dengan skenario tersimpan", {}, { timeout: 5000 })).closest(".callout") as HTMLElement;
    expect(notice).toHaveTextContent('Sudah diarsipkan: "Brent 120".');
    const compare = await screen.findByRole("button", { name: /Bandingkan terpilih \(1\)/ });
    expect(screen.getByText("1 dipilih")).toBeInTheDocument();
    fireEvent.click(compare);
    await waitFor(() => expect(calls.filter((c) => c.path === "/api/v1/scenarios/compare")).toHaveLength(1));
    expect(calls.find((c) => c.path === "/api/v1/scenarios/compare")!.body).toEqual({ scenario_ids: ["a"] });
  }, 30_000);

  it("reads every page of saved scenarios, and a compared scenario from a later page is not called archived (W2-R5)", async () => {
    const page1 = [scen("a", "Brent 70", 70)];
    const page2 = [scen("z", "Skenario lama", 90)];
    sessionStorage.setItem("icp.compare.a,z", JSON.stringify(compareOf([...page1, ...page2])));
    const calls = server((c) => {
      if (c.path === "/api/v1/org") return json(org);
      if (c.path === "/api/v1/scenarios" && c.method === "GET") return c.query.get("cursor") === "p2" ? json({ items: page2, next_cursor: null }) : json({ items: page1, next_cursor: "p2" });
      if (c.path === "/api/v1/data/history") return json({ points: [{ period: "2026-09", icp_price: 112.81 }] });
      if (c.path === "/api/v1/forecasts") return json({ items: [{ id: "fc_0", forecast_origin: "2026-09", status: "succeeded" }], next_cursor: null });
      return null;
    });
    mount("/skenario?bandingkan=a,z");
    expect(await screen.findByRole("checkbox", { name: 'Pilih "Skenario lama" untuk dibandingkan' }, { timeout: 5000 })).toBeChecked();
    await screen.findByRole("heading", { name: "Perbandingan terhadap proyeksi dasar" });
    expect(screen.queryByText(/Sudah diarsipkan/)).toBeNull();
    expect(screen.getByRole("link", { name: /Buat laporan perbandingan/ })).toBeInTheDocument();
    const reads = calls.filter((c) => c.path === "/api/v1/scenarios" && c.method === "GET");
    expect(reads.map((c) => c.query.get("limit"))).toEqual(["100", "100"]);
    expect(calls.some((c) => c.path === "/api/v1/scenarios/compare")).toBe(false);
  }, 30_000);

  it("explains an unreadable plan and keeps \"new scenario\" usable; the server still enforces the limit (W2-R4)", async () => {
    server((c) => c.path === "/api/v1/org" ? err(502, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE")
      : c.path === "/api/v1/scenarios" ? json({ items: [scen("a", "Brent 70", 70)], next_cursor: null })
      : c.path === "/api/v1/forecasts" ? json({ items: [{ id: "fc_0", forecast_origin: "2026-09" }], next_cursor: null }) : null);
    mount("/skenario");
    const alert = (await screen.findByText("Batas skenario paket Anda tidak dapat dimuat", {}, { timeout: 5000 })).closest(".callout") as HTMLElement;
    expect(within(alert).getByRole("button", { name: "Coba lagi" })).toBeInTheDocument();
    await waitFor(() => expect(screen.getAllByRole("button", { name: "Skenario baru" })[0]).toBeEnabled());
  }, 30_000);

  it("does not present a restored comparison as current while the scenario list cannot be read (W2-R6)", async () => {
    sessionStorage.setItem("icp.compare.a,b", JSON.stringify(compareOf([scen("a", "Brent 70", 70), scen("b", "Brent 120", 120)])));
    server((c) => c.path === "/api/v1/org" ? json(org) : c.path === "/api/v1/scenarios" ? err(502, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE")
      : c.path === "/api/v1/forecasts" ? json({ items: [], next_cursor: null }) : null);
    mount("/skenario?bandingkan=a,b");
    expect(await screen.findByText("Belum dapat diperiksa apakah perbandingan ini masih sesuai", {}, { timeout: 5000 })).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: /Buat laporan perbandingan/ })).toBeNull();
  }, 30_000);

  it("labels a comparison made before the latest official ICP month, next to its chart and table too (W2-R6)", async () => {
    const list = [scen("a", "Brent 70", 70)];
    sessionStorage.setItem("icp.compare.a", JSON.stringify({ ...compareOf(list), forecast_origin: "2026-08" }));
    scenarioServer({ list });
    mount("/skenario?bandingkan=a");
    const notice = (await screen.findByText("Perbandingan ini dibuat sebelum data ICP resmi terbaru", {}, { timeout: 5000 })).closest(".callout") as HTMLElement;
    expect(notice).toHaveTextContent("Data ICP resmi kini sampai September 2026");
    expect(screen.getAllByText(/Perbandingan lama: tidak lagi sesuai/)).toHaveLength(2);
    expect(screen.queryByRole("link", { name: /Buat laporan perbandingan/ })).toBeNull();
  }, 30_000);

  it("keeps a card's colour and marker those of the comparison on screen when the selection changes (W2-R7)", async () => {
    scenarioServer({ list: [scen("a", "Brent 70", 70), scen("b", "Brent 120", 120)] });
    mount("/skenario");
    fireEvent.click(await screen.findByRole("checkbox", { name: 'Pilih "Brent 70" untuk dibandingkan' }, { timeout: 5000 }));
    fireEvent.click(screen.getByRole("checkbox", { name: 'Pilih "Brent 120" untuk dibandingkan' }));
    fireEvent.click(screen.getByRole("button", { name: /Bandingkan terpilih \(2\)/ }));
    await screen.findByRole("heading", { name: "Perbandingan terhadap proyeksi dasar" });
    fireEvent.click(screen.getByRole("checkbox", { name: 'Pilih "Brent 70" untuk dibandingkan' })); // untick A
    const cardB = screen.getByRole("checkbox", { name: 'Pilih "Brent 120" untuk dibandingkan' }).closest("li") as HTMLElement;
    expect(within(cardB).getByText("skenario ke-2 dalam perbandingan")).toBeInTheDocument();
  }, 30_000);
});

describe("notifications: rule kinds that fail to load", () => {
  it("explains why the add-rule form is unavailable and offers a retry (W2-R3)", async () => {
    let kindReads = 0;
    server((c) => {
      if (c.path === "/api/v1/org") return json(org);
      if (c.path === "/api/v1/alerts/kinds") return ++kindReads === 1 ? err(502, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE")
        : json([{ kind: "icp_published", label_id: "ICP baru terbit", label_en: "New ICP published", params: {} }]);
      if (c.path === "/api/v1/alerts/rules") return json([]);
      if (c.path === "/api/v1/alerts/events") return json({ items: [], next_cursor: null, unread: 0 });
      return null;
    });
    mount("/pemberitahuan");
    const alert = (await screen.findByText("Jenis aturan tidak dapat dimuat", {}, { timeout: 5000 })).closest(".callout") as HTMLElement;
    expect(screen.getByText("Aturan baru dapat ditambahkan setelah jenis aturan dimuat.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Tambah aturan" })).toBeDisabled();
    fireEvent.click(within(alert).getByRole("button", { name: "Coba lagi" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "Tambah aturan" })).toBeEnabled());
    expect(screen.queryByText("Jenis aturan tidak dapat dimuat")).toBeNull();
  }, 30_000);
});

/* ---------- reports ---------------------------------------------------------------------------- */
const report = (id: string, status: "succeeded" | "failed") => ({ id, org_id: "org_1", created_by: "user:u1", kind: "market_monthly", label: "x", params: {}, status,
  created_at: "2026-10-25T07:00:00Z", finished_at: "2026-10-25T07:00:01Z", formats: ["pdf", "csv"], error_message: status === "failed" ? "boom" : null, summary: status === "failed" ? null : { period: "2026-09" } });
function reportServer(post: (init: RequestInit) => Response | Promise<Response>, list: unknown[] = []) {
  return server((c, init) => {
    if (c.path === "/api/v1/org") return json(org);
    if (c.path === "/api/v1/org/members") return json([{ id: "u1", name: "Penguji UX", email: "ux@test.id", role: "owner", status: "active" }]);
    if (c.path === "/api/v1/reports" && c.method === "POST") return post(init);
    if (c.path === "/api/v1/reports") return json({ items: list, next_cursor: null });
    if (c.path === "/api/v1/scenarios") return json({ items: [], next_cursor: null });
    return null;
  });
}

describe("reports: explicit outcomes", () => {
  it("never offers downloads for a failed report, in the result or in the list, and names the creator", async () => {
    reportServer(() => json(report("r_new", "failed"), 201), [report("r_old", "failed"), report("r_ok", "succeeded")]);
    mount("/laporan");
    fireEvent.click(await screen.findByRole("button", { name: "Buat laporan" }, { timeout: 5000 }));
    const failed = (await screen.findByText("Laporan gagal dibuat")).closest(".callout") as HTMLElement;
    expect(within(failed).queryByRole("button", { name: /^(PDF|CSV)$/ })).toBeNull();
    expect(failed).toHaveTextContent("Kode laporan: r_new");
    const rows = screen.getAllByRole("row").slice(1);
    const failedRow = rows.find((r) => r.textContent?.includes("Gagal"))!;
    expect(within(failedRow).queryByRole("button", { name: /Unduh/ })).toBeNull();
    expect(failedRow).toHaveTextContent("Tidak ada berkas karena laporan gagal dibuat.");
    const okRow = rows.find((r) => r.textContent?.includes("Berhasil"))!;
    expect(within(okRow).getAllByRole("button", { name: /Unduh (PDF|CSV)/ })).toHaveLength(2);
    expect(okRow).toHaveTextContent("oleh Penguji UX");
  }, 30_000);

  it("explains a client timeout: the report may still finish, check the list instead of generating again", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    let posts = 0;
    const calls = reportServer((init) => ++posts > 1 ? json(report("r_late", "succeeded"), 201) : new Promise<Response>((_, reject) => {
      init.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")));
    }));
    mount("/laporan");
    fireEvent.click(await screen.findByRole("button", { name: "Buat laporan" }, { timeout: 5000 }));
    expect(await screen.findByText("Membuat laporan pasar bulanan…")).toBeInTheDocument(); // sentence case inside a sentence
    await act(async () => { vi.advanceTimersByTime(96_000); });
    const notice = (await screen.findByText("Server belum menjawab dalam 95 detik")).closest(".callout") as HTMLElement;
    expect(notice).toHaveTextContent(/Laporan mungkin tetap selesai di server/);
    expect(notice).toHaveTextContent(/Periksa daftar Laporan yang dibuat/);
    const listReads = () => calls.filter((c) => c.path === "/api/v1/reports" && c.method === "GET").length;
    const before = listReads();
    fireEvent.click(within(notice).getByRole("button", { name: "Periksa daftar sekarang" }));
    await waitFor(() => expect(listReads()).toBeGreaterThan(before));
    expect(calls.filter((c) => c.path === "/api/v1/reports" && c.method === "POST")).toHaveLength(1);
    // Completion is never promised; a retry of the same request reuses its Idempotency-Key, so a report
    // that did finish comes back instead of a second, separately counted one.
    expect(notice).toHaveTextContent(/atau mungkin gagal/);
    fireEvent.click(within(notice).getByRole("button", { name: "Coba lagi" }));
    expect(await screen.findByText("Laporan siap diunduh")).toBeInTheDocument();
    const sent = calls.filter((c) => c.path === "/api/v1/reports" && c.method === "POST");
    expect(sent).toHaveLength(2);
    expect(sent[0]!.key).toMatch(/^[A-Za-z0-9._:-]{8,128}$/);
    expect(sent[1]!.key).toBe(sent[0]!.key);
    expect(sent[1]!.body).toEqual(sent[0]!.body);
  }, 30_000);

  it("treats a gateway 504 as an unknown outcome (not an outage) and retries with the same key", async () => {
    let posts = 0;
    const calls = reportServer(() => ++posts === 1
      ? err(504, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE")
      : json(report("r_1", "succeeded"), 201));
    mount("/laporan");
    fireEvent.click(await screen.findByRole("button", { name: "Buat laporan" }, { timeout: 5000 }));
    const notice = (await screen.findByText("Server tidak menjawab dalam batas waktu layanan")).closest(".callout") as HTMLElement;
    expect(notice).toHaveTextContent(/Laporan mungkin tetap selesai di server, atau mungkin gagal/);
    expect(screen.queryByText(/Layanan data sedang tidak tersedia/)).toBeNull();
    // The list is re-read at once to look for the report.
    await waitFor(() => expect(calls.filter((c) => c.path === "/api/v1/reports" && c.method === "GET").length).toBeGreaterThan(1));
    fireEvent.click(within(notice).getByRole("button", { name: "Coba lagi" }));
    expect(await screen.findByText("Laporan siap diunduh")).toBeInTheDocument();
    const sent = calls.filter((c) => c.path === "/api/v1/reports" && c.method === "POST");
    expect(sent.map((c) => c.key)).toEqual([sent[0]!.key, sent[0]!.key]);
  }, 30_000);

  it("says a retried report is still being generated (409 IDEMPOTENCY_IN_PROGRESS), keeps its key and waits as long as the server asks", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const inProgress = new Response(JSON.stringify({ error: { code: "IDEMPOTENCY_IN_PROGRESS", message: "Laporan dari permintaan ini masih dibuat.", request_id: "r", details: { retry_after_seconds: 40 } } }),
      { status: 409, headers: { "content-type": "application/json", "retry-after": "40" } });
    const answers = [err(504, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE"), inProgress];
    const calls = reportServer(() => answers.shift() ?? json(report("r_1", "succeeded"), 201));
    mount("/laporan");
    fireEvent.click(await screen.findByRole("button", { name: "Buat laporan" }, { timeout: 5000 }));
    const first = (await screen.findByText("Server tidak menjawab dalam batas waktu layanan")).closest(".callout") as HTMLElement;
    const firstSent = first.textContent?.match(/Permintaan dikirim (.+?)\.(?=Jika)/)?.[1];
    expect(firstSent).toBeTruthy();
    await act(async () => { vi.advanceTimersByTime(61_000); }); // the retry is sent a minute later
    fireEvent.click(within(first).getByRole("button", { name: "Coba lagi" }));
    const notice = (await screen.findByText("Laporan ini masih dibuat di server")).closest(".callout") as HTMLElement;
    expect(notice).toHaveTextContent(/server tidak membuat laporan kedua/);
    expect(notice).toHaveTextContent(/berhasil atau gagal/);
    // The request still running is the first one: its send time, not the retry's, is named.
    expect(notice).toHaveTextContent(`Permintaan pertama dikirim ${firstSent!}.`);
    expect(screen.queryByText(/Permintaan tidak dapat diselesaikan/)).toBeNull();
    // The server asked for 40 s (not the 15 s fallback): neither the retry nor a new submission is
    // possible before that, and the notice says so next to its own disabled button.
    expect(within(notice).getByText(/Tombol Coba lagi aktif kembali dalam 40 detik/)).toBeInTheDocument();
    expect(screen.getByText(/Tombol Buat laporan aktif kembali dalam 40 detik/)).toBeInTheDocument();
    expect(within(notice).getByRole("button", { name: "Coba lagi" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Buat laporan" })).toBeDisabled();
    let sent = calls.filter((c) => c.path === "/api/v1/reports" && c.method === "POST");
    expect(sent).toHaveLength(2);
    expect(sent[1]!.key).toBe(sent[0]!.key);
    // After the wait the same request (same key and body) can be sent again.
    for (let i = 0; i < 41; i++) await act(async () => { vi.advanceTimersByTime(1000); });
    const retry = within(notice).getByRole("button", { name: "Coba lagi" });
    expect(retry).toBeEnabled();
    fireEvent.click(retry);
    expect(await screen.findByText("Laporan siap diunduh")).toBeInTheDocument();
    sent = calls.filter((c) => c.path === "/api/v1/reports" && c.method === "POST");
    expect(sent).toHaveLength(3);
    expect(sent[2]!.key).toBe(sent[0]!.key);
    expect(sent[2]!.body).toEqual(sent[0]!.body);
  }, 30_000);

  it("waits the fallback 15 s when the in-progress answer names no wait", async () => {
    const inProgress = new Response(JSON.stringify({ error: { code: "IDEMPOTENCY_IN_PROGRESS", message: "Laporan dari permintaan ini masih dibuat.", request_id: "r", details: {} } }),
      { status: 409, headers: { "content-type": "application/json" } });
    const answers = [err(504, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE"), inProgress];
    reportServer(() => answers.shift() ?? json(report("r_1", "succeeded"), 201));
    mount("/laporan");
    fireEvent.click(await screen.findByRole("button", { name: "Buat laporan" }, { timeout: 5000 }));
    const first = (await screen.findByText("Server tidak menjawab dalam batas waktu layanan")).closest(".callout") as HTMLElement;
    fireEvent.click(within(first).getByRole("button", { name: "Coba lagi" }));
    const notice = (await screen.findByText("Laporan ini masih dibuat di server")).closest(".callout") as HTMLElement;
    expect(within(notice).getByText(/Tombol Coba lagi aktif kembali dalam 15 detik/)).toBeInTheDocument();
    expect(within(notice).getByRole("button", { name: "Coba lagi" })).toBeDisabled();
  }, 30_000);

  it("gives a changed form a new key, reuses the key for the same form, and starts afresh after a finished report", async () => {
    const answers = [err(502, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE"), err(504, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE"), json(report("r_2", "succeeded"), 201), json(report("r_3", "succeeded"), 201)];
    const calls = reportServer(() => answers.shift()!);
    mount("/laporan");
    const generate = await screen.findByRole("button", { name: "Buat laporan" }, { timeout: 5000 });
    fireEvent.click(generate);
    expect(await screen.findByText("Sambungan ke server terputus sebelum jawaban diterima")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("checkbox", { name: "CSV" })); // a different request now
    fireEvent.click(generate);
    await screen.findByText("Server tidak menjawab dalam batas waktu layanan");
    fireEvent.click(generate); // the same form again: same key
    await screen.findByText("Laporan siap diunduh");
    fireEvent.click(generate); // a new submission after a definite outcome: new key
    await waitFor(() => expect(calls.filter((c) => c.path === "/api/v1/reports" && c.method === "POST")).toHaveLength(4));
    const [k1, k2, k3, k4] = calls.filter((c) => c.path === "/api/v1/reports" && c.method === "POST").map((c) => c.key);
    expect(k2).not.toBe(k1);
    expect(k3).toBe(k2);
    expect(k4).not.toBe(k3);
    expect(new Set([k1, k2, k4]).size).toBe(3);
  }, 30_000);

  it("offers only CSV while the plan is unknown, says why, and never marks old reports new", async () => {
    let listReads = 0;
    const old = { ...report("r_old", "succeeded"), created_at: "2026-10-01T07:00:00Z" };
    server((c) => {
      if (c.path === "/api/v1/org") return err(502, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE");
      if (c.path === "/api/v1/reports" && c.method === "POST") return json({ ...report("r_now", "succeeded"), formats: ["csv"], created_at: new Date().toISOString() }, 201);
      // The list fails on the first read (so nothing was known when the report was requested).
      if (c.path === "/api/v1/reports") return ++listReads === 1 ? err(500, "INTERNAL_ERROR") : json({ items: [{ ...report("r_now", "succeeded"), formats: ["csv"], created_at: new Date().toISOString() }, old], next_cursor: null });
      if (c.path === "/api/v1/scenarios") return json({ items: [], next_cursor: null });
      return null;
    });
    mount("/laporan");
    await screen.findByRole("button", { name: "Buat laporan" }, { timeout: 5000 });
    expect(screen.getByRole("checkbox", { name: /PDF/ })).toBeDisabled();
    expect(screen.getByRole("checkbox", { name: /XLSX/ })).toBeDisabled();
    expect(screen.getByRole("checkbox", { name: "CSV" })).toBeChecked();
    expect(screen.queryByText("tidak termasuk paket Anda")).toBeNull(); // unknown is not "excluded"
    expect(await screen.findByText(/CSV tersedia di semua paket/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Buat laporan" }));
    await screen.findByText("Laporan siap diunduh");
    const rows = await screen.findAllByRole("row");
    // Not a plain substring: on the 11th, 21st or 31st the new report's "31 Okt 2026" contains "1 Okt 2026".
    const oldRow = rows.find((r) => /(?<!\d)1 Okt 2026/.test(r.textContent ?? ""))!;
    expect(within(oldRow).queryByText("Baru")).toBeNull();
    // With the list readable again, the unreadable plan is explained with a retry (one error at a time).
    const planError = (await screen.findByText("Paket organisasi tidak dapat dimuat")).closest(".callout") as HTMLElement;
    expect(within(planError).getByRole("button", { name: "Coba lagi" })).toBeInTheDocument();
  }, 30_000);

  it("waits out a rate limit before the button works again", async () => {
    reportServer(() => new Response(JSON.stringify({ error: { code: "RATE_LIMITED", message: "Too many requests.", request_id: "r", details: { retry_after_seconds: 30 } } }), { status: 429, headers: { "content-type": "application/json", "retry-after": "30" } }));
    mount("/laporan");
    fireEvent.click(await screen.findByRole("button", { name: "Buat laporan" }, { timeout: 5000 }));
    expect(await screen.findByText(/aktif kembali dalam 30 detik/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Buat laporan" })).toBeDisabled();
  }, 30_000);
});

/* ---------- insight sheet and account ------------------------------------------------------------ */
describe("insight sheet evidence", () => {
  it("names the kind of evidence, the month, the comparison window and where the data comes from", async () => {
    const fact = (key: string, label: string, display: string) => ({ key, label, value: 1, unit: "", display });
    const insight = {
      id: "ins_fc", rule_id: "fc", rule_version: 1, kind: "forecast_outlook", importance: "medium", severity: "notice", audience: "customer", period: "2026-10",
      title: "Proyeksi Oktober 2026: USD 112,90", body: "Ringkasan.", next_step: null, generated: true, facts: [fact("p", "Proyeksi", "USD 112,90")],
      evidence: { datasets: [{ dataset: "icp", label: "ICP resmi", provider: "Kementerian ESDM", version_id: "v1", period_end: "2026-09" }],
        periods: { subject: "2026-10", window_start: "2023-09", window_end: "2026-08", n: 36 }, inputs: [fact("i", "ICP Sep 2026", "112,81 USD/bbl")], outputs: [fact("o", "Batas atas 80%", "USD 121,85")],
        method: { key: "m", text: "Metode." }, model: null, forecast_ids: ["fc_77"] },
      metrics: [], sources: [],
    };
    server((c) => c.path === "/api/v1/plans" ? json([trial]) : c.path === "/api/v1/insights"
      ? json({ generated_at: "2026-10-25T00:00:00Z", lang: "id", engine_version: "e1", basis: { icp_period: "2026-09" }, disclaimer: "D.", insights: [insight] }) : null);
    mount("/insight?insight=ins_fc");
    const sheet = await screen.findByRole("dialog", { name: /Proyeksi Oktober 2026/ }, { timeout: 5000 });
    for (const h of ["Apa yang terjadi", "Angka pendukung", "Periode pembanding", "Jenis bukti", "Mengapa ditampilkan", "Cara menghitung", "Batasan", "Sumber dan tautan ke data"]) {
      expect(within(sheet).getByRole("heading", { level: 3, name: h })).toBeInTheDocument();
    }
    expect(within(sheet).getByText("Terkait proyeksi")).toBeInTheDocument();
    expect(within(sheet).getByText(/bukan harga resmi yang akan ditetapkan/)).toBeInTheDocument();
    expect(within(sheet).getByText("Jendela pembanding: Sep 2023 s.d. Agu 2026 (36 bulan data)")).toBeInTheDocument();
    expect(within(sheet).getByText("ICP resmi · Kementerian ESDM · data hingga Sep 2026")).toBeInTheDocument();
    expect(within(sheet).getByRole("link", { name: /Metodologi insight dan proyeksi/ })).toHaveAttribute("href", "/metodologi");
  }, 30_000);
});

describe("account page structure", () => {
  it("uses only dt/dd groups inside its definition lists (axe definition-list)", async () => {
    const me = { user: { id: "u1", name: "Penguji UX", email: "ux@test.id", role: "owner", status: "active", created_at: "2026-10-01T00:00:00Z", last_login_at: null, must_change_password: false }, organization: org, sessions: [] };
    server((c) => c.path === "/api/v1/auth/me" ? json(me) : c.path === "/api/v1/org" ? json(org) : c.path === "/api/v1/org/members" ? json([me.user])
      : c.path === "/api/v1/org/plan-requests" ? json({ items: [] }) : c.path === "/api/v1/org/deletion-requests" ? json({ items: [] }) : c.path === "/api/v1/auth/sessions" ? json([]) : null);
    const { container } = mount("/akun");
    await screen.findByText("Batas paket yang terpakai", {}, { timeout: 5000 });
    const lists = [...container.querySelectorAll("dl")];
    expect(lists.length).toBeGreaterThan(1);
    for (const dl of lists) {
      for (const child of [...dl.children]) expect(["DT", "DD", "DIV"]).toContain(child.tagName);
      for (const div of [...dl.children].filter((c) => c.tagName === "DIV")) for (const g of [...div.children]) expect(["DT", "DD"]).toContain(g.tagName);
    }
    expect(screen.getByRole("navigation", { name: "Bagian halaman akun" })).toBeInTheDocument();
    expect(screen.getByText("2 dari 5")).toBeInTheDocument();
  }, 30_000);
});
