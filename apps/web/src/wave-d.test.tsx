/**
 * Production-readiness wave D (audit ux7/audit-3): server messages in the reader's language,
 * focus kept on busy buttons, field errors tied to their fields, accessible plan features,
 * confirmed destructive member/key actions, own-row detection by id, WIB times, localized month
 * codes, timeout vs offline, hidden operator title and the in-memory public cache.
 */
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import { api, ApiError, publicGet } from "./api/client";
import { AuthProvider } from "./auth";
import { ToastProvider } from "./components/toast";
import { Button, ErrorState, errorText, messageLocale } from "./components/ui";
import { dateTime, localizePeriods } from "./format";
import { I18nProvider, translate, type Key } from "./i18n";
import { PlanCard } from "./pages/LandingPage";

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}
const apiErr = (status: number, code: string, message: string) => json({ error: { code, message, request_id: "req_d1", details: {} } }, status);
type Vars = Record<string, string | number | null | undefined>;
const tId = (k: Key, v?: Vars) => translate("id", k, v);
const tEn = (k: Key, v?: Vars) => translate("en", k, v);

afterEach(() => { cleanup(); vi.unstubAllGlobals(); vi.useRealTimers(); localStorage.clear(); sessionStorage.clear(); });

describe("COPY-02: server messages in the reader's language", () => {
  it("detects Indonesian and English server sentences", () => {
    expect(messageLocale("Email ini sudah terdaftar. Silakan masuk.")).toBe("id");
    expect(messageLocale("Email atau kata sandi salah.")).toBe("id");
    expect(messageLocale("Invalid credentials.")).toBe("en");
    expect(messageLocale("2 month-over-month change(s) larger than 40%")).not.toBe("id");
  });

  it("shows the Indonesian EMAIL_TAKEN and AUTH_INVALID texts instead of a generic conflict/unauthorized text", () => {
    const taken = errorText(new ApiError(409, "EMAIL_TAKEN", "Email ini sudah terdaftar. Silakan masuk.", "r"), tId, "id");
    expect(taken.title).toBe("Email sudah terdaftar");
    expect(taken.body).toBe("Email ini sudah terdaftar. Silakan masuk.");
    const bad = errorText(new ApiError(401, "AUTH_INVALID", "Email atau kata sandi salah.", "r"), tId, "id");
    expect(bad.title).toBe("Periksa kembali isian Anda");
    expect(bad.body).toBe("Email atau kata sandi salah.");
  });

  it("keeps the translated text when the server wrote English, and in English uses the English fallback for an Indonesian message", () => {
    const expired = errorText(new ApiError(401, "AUTH_INVALID", "Invalid credentials.", "r"), tId, "id");
    expect(expired.body).toBe(tId("error.unauthorizedBody"));
    const enTaken = errorText(new ApiError(409, "EMAIL_TAKEN", "Email ini sudah terdaftar. Silakan masuk.", "r"), tEn, "en");
    expect(enTaken.body).toBe(tEn("error.emailTakenBody"));
  });

  it("never shows server text for 5xx or anything that looks like a stack trace", () => {
    expect(errorText(new ApiError(500, "INTERNAL_ERROR", "Traceback (most recent call last): sqlite3", "r"), tId, "id").body).toBe(tId("error.genericBody"));
    expect(errorText(new ApiError(422, "VALIDATION_ERROR", "Exception at icp_api/x.py yang tidak valid", "r"), tId, "id").body).toBe(tId("error.validationBody"));
  });

  it("shows the request id for a 409 (support can trace it) but not for a credential 401, and no sign-in link on a credential error", () => {
    const { rerender } = render(<MemoryRouter><I18nProvider initial="id"><ErrorState error={new ApiError(409, "CONFLICT", "Masih ada permintaan perubahan paket yang menunggu keputusan.", "req_conf")} /></I18nProvider></MemoryRouter>);
    expect(screen.getByRole("alert")).toHaveTextContent("Masih ada permintaan perubahan paket yang menunggu keputusan.");
    expect(screen.getByRole("alert")).toHaveTextContent("req_conf");
    rerender(<MemoryRouter><I18nProvider initial="id"><ErrorState error={new ApiError(401, "AUTH_INVALID", "Email atau kata sandi salah.", "req_401")} /></I18nProvider></MemoryRouter>);
    expect(screen.getByRole("alert")).not.toHaveTextContent("req_401");
    expect(screen.queryByRole("link", { name: "Masuk" })).toBeNull();
  });
});

describe("UX-04: timeout, offline and network errors are told apart", () => {
  it("a request that exceeds the client timeout is TIMEOUT, not 'check your connection'", async () => {
    vi.stubGlobal("fetch", (_url: string, init: RequestInit) => new Promise((_res, rej) => {
      init.signal?.addEventListener("abort", () => rej(new DOMException("aborted", "AbortError")));
    }));
    await expect(api("/slow", { timeoutMs: 20 })).rejects.toMatchObject({ code: "TIMEOUT", status: 0 });
    const text = errorText(new ApiError(0, "TIMEOUT", "x", null), tId, "id");
    expect(text.title).toBe("Server lambat menjawab");
    expect(text.body).toMatch(/Koneksi Anda berfungsi/);
  });

  it("a failed fetch while the browser is offline is OFFLINE", async () => {
    vi.stubGlobal("fetch", async () => { throw new TypeError("Failed to fetch"); });
    const spy = vi.spyOn(navigator, "onLine", "get").mockReturnValue(false);
    await expect(api("/x")).rejects.toMatchObject({ code: "OFFLINE" });
    spy.mockReturnValue(true);
    await expect(api("/x")).rejects.toMatchObject({ code: "NETWORK_ERROR" });
    spy.mockRestore();
  });
});

describe("A11Y-01: a busy button keeps keyboard focus", () => {
  it("stays focusable (aria-disabled, not disabled) and ignores clicks while loading", () => {
    const onClick = vi.fn();
    const { rerender } = render(<Button onClick={onClick}>Kirim</Button>);
    const btn = screen.getByRole("button", { name: "Kirim" });
    btn.focus();
    rerender(<Button onClick={onClick} loading>Kirim</Button>);
    expect(btn).not.toBeDisabled();
    expect(btn).toHaveAttribute("aria-disabled", "true");
    expect(btn).toHaveAttribute("aria-busy", "true");
    expect(btn).toHaveFocus();
    fireEvent.click(btn);
    expect(onClick).not.toHaveBeenCalled();
    rerender(<Button onClick={onClick}>Kirim</Button>);
    fireEvent.click(btn);
    expect(onClick).toHaveBeenCalledTimes(1);
  });
});

const mount = (path: string) => render(
  <MemoryRouter initialEntries={[path]}><I18nProvider initial="id"><ToastProvider><AuthProvider><App /></AuthProvider></ToastProvider></I18nProvider></MemoryRouter>,
);

describe("A11Y-02 + COPY-02: registration errors belong to their fields", () => {
  it("marks every invalid field, describes it and focuses the first one", async () => {
    vi.stubGlobal("fetch", async () => json({ error: { code: "NOT_FOUND", message: "x", request_id: null, details: {} } }, 404));
    mount("/daftar");
    fireEvent.click(await screen.findByRole("button", { name: "Daftar" }));
    const org = screen.getByLabelText("Nama organisasi");
    await waitFor(() => expect(org).toHaveFocus());
    expect(org).toHaveAttribute("aria-invalid", "true");
    expect(org).toHaveAccessibleDescription("Kolom ini wajib diisi.");
    // the error is a description, not part of the name
    expect(org).toHaveAccessibleName("Nama organisasi");
    const pw = screen.getByLabelText("Kata sandi");
    expect(pw).toHaveAccessibleDescription("Kata sandi minimal 10 karakter. Minimal 10 karakter.");
    expect(screen.getByRole("checkbox")).toHaveAccessibleDescription("Anda harus menyetujui syarat dan kebijakan privasi.");
    expect(screen.getByText("Perbaiki 5 isian yang ditandai.")).toBeInTheDocument();
  });

  it("puts the server's 'email already registered' message on the email field", async () => {
    vi.stubGlobal("fetch", async (url: string) => url.includes("/auth/register")
      ? apiErr(409, "EMAIL_TAKEN", "Email ini sudah terdaftar. Silakan masuk.")
      : json({ error: { code: "NOT_FOUND", message: "x", request_id: null, details: {} } }, 404));
    mount("/daftar");
    fireEvent.change(await screen.findByLabelText("Nama organisasi"), { target: { value: "PT Uji" } });
    fireEvent.change(screen.getByLabelText("Nama lengkap"), { target: { value: "Penguji" } });
    fireEvent.change(screen.getByLabelText("Email"), { target: { value: "ada@uji.test" } });
    fireEvent.change(screen.getByLabelText("Kata sandi"), { target: { value: "panjang-sekali-123" } });
    fireEvent.click(screen.getByRole("checkbox"));
    fireEvent.click(screen.getByRole("button", { name: "Daftar" }));
    const email = screen.getByLabelText("Email");
    await waitFor(() => expect(email).toHaveAccessibleDescription("Email ini sudah terdaftar. Silakan masuk."));
    await waitFor(() => expect(email).toHaveFocus());
    expect(document.body.textContent).not.toMatch(/bertentangan dengan keadaan saat ini/);
  });

  it("shows 'Email atau kata sandi salah.' on a failed sign-in, without a sign-in link to the same page", async () => {
    vi.stubGlobal("fetch", async (url: string) => url.includes("/auth/login")
      ? apiErr(401, "AUTH_INVALID", "Email atau kata sandi salah.")
      : json({ error: { code: "NOT_FOUND", message: "x", request_id: null, details: {} } }, 404));
    mount("/masuk");
    fireEvent.change(await screen.findByLabelText("Email"), { target: { value: "a@uji.test" } });
    fireEvent.change(screen.getByLabelText("Kata sandi"), { target: { value: "salah" } });
    const submit = screen.getByRole("button", { name: "Masuk" });
    submit.focus();
    fireEvent.click(submit);
    const alert = (await screen.findByText("Email atau kata sandi salah.")).closest("[role=alert]") as HTMLElement;
    expect(alert).not.toBeNull();
    expect(within(alert).queryByRole("link")).toBeNull();
    expect(submit).toHaveFocus();
  });
});

const trial = { code: "trial", label_id: "Uji coba", label_en: "Trial", price_note_id: "Gratis", price_note_en: "Free", seats: 2, forecast_horizon_max: 3, scenarios: true,
  saved_scenarios_max: 2, reports_pdf: true, reports_excel: false, alerts: true, alert_rules_max: 2, api_access: false, api_requests_per_month: 0, history_years: 3, insights: true };

describe("A11Y-03 + COPY-03: plan cards", () => {
  it("says in text which features are not included, and groups thousands", () => {
    render(<MemoryRouter><I18nProvider initial="id"><PlanCard p={{ ...trial, seats: 1000, saved_scenarios_max: 1000, alert_rules_max: 500 }} /></I18nProvider></MemoryRouter>);
    const items = screen.getAllByRole("listitem").map((li) => li.textContent);
    expect(items).toContain("Laporan Excel (tidak termasuk)");
    expect(items).toContain("Tanpa akses API (tidak termasuk)");
    expect(items).toContain("Laporan PDF");
    expect(items).toContain("1.000 kursi");
    expect(items).toContain("1.000 skenario tersimpan");
  });
});

describe("COPY-03: times and month codes", () => {
  it("shows times in WIB with the zone label, whatever the device zone", () => {
    expect(dateTime("2026-10-20T02:02:00Z", "id-ID")).toMatch(/20 Okt 2026.*09[.:]02 WIB$/);
  });

  it("turns bare month codes in server sentences into month labels, leaving full dates alone", () => {
    expect(localizePeriods("Rentang untuk 2026-11, 2026-12 lebih sempit", "id")).toBe("Rentang untuk Nov 2026, Des 2026 lebih sempit");
    expect(localizePeriods("dimuat 2026-10-17", "id")).toBe("dimuat 2026-10-17");
  });
});

describe("PERF-01: public reference data is cached in memory only", () => {
  it("fetches /plans once for several callers and never sends a credential with it", async () => {
    localStorage.setItem("icp.sessionToken", "icp_sess_secret");
    const fetchMock = vi.fn(async () => json([trial]));
    vi.stubGlobal("fetch", fetchMock);
    await publicGet("/plans");
    await publicGet("/plans");
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const init = (fetchMock.mock.calls[0] as unknown as [string, RequestInit])[1];
    expect((init.headers as Record<string, string>).Authorization).toBeUndefined();
  });
});

/* ---------- account: members and API keys --------------------------------------------------- */
const pro = { ...trial, code: "professional", label_id: "Profesional", label_en: "Professional", seats: 10, api_access: true, api_requests_per_month: 20000, reports_excel: true };
const orgFor = (ent: typeof trial) => ({ id: "org_1", name: "PT Uji", slug: "pt-uji", plan: ent.code, status: "active", created_at: "2026-10-01T00:00:00Z", entitlements: ent, plans: [trial, pro],
  members: 3, active_members: 3, saved_scenarios: 0, alert_rules: 0, usage_this_month: {} });
const user = (id: string, name: string, email: string, role: string) => ({ id, name, email, role, status: "active", created_at: "2026-10-01T00:00:00Z", last_login_at: "2026-10-20T02:02:00Z", must_change_password: false });
type Call = { path: string; method: string; body: unknown };

function accountServer(role: "owner" | "admin", self: ReturnType<typeof user>, members: Array<ReturnType<typeof user>>, extra: (c: Call) => Response | null = () => null) {
  localStorage.setItem("icp.sessionToken", "icp_sess_test");
  const calls: Call[] = [];
  vi.stubGlobal("fetch", async (url: string, init: RequestInit = {}) => {
    const path = url.split("?")[0] ?? "";
    const c = { path, method: init.method ?? "GET", body: init.body ? JSON.parse(String(init.body)) : null };
    calls.push(c);
    const x = extra(c);
    if (x) return x;
    // whoami's display name equals another member's e-mail-like name on purpose: own-row detection must use ids.
    if (path === "/api/v1/auth/whoami") return json({ authenticated: true, name: self.name, role, kind: "user", org_id: "org_1", org_name: "PT Uji", plan: "professional", must_change_password: false, horizon_max: 6 });
    if (path === "/api/v1/auth/me") return json({ user: self, organization: orgFor(pro), sessions: [] });
    if (path === "/api/v1/org") return json(orgFor(pro));
    if (path === "/api/v1/org/members") return json(members);
    if (path === "/api/v1/org/api-keys") return json([{ id: "key_1", name: "ERP", prefix: "icp_live_ab", created_at: "2026-10-01T00:00:00Z", last_used_at: null, revoked_at: null }]);
    if (path === "/api/v1/org/plan-requests" || path === "/api/v1/org/deletion-requests") return json({ items: [] });
    return json({ error: { code: "NOT_FOUND", message: "x", request_id: null, details: {} } }, 404);
  });
  return calls;
}

describe("A11Y-04 / UX-02 / UX-03 / A11Y-06: members and API keys", () => {
  const owner = user("u_owner", "Pemilik Uji", "pemilik@uji.test", "owner");
  const admin = user("u_admin", "Admin Uji", "admin@uji.test", "admin");
  const viewer = user("u_viewer", "Pembaca Uji", "pembaca@uji.test", "viewer");

  it("changes a role only after Apply and a confirmation; the select is labelled per member", async () => {
    const calls = accountServer("owner", owner, [owner, admin, viewer], (c) => c.path === "/api/v1/org/members/u_viewer" && c.method === "POST" ? json({ ...viewer, role: "analyst" }) : null);
    mount("/akun");
    const select = await screen.findByRole("combobox", { name: "Peran untuk Pembaca Uji" });
    fireEvent.change(select, { target: { value: "analyst" } });
    expect(calls.some((c) => c.path === "/api/v1/org/members/u_viewer")).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Terapkan peran untuk Pembaca Uji" }));
    const dialog = await screen.findByRole("alertdialog", { name: "Ubah peran Pembaca Uji menjadi Analis?" });
    fireEvent.click(within(dialog).getByRole("button", { name: "Ubah peran" }));
    await waitFor(() => expect(calls.find((c) => c.path === "/api/v1/org/members/u_viewer" && c.method === "POST")?.body).toEqual({ role: "analyst" }));
  });

  it("asks before disabling a member or resetting a password, with unique action names, and announces the temporary password", async () => {
    const calls = accountServer("owner", owner, [owner, viewer], (c) => c.path === "/api/v1/org/members/u_viewer/reset-password" ? json({ temporary_password: "Icp-sementara" }) : null);
    mount("/akun");
    fireEvent.click(await screen.findByRole("button", { name: "Nonaktifkan: Pembaca Uji" }));
    const d1 = await screen.findByRole("alertdialog", { name: "Nonaktifkan Pembaca Uji?" });
    fireEvent.click(within(d1).getByRole("button", { name: "Batal" }));
    expect(calls.some((c) => c.path === "/api/v1/org/members/u_viewer" && c.method === "POST")).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Atur ulang kata sandi: Pembaca Uji" }));
    const d2 = await screen.findByRole("alertdialog", { name: "Atur ulang kata sandi Pembaca Uji?" });
    fireEvent.click(within(d2).getByRole("button", { name: "Atur ulang kata sandi" }));
    const secret = await screen.findByText("Icp-sementara");
    expect(secret.closest("[role=status]")).not.toBeNull();
    const table = secret.closest("section")!.querySelector("table") as HTMLElement;
    expect(within(table).getAllByRole("columnheader").at(-1)).toHaveAccessibleName("Tindakan");
  });

  it("an admin gets no role select or actions on their own row (matched by id, not by display name)", async () => {
    accountServer("admin", admin, [owner, admin, viewer]);
    mount("/akun");
    await screen.findByRole("combobox", { name: "Peran untuk Pembaca Uji" });
    expect(screen.queryByRole("combobox", { name: "Peran untuk Admin Uji" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Nonaktifkan: Admin Uji" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Atur ulang kata sandi: Admin Uji" })).toBeNull();
  });

  it("asks before revoking an API key", async () => {
    const calls = accountServer("owner", owner, [owner], (c) => c.path === "/api/v1/org/api-keys/key_1/revoke" ? new Response(null, { status: 204 }) : null);
    mount("/akun");
    fireEvent.click(await screen.findByRole("button", { name: "Cabut: ERP" }));
    const dialog = await screen.findByRole("alertdialog", { name: "Cabut kunci API \"ERP\"?" });
    expect(calls.some((c) => c.path.endsWith("/revoke"))).toBe(false);
    fireEvent.click(within(dialog).getByRole("button", { name: "Cabut" }));
    await waitFor(() => expect(calls.some((c) => c.path === "/api/v1/org/api-keys/key_1/revoke")).toBe(true));
  });
});

describe("COPY-04: one message after a password change", () => {
  it("shows only 'password changed', not also 'session expired'", async () => {
    const owner = user("u_owner", "Pemilik Uji", "pemilik@uji.test", "owner");
    let ended = false;
    accountServer("owner", owner, [owner], (c) => {
      if (c.path === "/api/v1/auth/password") { ended = true; return json({ ok: true }); }
      if (ended && (c.path === "/api/v1/auth/logout" || c.path === "/api/v1/auth/whoami")) return apiErr(401, "AUTH_INVALID", "Invalid credentials.");
      return null;
    });
    mount("/akun");
    fireEvent.change(await screen.findByLabelText("Kata sandi saat ini"), { target: { value: "lama-sekali-123" } });
    fireEvent.change(screen.getByLabelText("Kata sandi baru"), { target: { value: "baru-sekali-12345" } });
    fireEvent.click(screen.getByRole("button", { name: "Ganti kata sandi" }));
    expect(await screen.findByText(/Kata sandi diganti/)).toBeInTheDocument();
    await act(async () => { await new Promise((r) => setTimeout(r, 50)); });
    expect(screen.queryByText("Sesi Anda telah berakhir. Silakan masuk kembali.")).toBeNull();
  });
});

describe("UX-06: the operations console is not named to customers", () => {
  it("uses the not-found title for a signed-in customer on /operasi", async () => {
    accountServer("owner", user("u_owner", "Pemilik Uji", "pemilik@uji.test", "owner"), []);
    mount("/operasi");
    await screen.findByText("Halaman tidak ditemukan");
    await waitFor(() => expect(document.title).toBe("Halaman tidak ditemukan · ICP Intelligence"));
  });
});

/* ---------- Wave A error codes (2026-10-20) ----------------------------------------------------- */
describe("Wave A codes get dedicated titles and messages", () => {
  it("EMAIL_UNAVAILABLE (invite) shows its own title and the Indonesian server text", () => {
    const r = errorText(new ApiError(409, "EMAIL_UNAVAILABLE", "Alamat email ini tidak dapat dipakai untuk anggota baru.", "r"), tId, "id");
    expect(r.title).toBe("Email tidak dapat dipakai");
    expect(r.body).toBe("Alamat email ini tidak dapat dipakai untuk anggota baru.");
    expect(errorText(new ApiError(409, "EMAIL_UNAVAILABLE", "Email unavailable.", "r"), tId, "id").body).toBe(tId("error.emailUnavailableBody"));
  });

  it("IDEMPOTENCY_KEY_REUSED (reports) explains a duplicate request, not a generic conflict", () => {
    const r = errorText(new ApiError(409, "IDEMPOTENCY_KEY_REUSED", "Idempotency-Key ini sudah dipakai untuk permintaan laporan yang berbeda.", "r"), tId, "id");
    expect(r.title).toBe("Permintaan yang sama sudah diterima");
    expect(r.body).toBe(tId("error.duplicateRequestBody"));
  });

  it("FEATURE_NOT_IN_PLAN on scenario/alert paths shows the server's specific sentence and the upgrade link", () => {
    render(<MemoryRouter><I18nProvider initial="id"><ErrorState error={new ApiError(403, "FEATURE_NOT_IN_PLAN", "Skenario tidak termasuk dalam paket Uji coba.", "req_f")} /></I18nProvider></MemoryRouter>);
    const alert = screen.getByRole("alert");
    expect(alert).toHaveTextContent("Fitur tidak termasuk dalam paket Anda");
    expect(alert).toHaveTextContent("Skenario tidak termasuk dalam paket Uji coba.");
    expect(within(alert).getByRole("link", { name: "Tingkatkan paket" })).toHaveAttribute("href", "/akun#paket");
  });

  it("CONFLICT on registration reads as 'try again', with the server's sentence", async () => {
    vi.stubGlobal("fetch", async (url: string) => url.includes("/auth/register")
      ? apiErr(409, "CONFLICT", "Pendaftaran bersamaan; silakan coba lagi.")
      : json({ error: { code: "NOT_FOUND", message: "x", request_id: null, details: {} } }, 404));
    mount("/daftar");
    fireEvent.change(await screen.findByLabelText("Nama organisasi"), { target: { value: "PT Uji" } });
    fireEvent.change(screen.getByLabelText("Nama lengkap"), { target: { value: "Penguji" } });
    fireEvent.change(screen.getByLabelText("Email"), { target: { value: "baru@uji.test" } });
    fireEvent.change(screen.getByLabelText("Kata sandi"), { target: { value: "panjang-sekali-123" } });
    fireEvent.click(screen.getByRole("checkbox"));
    fireEvent.click(screen.getByRole("button", { name: "Daftar" }));
    const alert = (await screen.findByText("Pendaftaran bersamaan; silakan coba lagi.")).closest("[role=alert]") as HTMLElement;
    expect(alert).toHaveTextContent("Pendaftaran belum diproses");
  });

  it("429 on a password change says the change is paused (not 'sign-in attempts')", async () => {
    const owner = user("u_owner", "Pemilik Uji", "pemilik@uji.test", "owner");
    accountServer("owner", owner, [owner], (c) => c.path === "/api/v1/auth/password"
      ? json({ error: { code: "RATE_LIMITED", message: "Terlalu banyak percobaan masuk yang gagal. Coba lagi nanti.", request_id: "req_429", details: { retry_after_seconds: 42, scope: "client" } } }, 429)
      : null);
    mount("/akun");
    fireEvent.change(await screen.findByLabelText("Kata sandi saat ini"), { target: { value: "lama-sekali-123" } });
    fireEvent.change(screen.getByLabelText("Kata sandi baru"), { target: { value: "baru-sekali-12345" } });
    fireEvent.click(screen.getByRole("button", { name: "Ganti kata sandi" }));
    const alert = (await screen.findByText("Penggantian kata sandi dijeda")).closest("[role=alert]") as HTMLElement;
    expect(alert).toHaveTextContent("coba lagi dalam 42 detik");
    // long waits read in minutes (the live throttle answered 896 s)
    expect(alert).not.toHaveTextContent(/percobaan masuk/);
  });

  it("a long password pause reads in minutes", async () => {
    const owner = user("u_owner", "Pemilik Uji", "pemilik@uji.test", "owner");
    accountServer("owner", owner, [owner], (c) => c.path === "/api/v1/auth/password"
      ? json({ error: { code: "RATE_LIMITED", message: "Terlalu banyak percobaan masuk yang gagal. Coba lagi nanti.", request_id: "req_429", details: { retry_after_seconds: 896, scope: "client" } } }, 429)
      : null);
    mount("/akun");
    fireEvent.change(await screen.findByLabelText("Kata sandi saat ini"), { target: { value: "lama-sekali-123" } });
    fireEvent.change(screen.getByLabelText("Kata sandi baru"), { target: { value: "baru-sekali-12345" } });
    fireEvent.click(screen.getByRole("button", { name: "Ganti kata sandi" }));
    const alert = (await screen.findByText("Penggantian kata sandi dijeda")).closest("[role=alert]") as HTMLElement;
    expect(alert).toHaveTextContent("coba lagi dalam 15 menit");
  });

  it("an admin sees other admins read-only; a 403 FORBIDDEN from the server gets the owner-only title", async () => {
    const owner = user("u_owner", "Pemilik Uji", "pemilik@uji.test", "owner");
    const admin = user("u_admin", "Admin Uji", "admin@uji.test", "admin");
    const admin2 = user("u_admin2", "Admin Dua", "admin2@uji.test", "admin");
    const viewer = user("u_viewer", "Pembaca Uji", "pembaca@uji.test", "viewer");
    accountServer("admin", admin, [owner, admin, admin2, viewer], (c) => c.path === "/api/v1/org/members/u_viewer" && c.method === "POST"
      ? apiErr(403, "FORBIDDEN", "Hanya pemilik organisasi yang dapat mengelola akun administrator lain.") : null);
    mount("/akun");
    await screen.findByRole("combobox", { name: "Peran untuk Pembaca Uji" });
    expect(screen.queryByRole("combobox", { name: "Peran untuk Admin Dua" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Nonaktifkan: Admin Dua" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Atur ulang kata sandi: Admin Dua" })).toBeNull();
    // Should the server still refuse (e.g. the viewer was promoted meanwhile), the message is specific.
    fireEvent.click(screen.getByRole("button", { name: "Nonaktifkan: Pembaca Uji" }));
    fireEvent.click(within(await screen.findByRole("alertdialog")).getByRole("button", { name: "Nonaktifkan" }));
    const alert = (await screen.findByText("Hanya pemilik organisasi yang dapat mengelola akun administrator lain.")).closest("[role=alert]") as HTMLElement;
    expect(alert).toHaveTextContent("Hanya pemilik organisasi");
  });

  it("the owner can still manage admins", async () => {
    const owner = user("u_owner", "Pemilik Uji", "pemilik@uji.test", "owner");
    const admin2 = user("u_admin2", "Admin Dua", "admin2@uji.test", "admin");
    accountServer("owner", owner, [owner, admin2]);
    mount("/akun");
    expect(await screen.findByRole("combobox", { name: "Peran untuk Admin Dua" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Nonaktifkan: Admin Dua" })).toBeInTheDocument();
  });
});

/* ---------- Wave B: sessions and temporary-password expiry -------------------------------------- */
describe("session management on the account page", () => {
  const owner = user("u_owner", "Pemilik Uji", "pemilik@uji.test", "owner");
  const sessions = [
    { id: "ses_cur", device: "Chrome di Windows", created_at: "2026-10-20T01:00:00Z", last_seen_at: "2026-10-20T02:02:00Z", expires_at: "2026-11-19T01:00:00Z", idle_expires_at: "2026-10-27T02:02:00Z", current: true },
    { id: "ses_old", device: "Firefox di Linux", created_at: "2026-10-18T03:00:00Z", last_seen_at: "2026-10-19T03:00:00Z", expires_at: "2026-11-17T03:00:00Z", idle_expires_at: "2026-10-26T03:00:00Z", current: false },
  ];

  it("lists sessions with headers and WIB times; the current one has no revoke action", async () => {
    accountServer("owner", owner, [owner], (c) => c.path === "/api/v1/auth/sessions" ? json(sessions) : null);
    mount("/akun");
    const table = (await screen.findByText("Firefox di Linux")).closest("table") as HTMLElement;
    expect(within(table).getAllByRole("columnheader").map((h) => h.textContent)).toEqual(["Perangkat", "Masuk", "Terakhir aktif", "Berakhir paling lambat", "Berakhir bila tidak dipakai", "Tindakan"]);
    expect(table).toHaveTextContent("20 Okt 2026, 09.02 WIB");
    const current = within(table).getByText("Chrome di Windows").closest("tr") as HTMLElement;
    expect(current).toHaveTextContent("Sesi ini");
    expect(within(current).queryByRole("button")).toBeNull();
  });

  it("ends another session only after confirmation and announces the result", async () => {
    let list = sessions;
    const calls = accountServer("owner", owner, [owner], (c) => {
      if (c.path === "/api/v1/auth/sessions") return json(list);
      if (c.path === "/api/v1/auth/sessions/ses_old/revoke") { list = sessions.filter((s) => s.current); return new Response(null, { status: 204 }); }
      return null;
    });
    mount("/akun");
    fireEvent.click(await screen.findByRole("button", { name: /^Akhiri sesi Firefox di Linux, masuk 18 Okt 2026/ }));
    const dialog = await screen.findByRole("alertdialog", { name: "Akhiri sesi Firefox di Linux?" });
    expect(calls.some((c) => c.path.endsWith("/revoke"))).toBe(false);
    fireEvent.click(within(dialog).getByRole("button", { name: "Akhiri" }));
    await waitFor(() => expect(calls.some((c) => c.path === "/api/v1/auth/sessions/ses_old/revoke" && c.method === "POST")).toBe(true));
    const live = await screen.findAllByText("Sesi Firefox di Linux diakhiri.");
    expect(live.some((el) => el.closest("[aria-live]"))).toBe(true);
    await waitFor(() => expect(screen.queryByText("Firefox di Linux")).toBeNull());
    expect(screen.getByRole("button", { name: "Keluar dari semua perangkat lain" })).toBeDisabled();
  });

  it("signs out all other devices after confirmation, with the count in a toast", async () => {
    const calls = accountServer("owner", owner, [owner], (c) => {
      if (c.path === "/api/v1/auth/sessions") return json(sessions);
      if (c.path === "/api/v1/auth/sessions/revoke-others") return json({ revoked: 3 });
      return null;
    });
    mount("/akun");
    await screen.findByText("Firefox di Linux");
    fireEvent.click(screen.getByRole("button", { name: "Keluar dari semua perangkat lain" }));
    const dialog = await screen.findByRole("alertdialog", { name: "Keluar dari semua perangkat lain?" });
    fireEvent.click(within(dialog).getByRole("button", { name: "Keluar dari semua perangkat lain" }));
    await waitFor(() => expect(calls.some((c) => c.path === "/api/v1/auth/sessions/revoke-others")).toBe(true));
    expect((await screen.findAllByText("3 sesi lain diakhiri.")).length).toBeGreaterThan(0);
  });

  it("a session that already ended (404) reads as such", () => {
    render(<MemoryRouter><I18nProvider initial="id"><ErrorState error={new ApiError(404, "NOT_FOUND", "Sesi tidak ditemukan atau sudah berakhir.", "r")} titles={{ NOT_FOUND: "Sesi sudah berakhir" }} /></I18nProvider></MemoryRouter>);
    expect(screen.getByRole("alert")).toHaveTextContent("Sesi sudah berakhir");
    expect(screen.getByRole("alert")).toHaveTextContent("Sesi tidak ditemukan atau sudah berakhir.");
  });
});

describe("temporary-password expiry", () => {
  it("shows the expiry in the forced-change flow", async () => {
    localStorage.setItem("icp.sessionToken", "icp_sess_test");
    const member = { ...user("u_m", "Anggota Baru", "baru@uji.test", "viewer"), must_change_password: true, temporary_login_expires_at: "2026-10-23T02:02:00Z" };
    vi.stubGlobal("fetch", async (url: string) => {
      const path = url.split("?")[0];
      if (path === "/api/v1/auth/whoami") return json({ authenticated: true, name: member.name, role: "viewer", kind: "user", org_id: "org_1", org_name: "PT Uji", plan: "trial", must_change_password: true, horizon_max: 3 });
      if (path === "/api/v1/auth/me") return json({ user: member, organization: null, sessions: [] });
      if (path === "/api/v1/auth/sessions") return json([]);
      return apiErr(403, "PASSWORD_CHANGE_REQUIRED", "Ganti kata sandi sementara terlebih dahulu.");
    });
    mount("/ringkasan");
    expect(await screen.findByText(/Berlaku sampai 23 Okt 2026, 09\.02 WIB\./)).toBeInTheDocument();
  });

  it("shows the expiry next to a reset temporary password", async () => {
    const owner = user("u_owner", "Pemilik Uji", "pemilik@uji.test", "owner");
    const viewer = user("u_viewer", "Pembaca Uji", "pembaca@uji.test", "viewer");
    accountServer("owner", owner, [owner, viewer], (c) => c.path === "/api/v1/org/members/u_viewer/reset-password" ? json({ temporary_password: "Icp-sementara", expires_at: "2026-10-23T02:02:00Z" }) : null);
    mount("/akun");
    fireEvent.click(await screen.findByRole("button", { name: "Atur ulang kata sandi: Pembaca Uji" }));
    fireEvent.click(within(await screen.findByRole("alertdialog")).getByRole("button", { name: "Atur ulang kata sandi" }));
    const secret = await screen.findByText("Icp-sementara");
    expect(secret.closest(".callout")).toHaveTextContent("Berlaku sampai 23 Okt 2026, 09.02 WIB.");
  });

  it("TEMP_PASSWORD_EXPIRED at sign-in has its own title", async () => {
    vi.stubGlobal("fetch", async (url: string) => url.includes("/auth/login")
      ? apiErr(403, "TEMP_PASSWORD_EXPIRED", "Kata sandi sementara sudah kedaluwarsa. Minta administrator organisasi Anda mengatur ulang kata sandi.")
      : json({ error: { code: "NOT_FOUND", message: "x", request_id: null, details: {} } }, 404));
    mount("/masuk");
    fireEvent.change(await screen.findByLabelText("Email"), { target: { value: "a@uji.test" } });
    fireEvent.change(screen.getByLabelText("Kata sandi"), { target: { value: "Icp-lama" } });
    fireEvent.click(screen.getByRole("button", { name: "Masuk" }));
    const alert = (await screen.findByText("Kata sandi sementara kedaluwarsa")).closest("[role=alert]") as HTMLElement;
    expect(alert).toHaveTextContent("Minta administrator organisasi Anda mengatur ulang kata sandi.");
    // No e-mail based recovery is offered while e-mail is disabled: the only path the page names is an
    // owner or admin resetting the password (UX-05), and nothing links to a reset page.
    expect(screen.getByLabelText("Kata sandi")).toHaveAccessibleDescription("Lupa kata sandi? Minta pemilik atau admin organisasi Anda mengatur ulang dari Akun & organisasi.");
    expect(screen.queryByRole("link", { name: /lupa|reset|atur ulang/i })).toBeNull();
  });
});

describe("Wave B/E codes", () => {
  it.each([
    [503, "EMAIL_DISABLED", "Pengiriman email belum aktif"],
    [503, "BILLING_DISABLED", "Pembayaran belum diaktifkan"],
    [400, "TOKEN_INVALID", "Tautan tidak berlaku"],
    [409, "EMAIL_ALREADY_VERIFIED", "Email sudah terverifikasi"],
    [409, "MEMBER_ALREADY_ACTIVE", "Anggota sudah aktif"],
  ])("%s %s gets the title %s", (status, code, title) => {
    const r = errorText(new ApiError(status, code, "x", "r"), tId, "id");
    expect(r.title).toBe(title);
    expect(r.body).not.toBe(tId("error.unavailableBody"));
  });

  it("switched-off services are explained without a retry button and without a price or checkout", () => {
    render(<MemoryRouter><I18nProvider initial="id"><ErrorState error={new ApiError(503, "BILLING_DISABLED", "Billing disabled", "req_b")} onRetry={() => undefined} /></I18nProvider></MemoryRouter>);
    const alert = screen.getByRole("alert");
    expect(alert).toHaveTextContent("Pembayaran belum diaktifkan di layanan ini.");
    expect(within(alert).queryByRole("button", { name: "Coba lagi" })).toBeNull();
    expect(alert.textContent).not.toMatch(/Rp|checkout|bayar sekarang/i);
  });
});
