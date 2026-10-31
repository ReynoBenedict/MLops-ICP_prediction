import { afterEach, describe, expect, it, vi } from "vitest";
import { api, ApiError, buildUrl, cacheMeta, clearSharedGets, downloadWithAuth, publicGet, publicGetMeta, setSessionToken, setToken, sharedGet, timeoutFor, TIMEOUTS } from "./client";

afterEach(() => { vi.unstubAllGlobals(); setToken(null); });

describe("api client", () => {
  it("builds same-origin URLs and drops empty query values", () => {
    expect(buildUrl("/forecasts", { limit: 10, cursor: null, status: "" })).toBe("/api/v1/forecasts?limit=10");
  });

  it("sends JSON and the admin token, and parses success", async () => {
    const fetchMock = vi.fn(async () => new Response('{"ok":true}', { headers: { "content-type": "application/json" } }));
    vi.stubGlobal("fetch", fetchMock);
    setToken("tok");
    await expect(api("/x", { method: "POST", json: { a: 1 } })).resolves.toEqual({ ok: true });
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("/api/v1/x");
    expect((init.headers as Record<string, string>).Authorization).toBe("Bearer tok");
    expect(init.body).toBe('{"a":1}');
  });

  it("sends extra headers but never lets them replace its own", async () => {
    const fetchMock = vi.fn(async () => new Response('{"ok":true}', { headers: { "content-type": "application/json" } }));
    vi.stubGlobal("fetch", fetchMock);
    setToken("tok");
    await api("/reports", {
      method: "POST", json: {},
      headers: { "Idempotency-Key": "rep_12345678", authorization: "Bearer other", "Content-Type": "text/html" },
    });
    const sent = (fetchMock.mock.calls[0] as unknown as [string, RequestInit])[1].headers as Record<string, string>;
    expect(sent["Idempotency-Key"]).toBe("rep_12345678");
    expect(sent.Authorization).toBe("Bearer tok");
    expect(sent["Content-Type"]).toBe("application/json");
    expect(Object.keys(sent).filter((k) => k.toLowerCase() === "authorization")).toHaveLength(1);
    expect(Object.keys(sent).filter((k) => k.toLowerCase() === "content-type")).toHaveLength(1);
  });

  it("forgets shared answers of a session once its credential is removed (sign-out, expiry)", async () => {
    const fetchMock = vi.fn(async () => new Response('{"ok":true}', { headers: { "content-type": "application/json" } }));
    vi.stubGlobal("fetch", fetchMock);
    setSessionToken("member-a");
    await sharedGet("/data/summary", { maxAgeMs: 60_000 });
    await sharedGet("/data/summary", { maxAgeMs: 60_000 });
    expect(fetchMock).toHaveBeenCalledTimes(1); // shared within one session
    setSessionToken(null);
    setSessionToken("member-a");
    await sharedGet("/data/summary", { maxAgeMs: 60_000 });
    expect(fetchMock).toHaveBeenCalledTimes(2); // nothing kept from before the sign-out
    setSessionToken(null);
  });

  it("maps the error envelope to ApiError with the request id", async () => {
    vi.stubGlobal("fetch", async () => new Response(
      JSON.stringify({ error: { code: "MODEL_UNAVAILABLE", message: "No model", request_id: "req_1", details: {} } }),
      { status: 503, headers: { "content-type": "application/json" } }));
    const err = await api("/forecasts").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect(err).toMatchObject({ status: 503, code: "MODEL_UNAVAILABLE", requestId: "req_1" });
  });

  it("reports network failures and non-JSON responses distinctly", async () => {
    vi.stubGlobal("fetch", async () => { throw new TypeError("offline"); });
    await expect(api("/x")).rejects.toMatchObject({ code: "NETWORK_ERROR" });
    vi.stubGlobal("fetch", async () => new Response("<html>", { status: 502, headers: { "content-type": "text/html" } }));
    await expect(api("/x")).rejects.toMatchObject({ code: "HTTP_ERROR", status: 502 });
  });
});

describe("contract C3: public cache metadata and per-request timeouts", () => {
  const snapshot = { icp_usd: 112.81 };
  const jsonResponse = (body: unknown, headers: Record<string, string> = {}) =>
    new Response(JSON.stringify(body), { headers: { "content-type": "application/json", ...headers } });

  it("reads a live public answer (no gateway cache header) as live", async () => {
    vi.stubGlobal("fetch", async () => jsonResponse(snapshot, { "x-icp-cache": "live" }));
    await expect(publicGetMeta("/public/market-snapshot")).resolves.toEqual({ data: snapshot, stale: false, cachedAt: null });
    clearSharedGets();
    vi.stubGlobal("fetch", async () => jsonResponse(snapshot));
    await expect(publicGetMeta("/public/market-snapshot")).resolves.toEqual({ data: snapshot, stale: false, cachedAt: null });
  });

  it("reads the gateway's last-known-good copy as stale with its storage time", async () => {
    vi.stubGlobal("fetch", async () => jsonResponse(snapshot, { "X-ICP-Cache": "stale", "X-ICP-Cached-At": "2026-10-24T08:15:00.000Z" }));
    await expect(publicGetMeta("/public/market-snapshot")).resolves.toEqual({ data: snapshot, stale: true, cachedAt: "2026-10-24T08:15:00.000Z" });
    // publicGet keeps returning the data only.
    clearSharedGets();
    await expect(publicGet("/public/market-snapshot")).resolves.toEqual(snapshot);
  });

  it("ignores a cached-at value that is not a timestamp, and cache headers on live answers", () => {
    expect(cacheMeta(new Headers({ "x-icp-cache": "STALE", "x-icp-cached-at": "yesterday" }))).toEqual({ stale: true, cachedAt: null });
    expect(cacheMeta(new Headers({ "x-icp-cache": "live", "x-icp-cached-at": "2026-10-24T08:15:00Z" }))).toEqual({ stale: false, cachedAt: null });
  });

  it("keeps a stale public answer only briefly, a live one for minutes", async () => {
    vi.useFakeTimers();
    try {
      const fetchMock = vi.fn(async () => jsonResponse(snapshot, { "x-icp-cache": "stale", "x-icp-cached-at": "2026-10-24T08:15:00Z" }));
      vi.stubGlobal("fetch", fetchMock);
      await publicGetMeta("/public/market-snapshot");
      await publicGetMeta("/public/market-snapshot");
      expect(fetchMock).toHaveBeenCalledTimes(1);
      vi.advanceTimersByTime(31_000);
      await publicGetMeta("/public/market-snapshot");
      expect(fetchMock).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it("chooses the timeout by endpoint", () => {
    expect(timeoutFor("/public/market-snapshot")).toBe(10_000);
    expect(timeoutFor("/plans")).toBe(10_000);
    expect(timeoutFor("/auth/whoami")).toBe(8_000);
    expect(timeoutFor("/forecasts", "POST")).toBe(60_000);
    expect(timeoutFor("/scenarios/abc123/run", "POST")).toBe(60_000);
    expect(timeoutFor("/scenarios/compare", "POST")).toBe(60_000);
    expect(timeoutFor("/reports", "POST")).toBe(95_000);
    expect(timeoutFor("/forecasts")).toBe(35_000);
    expect(timeoutFor("/reports")).toBe(35_000);
    expect(timeoutFor("/history")).toBe(35_000);
  });

  it("maps a request that outlives its timeout to TIMEOUT with the waited seconds", async () => {
    vi.useFakeTimers();
    try {
      vi.stubGlobal("fetch", (_url: string, init: RequestInit) => new Promise((_res, rej) => {
        init.signal?.addEventListener("abort", () => rej(new DOMException("aborted", "AbortError")));
      }));
      const pending = publicGetMeta("/public/market-snapshot").catch((e: unknown) => e);
      await vi.advanceTimersByTimeAsync(9_999);
      let settled = false;
      void pending.then(() => { settled = true; });
      await Promise.resolve();
      expect(settled).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      const err = await pending;
      expect(err).toBeInstanceOf(ApiError);
      expect(err).toMatchObject({ status: 0, code: "TIMEOUT", details: { timeout_seconds: 10 } });
    } finally {
      vi.useRealTimers();
    }
  });

  it("gives up on a protected download that never answers", async () => {
    vi.useFakeTimers();
    try {
      vi.stubGlobal("fetch", (_url: string, init: RequestInit) => new Promise((_res, rej) => {
        init.signal?.addEventListener("abort", () => rej(new DOMException("aborted", "AbortError")));
      }));
      const pending = downloadWithAuth("/reports/r1/download", { format: "pdf" }, "laporan.pdf").catch((e: unknown) => e);
      await vi.advanceTimersByTimeAsync(TIMEOUTS.download);
      expect(await pending).toMatchObject({ status: 0, code: "TIMEOUT", details: { timeout_seconds: 60 } });
    } finally {
      vi.useRealTimers();
    }
  });
});
