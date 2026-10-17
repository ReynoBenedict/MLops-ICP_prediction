import { afterEach, describe, expect, it, vi } from "vitest";
import { api, ApiError, buildUrl, setToken } from "./client";

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
