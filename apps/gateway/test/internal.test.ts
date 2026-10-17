import { afterEach, describe, expect, it, vi } from "vitest";
import worker, { secretMatches, type Env } from "../src/index";

const ctx = { waitUntil: () => undefined, passThroughOnException: () => undefined } as unknown as ExecutionContext;
const SECRET = "s".repeat(40);

/** Minimal D1 fake: records the SQL and bound params; `rows` / `fail` steer the result. */
function fakeD1(rows: Record<string, unknown>[] = [], fail?: string) {
  const calls: { sql: string; params: unknown[] }[] = [];
  const db = {
    prepare: (sql: string) => ({
      bind: (...params: unknown[]) => ({
        all: async () => {
          calls.push({ sql, params });
          if (fail) throw new Error(fail);
          return { success: true, results: rows, meta: { changes: 1, duration: 0.2 } };
        },
      }),
    }),
  } as unknown as D1Database;
  return { db, calls };
}

/** Minimal KV fake backed by a Map (values as ArrayBuffer + metadata). */
function fakeKV() {
  const store = new Map<string, { value: ArrayBuffer; metadata: unknown }>();
  const kv = {
    put: async (key: string, value: ArrayBuffer, opts?: { metadata?: unknown }) => {
      store.set(key, { value, metadata: opts?.metadata ?? null });
    },
    getWithMetadata: async (key: string, type: string) => {
      const hit = store.get(key);
      if (!hit) return { value: null, metadata: null };
      if (type === "stream") {
        return { value: new Response(hit.value).body, metadata: hit.metadata };
      }
      return { value: hit.value, metadata: hit.metadata };
    },
  } as unknown as KVNamespace;
  return { kv, store };
}

function env(overrides: Partial<Env> = {}): Env {
  return {
    BACKEND_URL: "https://backend.example",
    GATEWAY_SHARED_SECRET: SECRET,
    ENVIRONMENT: "test",
    MAX_BODY_BYTES: "100",
    ...overrides,
  };
}

const auth = { "x-gateway-secret": SECRET };

afterEach(() => vi.unstubAllGlobals());

describe("internal data plane: authentication", () => {
  it("refuses every internal route without the exact secret and never touches the bindings", async () => {
    const { db, calls } = fakeD1([{ ok: 1 }]);
    const e = env({ DB: db, OBJECTS: fakeKV().kv });
    const attempts: Record<string, string>[] = [{}, { "x-gateway-secret": "wrong" }, { "x-gateway-secret": SECRET.slice(0, 39) },
      { "x-gateway-secret": SECRET + "x" }, { authorization: `Bearer ${SECRET}` }];
    for (const headers of attempts) {
      const q = await worker.fetch(new Request("https://icp.example/api/internal/d1/query", {
        method: "POST", body: '{"sql":"SELECT 1"}', headers }), e, ctx);
      expect(q.status).toBe(401);
      const o = await worker.fetch(new Request("https://icp.example/api/internal/objects/datasets/x.csv", { headers }), e, ctx);
      expect(o.status).toBe(401);
      const h = await worker.fetch(new Request("https://icp.example/api/internal/health", { headers }), e, ctx);
      expect(h.status).toBe(401);
    }
    expect(calls).toHaveLength(0);
  });

  it("is unavailable when the Worker has no secret configured", async () => {
    const res = await worker.fetch(new Request("https://icp.example/api/internal/health", { headers: auth }),
      env({ GATEWAY_SHARED_SECRET: "" }), ctx);
    expect(res.status).toBe(503);
  });

  it("compares secrets in constant time and rejects short secrets", async () => {
    expect(await secretMatches(SECRET, SECRET)).toBe(true);
    expect(await secretMatches(SECRET + "x", SECRET)).toBe(false);
    expect(await secretMatches("short", "short")).toBe(false); // < 32 chars is never accepted
    expect(await secretMatches(null, SECRET)).toBe(false);
  });

  it("reports which bindings exist", async () => {
    const res = await worker.fetch(new Request("https://icp.example/api/internal/health", { headers: auth }),
      env({ DB: fakeD1().db }), ctx);
    expect(await res.json()).toEqual({ status: "ok", environment: "test", d1: true, kv: false });
  });

  it("bypasses the edge rate limiter and the upstream proxy", async () => {
    let upstreamCalls = 0;
    vi.stubGlobal("fetch", async () => { upstreamCalls++; return new Response("never"); });
    const deny = { limit: async () => ({ success: false }) } as unknown as RateLimit;
    const { db } = fakeD1([{ ok: 1 }]);
    const res = await worker.fetch(new Request("https://icp.example/api/internal/d1/query", {
      method: "POST", body: '{"sql":"SELECT 1 AS ok","params":[]}', headers: auth,
    }), env({ DB: db, API_RATE_LIMITER: deny, FORECAST_RATE_LIMITER: deny }), ctx);
    expect(res.status).toBe(200);
    expect(upstreamCalls).toBe(0);
  });
});

describe("internal data plane: D1", () => {
  it("runs a bound statement and answers in the D1 REST envelope", async () => {
    const { db, calls } = fakeD1([{ id: "a", n: 2 }]);
    const res = await worker.fetch(new Request("https://icp.example/api/internal/d1/query", {
      method: "POST", headers: auth,
      body: JSON.stringify({ sql: "SELECT * FROM t WHERE id = ? AND flag = ?", params: ["a", true] }),
    }), env({ DB: db }), ctx);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { success: boolean; result: { success: boolean; results: unknown[]; meta: { changes: number } }[] };
    expect(body.success).toBe(true);
    expect(body.result[0]!.results).toEqual([{ id: "a", n: 2 }]);
    expect(body.result[0]!.meta.changes).toBe(1);
    expect(calls).toEqual([{ sql: "SELECT * FROM t WHERE id = ? AND flag = ?", params: ["a", true] }]);
  });

  it("returns SQL errors (incl. constraint violations) as a failed envelope with the message", async () => {
    const { db } = fakeD1([], "D1_ERROR: UNIQUE constraint failed: dataset_versions.sha256");
    const res = await worker.fetch(new Request("https://icp.example/api/internal/d1/query", {
      method: "POST", headers: auth, body: JSON.stringify({ sql: "INSERT INTO t VALUES (?)", params: [1] }),
    }), env({ DB: db }), ctx);
    expect(res.status).toBe(400);
    const body = (await res.json()) as { success: boolean; errors: { message: string }[] };
    expect(body.success).toBe(false);
    expect(body.errors[0]!.message).toContain("UNIQUE constraint failed");
  });

  it("validates the request shape and requires POST", async () => {
    const { db, calls } = fakeD1();
    const e = env({ DB: db });
    for (const body of ["not json", "{}", '{"sql":""}', '{"sql":"SELECT 1","params":"x"}', '{"sql":"SELECT 1","params":[{"a":1}]}']) {
      const res = await worker.fetch(new Request("https://icp.example/api/internal/d1/query", { method: "POST", headers: auth, body }), e, ctx);
      expect(res.status).toBe(400);
    }
    const get = await worker.fetch(new Request("https://icp.example/api/internal/d1/query", { headers: auth }), e, ctx);
    expect(get.status).toBe(405);
    expect(calls).toHaveLength(0);
    const nodb = await worker.fetch(new Request("https://icp.example/api/internal/d1/query", {
      method: "POST", headers: auth, body: '{"sql":"SELECT 1"}' }), env(), ctx);
    expect(nodb.status).toBe(503);
  });
});

describe("internal data plane: objects (KV)", () => {
  it("stores, reports and returns objects with their content type; write-once via If-None-Match", async () => {
    const { kv, store } = fakeKV();
    const e = env({ OBJECTS: kv });
    const key = "datasets/icp/" + "a".repeat(64) + ".csv";
    const url = `https://icp.example/api/internal/objects/${key}`;
    expect((await worker.fetch(new Request(url, { method: "HEAD", headers: auth }), e, ctx)).status).toBe(404);
    expect((await worker.fetch(new Request(url, { headers: auth }), e, ctx)).status).toBe(404);

    const put = await worker.fetch(new Request(url, { method: "PUT", body: "period,value\n2026-09,112.81\n",
      headers: { ...auth, "content-type": "text/csv", "if-none-match": "*" } }), e, ctx);
    expect(put.status).toBe(201);
    expect(store.has(key)).toBe(true);

    const again = await worker.fetch(new Request(url, { method: "PUT", body: "other",
      headers: { ...auth, "content-type": "text/csv", "if-none-match": "*" } }), e, ctx);
    expect(again.status).toBe(412);
    expect(new TextDecoder().decode(store.get(key)!.value)).toContain("112.81"); // untouched

    const head = await worker.fetch(new Request(url, { method: "HEAD", headers: auth }), e, ctx);
    expect(head.status).toBe(200);
    expect(head.headers.get("content-type")).toBe("text/csv");
    const get = await worker.fetch(new Request(url, { headers: auth }), e, ctx);
    expect(get.status).toBe(200);
    expect(get.headers.get("content-type")).toBe("text/csv");
    expect(get.headers.get("cache-control")).toBe("no-store");
    expect(await get.text()).toContain("2026-09,112.81");

    const overwrite = await worker.fetch(new Request(url, { method: "PUT", body: "replaced",
      headers: { ...auth, "content-type": "text/plain" } }), e, ctx);
    expect(overwrite.status).toBe(201);
    expect(await (await worker.fetch(new Request(url, { headers: auth }), e, ctx)).text()).toBe("replaced");
  });

  it("stores large binary payloads unchanged (above the proxy body limit)", async () => {
    const { kv } = fakeKV();
    const e = env({ OBJECTS: kv, MAX_BODY_BYTES: "10" });
    const bytes = new Uint8Array(5000).map((_, i) => i % 251);
    const url = "https://icp.example/api/internal/objects/models/bundles/5/model.skops";
    const put = await worker.fetch(new Request(url, { method: "PUT", body: bytes,
      headers: { ...auth, "content-type": "application/octet-stream" } }), e, ctx);
    expect(put.status).toBe(201);
    const got = new Uint8Array(await (await worker.fetch(new Request(url, { headers: auth }), e, ctx)).arrayBuffer());
    expect(got).toEqual(bytes);
  });

  it("rejects invalid keys, unsupported methods and missing bindings", async () => {
    const { kv, store } = fakeKV();
    const e = env({ OBJECTS: kv });
    for (const bad of ["../etc", "a//b", "trailing/", "%2e%2e/x", ".hidden"]) {
      const res = await worker.fetch(new Request(`https://icp.example/api/internal/objects/${bad}`, {
        method: "PUT", body: "x", headers: auth }), e, ctx);
      // URL normalisation turns "../etc" into a non-internal path (404); anything that reaches the
      // handler with a bad key is refused (400). Either way nothing is written.
      expect([400, 404]).toContain(res.status);
    }
    expect(store.size).toBe(0);
    const del = await worker.fetch(new Request("https://icp.example/api/internal/objects/a/b", { method: "DELETE", headers: auth }), e, ctx);
    expect(del.status).toBe(405);
    const none = await worker.fetch(new Request("https://icp.example/api/internal/objects/", { headers: auth }), e, ctx);
    expect(none.status).toBe(404);
    const nokv = await worker.fetch(new Request("https://icp.example/api/internal/objects/a/b", { headers: auth }), env(), ctx);
    expect(nokv.status).toBe(503);
  });
});
