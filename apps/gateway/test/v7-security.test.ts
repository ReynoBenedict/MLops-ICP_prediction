/**
 * v7 security regressions for the gateway Worker (audit findings SEC-01, SEC-02, SEC-09, SEC-10,
 * OPS-04): behaviour the pre-v7 Worker did not have (client-IP header, IP bucket for junk
 * credentials, headers on its own error replies, SQL allow-list, secret rotation).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import worker, { anySecretMatches, CLIENT_IP_HEADER, sqlRejection, type Env } from "../src/index";

const ctx = { waitUntil: () => undefined, passThroughOnException: () => undefined } as unknown as ExecutionContext;
const SECRET = "s".repeat(40);
const OLD_SECRET = "o".repeat(40);
const SECURITY_HEADERS = {
  "x-content-type-options": "nosniff",
  "x-frame-options": "DENY",
  "content-security-policy": "default-src 'none'; frame-ancestors 'none'",
  "cross-origin-resource-policy": "same-origin",
  "referrer-policy": "no-referrer",
};

function env(overrides: Partial<Env> = {}): Env {
  return {
    BACKEND_URL: "https://backend.example",
    GATEWAY_SHARED_SECRET: SECRET,
    ENVIRONMENT: "test",
    MAX_BODY_BYTES: "100",
    UPSTREAM_TIMEOUT_MS: "200",
    ...overrides,
  };
}

function mockUpstream(handler: (req: Request) => Response) {
  const calls: Request[] = [];
  vi.stubGlobal("fetch", async (input: Request) => {
    calls.push(input);
    return handler(input);
  });
  return calls;
}

function fakeD1() {
  const calls: string[] = [];
  const db = {
    prepare: (sql: string) => ({
      bind: () => ({
        all: async () => {
          calls.push(sql);
          return { success: true, results: [], meta: { changes: 0, duration: 0.1 } };
        },
      }),
    }),
  } as unknown as D1Database;
  return { db, calls };
}

afterEach(() => vi.unstubAllGlobals());

describe("SEC-01 client IP header", () => {
  it("sends the edge's client IP in X-ICP-Client-IP and never forwards a client-supplied copy", async () => {
    const calls = mockUpstream(() => new Response("{}"));
    await worker.fetch(new Request("https://icp.example/api/v1/data/summary", {
      headers: { "cf-connecting-ip": "203.0.113.7", [CLIENT_IP_HEADER]: "198.51.100.66", "x-forwarded-for": "10.0.0.1" },
    }), env(), ctx);
    const up = calls[0]!;
    expect(up.headers.get(CLIENT_IP_HEADER)).toBe("203.0.113.7");
    expect(up.headers.get("cf-connecting-ip")).toBe("203.0.113.7");
    expect(up.headers.get("x-forwarded-for")).toBeNull();
  });
});

describe("SEC-02 edge limiter keys", () => {
  it("always applies the client-IP bucket, so rotating junk Authorization headers share one bucket", async () => {
    mockUpstream(() => new Response("ok"));
    const keys: string[] = [];
    const allow = { limit: async ({ key }: { key: string }) => { keys.push(key); return { success: true }; } } as unknown as RateLimit;
    const e = env({ API_RATE_LIMITER: allow });
    for (const junk of ["Bearer a", "Bearer b", "Bearer icp_sess_short", "Basic xyz"]) {
      await worker.fetch(new Request("https://icp.example/api/v1/org", { headers: { authorization: junk, "cf-connecting-ip": "198.51.100.9" } }), e, ctx);
    }
    expect(new Set(keys)).toEqual(new Set(["api:ip:198.51.100.9"]));
  });

  it("adds a per-credential bucket for well-formed session tokens and API keys, and refuses when either is exhausted", async () => {
    mockUpstream(() => new Response("ok"));
    const keys: string[] = [];
    const denyCredential = {
      limit: async ({ key }: { key: string }) => { keys.push(key); return { success: !key.startsWith("api:auth:") }; },
    } as unknown as RateLimit;
    const token = "Bearer icp_sess_" + "A".repeat(43);
    const res = await worker.fetch(new Request("https://icp.example/api/v1/org", { headers: { authorization: token, "cf-connecting-ip": "198.51.100.10" } }),
      env({ API_RATE_LIMITER: denyCredential }), ctx);
    expect(res.status).toBe(429);
    expect(keys[0]).toBe("api:ip:198.51.100.10");
    expect(keys[1]).toMatch(/^api:auth:[0-9a-f]{24}$/);
    expect(keys.join()).not.toContain("icp_sess_");
  });
});

describe("SEC-09 security headers on the Worker's own replies", () => {
  it("adds them to 404, 405, 413, 429, 502, 503 and 504 replies", async () => {
    const deny = { limit: async () => ({ success: false }) } as unknown as RateLimit;
    const replies: Response[] = [];
    replies.push(await worker.fetch(new Request("https://icp.example/api/unknown"), env(), ctx)); // 404
    replies.push(await worker.fetch(new Request("https://icp.example/api/v1/models", { method: "DELETE" }), env(), ctx)); // 405
    replies.push(await worker.fetch(new Request("https://icp.example/api/v1/forecasts", { method: "POST", body: "x".repeat(500) }), env(), ctx)); // 413
    replies.push(await worker.fetch(new Request("https://icp.example/api/v1/models"), env({ API_RATE_LIMITER: deny }), ctx)); // 429
    replies.push(await worker.fetch(new Request("https://icp.example/api/v1/models"), env({ BACKEND_URL: "" }), ctx)); // 503
    mockUpstream(() => new Response(null, { status: 302, headers: { location: "https://elsewhere" } }));
    replies.push(await worker.fetch(new Request("https://icp.example/api/v1/models"), env(), ctx)); // 502
    vi.stubGlobal("fetch", async () => { throw new Error("down"); });
    replies.push(await worker.fetch(new Request("https://icp.example/api/v1/models"), env(), ctx)); // 504
    expect(replies.map((r) => r.status)).toEqual([404, 405, 413, 429, 503, 502, 504]);
    for (const r of replies) {
      for (const [name, value] of Object.entries(SECURITY_HEADERS)) expect(r.headers.get(name), name).toBe(value);
      expect(r.headers.get("strict-transport-security")).toContain("max-age=");
      expect(r.headers.get("cache-control")).toBe("no-store");
    }
  });
});

describe("SEC-10 data-plane statement allow-list", () => {
  it("allows the statement kinds the backend issues", () => {
    for (const sql of [
      "SELECT * FROM users WHERE email = ?",
      "  select 1 as ok",
      "INSERT OR IGNORE INTO \"jobs\" (id) VALUES (?)",
      "INSERT INTO usage_events (id) SELECT ? WHERE (SELECT COUNT(*) FROM usage_events) < ?",
      "UPDATE users SET name = CASE WHEN ? THEN ? ELSE name END WHERE id = ?",
      "DELETE FROM organizations WHERE id = ? AND NOT EXISTS (SELECT 1 FROM users WHERE org_id = ?)",
      "WITH x AS (SELECT 1) SELECT * FROM x",
      "PRAGMA table_info(\"users\")",
      "SELECT 'DROP TABLE users; --' AS literal",
      "SELECT * FROM jobs WHERE status = 'rollback';",
      "SELECT created_at, dropped FROM t",
    ]) {
      expect(sqlRejection(sql), sql).toBeNull();
    }
  });

  it("refuses DDL, ATTACH, PRAGMA writes, transactions and stacked statements", () => {
    for (const sql of [
      "DROP TABLE users",
      "CREATE TABLE x (a)",
      "ALTER TABLE users ADD COLUMN x",
      "ATTACH DATABASE 'file.db' AS x",
      "PRAGMA foreign_keys=OFF",
      "PRAGMA writable_schema = 1",
      "VACUUM",
      "BEGIN",
      "SELECT 1; DROP TABLE users",
      "SELECT 1; SELECT 2",
      "WITH x AS (SELECT 1) SELECT 1; DELETE FROM users",
      "/* c */ DROP TABLE users",
      "SELECT 1 /* ; */ ; ATTACH 'x' AS y",
      "REPLACE INTO users VALUES (1)",
    ]) {
      expect(sqlRejection(sql), sql).not.toBeNull();
    }
  });

  it("answers 403 without touching D1", async () => {
    const { db, calls } = fakeD1();
    const res = await worker.fetch(new Request("https://icp.example/api/internal/d1/query", {
      method: "POST", headers: { "x-gateway-secret": SECRET }, body: JSON.stringify({ sql: "DROP TABLE users", params: [] }),
    }), env({ DB: db }), ctx);
    expect(res.status).toBe(403);
    expect(res.headers.get("x-content-type-options")).toBe("nosniff");
    expect(calls).toHaveLength(0);
  });
});

describe("OPS-04 gateway secret rotation", () => {
  it("accepts the current and any listed previous secret on internal routes, nothing else", async () => {
    const e = env({ GATEWAY_ACCEPTED_SECRETS: ` ${OLD_SECRET} ,` });
    expect(await anySecretMatches(SECRET, e)).toBe(true);
    expect(await anySecretMatches(OLD_SECRET, e)).toBe(true);
    expect(await anySecretMatches("x".repeat(40), e)).toBe(false);
    expect(await anySecretMatches("", e)).toBe(false);
    expect(await anySecretMatches(OLD_SECRET, env())).toBe(false);
    const ok = await worker.fetch(new Request("https://icp.example/api/internal/health", { headers: { "x-gateway-secret": OLD_SECRET } }), e, ctx);
    expect(ok.status).toBe(200);
    const no = await worker.fetch(new Request("https://icp.example/api/internal/health", { headers: { "x-gateway-secret": OLD_SECRET } }), env(), ctx);
    expect(no.status).toBe(401);
  });

  it("still sends only the current secret to the backend", async () => {
    const calls = mockUpstream(() => new Response("{}"));
    await worker.fetch(new Request("https://icp.example/api/v1/models"), env({ GATEWAY_ACCEPTED_SECRETS: OLD_SECRET }), ctx);
    expect(calls[0]!.headers.get("x-gateway-secret")).toBe(SECRET);
  });
});

describe("Wave C requests: idempotency header, D1 parameter cap, export TTL", () => {
  it("forwards Idempotency-Key and passes Idempotent-Replayed back (DATA-02)", async () => {
    const calls = mockUpstream(() => new Response("{}", { status: 201, headers: { "idempotent-replayed": "true" } }));
    const res = await worker.fetch(new Request("https://icp.example/api/v1/forecasts", {
      method: "POST", body: "{}", headers: { "idempotency-key": "fc-retry-0001", "content-type": "application/json" },
    }), env({ MAX_BODY_BYTES: "1000" }), ctx);
    expect(calls[0]!.headers.get("idempotency-key")).toBe("fc-retry-0001");
    expect(res.headers.get("idempotent-replayed")).toBe("true");
  });

  it("caps bound parameters at the D1 limit of 100 (DATA-05)", async () => {
    const { db, calls } = fakeD1();
    const post = (n: number) => worker.fetch(new Request("https://icp.example/api/internal/d1/query", {
      method: "POST", headers: { "x-gateway-secret": SECRET },
      body: JSON.stringify({ sql: "SELECT 1", params: Array.from({ length: n }, (_, i) => i) }),
    }), env({ DB: db }), ctx);
    expect((await post(100)).status).toBe(200);
    const over = await post(101);
    expect(over.status).toBe(400);
    expect(((await over.json()) as { success: boolean }).success).toBe(false);
    expect(calls).toHaveLength(1);
  });

  it("stores report and forecast exports in KV with the documented retention; other objects never expire (DATA-03)", async () => {
    const puts: { key: string; opts: { expirationTtl?: number } | undefined }[] = [];
    const kv = {
      put: async (key: string, _v: ArrayBuffer, opts?: { expirationTtl?: number }) => { puts.push({ key, opts }); },
      getWithMetadata: async () => ({ value: null, metadata: null }),
    } as unknown as KVNamespace;
    for (const key of ["exports/forecasts/2026/10/fc_1.csv", "exports/reports/2026/10/rpt_1.pdf", "datasets/icp/abc.csv"]) {
      const res = await worker.fetch(new Request(`https://icp.example/api/internal/objects/${key}`, {
        method: "PUT", headers: { "x-gateway-secret": SECRET, "content-type": "text/csv" }, body: "a,b",
      }), env({ OBJECTS: kv }), ctx);
      expect(res.status).toBe(201);
    }
    expect(puts.map((p) => p.opts?.expirationTtl)).toEqual([180 * 86_400, 365 * 86_400, undefined]);
  });
});
