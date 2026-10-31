// Wave F: independent data-plane check against the real Worker code (bundled with the repo's esbuild).
// Usage: node dataplane-check.mjs <path-to-bundled-worker.mjs>
const mod = await import(new URL(`file:///${process.argv[2].replace(/\\/g, "/")}`).href);
const worker = mod.default;
const SECRET = "q".repeat(48); // synthetic
const executed = [];
const DB = {
  prepare: (sql) => ({ bind: () => ({ all: async () => { executed.push(sql); return { success: true, results: [], meta: { changes: 0 } }; } }) }),
};
const env = { BACKEND_URL: "https://backend.invalid", GATEWAY_SHARED_SECRET: SECRET, ENVIRONMENT: "test", MAX_BODY_BYTES: "100000", UPSTREAM_TIMEOUT_MS: "200", DB };
const ctx = { waitUntil() {}, passThroughOnException() {} };
const cases = [
  ["DROP TABLE users", 403],
  ["CREATE TABLE x (a)", 403],
  ["ALTER TABLE users ADD COLUMN pwn TEXT", 403],
  ["PRAGMA writable_schema = 1", 403],
  ["ATTACH DATABASE 'x.db' AS y", 403],
  ["SELECT 1; DROP TABLE users", 403],
  ["sElEcT 1 /* x */ ; DeLeTe FROM users", 403],
  ["BEGIN", 403],
  ["SELECT password_hash FROM users", 200], // documented residual: DML/SELECT on any table is allowed
  ["PRAGMA table_info(\"users\")", 200],
];
const out = [];
for (const [sql, want] of cases) {
  const before = executed.length;
  const res = await worker.fetch(new Request("https://gw.test/api/internal/d1/query", {
    method: "POST", headers: { "x-gateway-secret": SECRET, "content-type": "application/json" }, body: JSON.stringify({ sql, params: [] }),
  }), env, ctx);
  out.push({ sql, status: res.status, want, reached_d1: executed.length > before, pass: res.status === want && (want !== 403 || executed.length === before) });
}
const noSecret = await worker.fetch(new Request("https://gw.test/api/internal/d1/query", {
  method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ sql: "SELECT 1", params: [] }),
}), env, ctx);
out.push({ sql: "SELECT 1 without secret", status: noSecret.status, want: 401, pass: noSecret.status === 401 });
console.log(JSON.stringify(out, null, 2));
process.exit(out.every((o) => o.pass) ? 0 : 1);
