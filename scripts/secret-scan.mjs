// Secret scan of source files and build output (no external tool required). Exit 1 on findings.
//
// - Flags provider-specific credential formats and quoted/assigned high-entropy values next to a
//   credential-like name. Placeholders (`<...>`, `${...}`, `change-me`, `example`, synthetic test
//   values) are exempt per *value*, never per line, so a real value on a line that also contains
//   markup is still reported.
// - Scans the built web app and Worker bundles (apps/*/dist) because a leaked value would ship there.
// - Local secret files (.env, .env.*, .dev.vars*) are not opened; they must be gitignored instead.
// - Reports only `file:line: rule`, never the matched value.
// - `--self-test` checks every rule against synthetic samples (assembled at run time, so this file
//   contains no literal credential) and is also run before every scan.
// Limits: no Git history scan (use a dedicated scanner such as gitleaks in CI for that).
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";
import { ROOT } from "./lib.mjs";

const SKIP_DIRS = new Set([".git", "node_modules", ".venv", "venv", "var", ".wrangler", "legacy", "data", ".dvc",
  "__pycache__", ".pytest_cache", ".ruff_cache", ".mypy_cache", ".idea", "notebooks", "mlruns", "mlartifacts",
  ".secvenv", "htmlcov"]);
// `dist` is skipped except the deployable bundles below.
const SCAN_DIST = new Set([path.join("apps", "web", "dist"), path.join("apps", "gateway", "dist")]);
const EXTS = new Set([".py", ".ts", ".tsx", ".js", ".mjs", ".cjs", ".json", ".jsonc", ".toml", ".yaml", ".yml", ".md",
  ".html", ".css", ".sql", ".txt", ".example", ".cfg", ".ini", ".sh", ".ps1", ".bat", ".pem", ".key", ".env", ".map",
  ".svg", ".conf", ""]);
const LOCAL_SECRET_FILE = /^(\.env(\..+)?|\.dev\.vars.*)$/;
const LOCAL_SECRET_EXAMPLE = /^(\.env\.example|\.dev\.vars\.example)$/;
const MAX_BYTES = 5_000_000;

// [label, regex, group-with-value (0 = whole match), needsEntropy]
const RULES = [
  ["AWS access key id", /\b(?:AKIA|ASIA)[0-9A-Z]{16}\b/, 0, false],
  ["Private key block", /-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED |PGP )?PRIVATE KEY( BLOCK)?-----/, 0, false],
  ["GitHub token", /\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{50,})\b/, 0, false],
  ["Slack token", /\bxox[baprs]-[A-Za-z0-9-]{10,}/, 0, false],
  ["Stripe secret/restricted key", /\b(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{16,}\b/, 0, false],
  ["Stripe webhook secret", /\bwhsec_[A-Za-z0-9]{24,}\b/, 0, false],
  ["Midtrans server key", /\b(?:SB-)?Mid-server-[A-Za-z0-9_-]{16,}/, 0, false],
  ["Xendit secret key", /\bxnd_(?:production|development)_[A-Za-z0-9]{20,}\b/, 0, false],
  ["Google API key", /\bAIza[0-9A-Za-z_-]{35}\b/, 0, false],
  ["npm token", /\bnpm_[A-Za-z0-9]{36}\b/, 0, false],
  ["Docker Hub token", /\bdckr_pat_[A-Za-z0-9_-]{20,}/, 0, false],
  ["Render API key", /\brnd_[A-Za-z0-9]{24,}\b/, 0, false],
  ["Resend API key", /\bre_[A-Za-z0-9]{8,}_[A-Za-z0-9]{16,}\b/, 0, false],
  ["SendGrid API key", /\bSG\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}/, 0, false],
  ["JSON Web Token", /\beyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}/, 0, false],
  ["Cloudflare API token/key assignment",
    /\b(?:CLOUDFLARE|CF)_[A-Z0-9_]*(?:TOKEN|KEY|SECRET)\b["']?\s*[:=]\s*["']?([A-Za-z0-9_-]{32,})/, 1, true],
  ["Connection string with password", /\b(?:postgres(?:ql)?|mysql|mongodb(?:\+srv)?|redis|amqp)(?:\+\w+)?:\/\/[^:\s/<"'@]+:([^@\s<>"']{6,})@/, 1, false],
  // Credential-like NAME followed by a quoted literal (any language) ...
  ["Credential assigned a quoted literal",
    /(?:secret|token|passw(?:or)?d|pwd|api[_-]?key|private[_-]?key|access[_-]?key|server[_-]?key|client[_-]?secret|pepper)\w*["']?\s*(?::|=|=>|:=)\s*[rbuf]?["'`]([^"'`\s]{20,})["'`]/i, 1, true],
  // ... or an unquoted value in KEY=VALUE / KEY: VALUE files (env, yaml, ini, Dockerfile).
  ["Credential assigned an unquoted value",
    /^\s*(?:export\s+|ENV\s+|ARG\s+|-\s+)?[A-Z0-9_]*(?:SECRET|TOKEN|PASSWORD|PASSWD|API_KEY|PRIVATE_KEY|ACCESS_KEY|SERVER_KEY|PEPPER)[A-Z0-9_]*\s*[:=]\s*([^\s"'#`]{20,})\s*$/, 1, true],
];

// Values that are clearly not credentials. Applied to the matched VALUE only.
const PLACEHOLDER = [
  /[<>]/, /\$\{?[A-Za-z_]/, /\{[A-Za-z_][\w.]*\}/, /\{\{.*\}\}/, /%[A-Z_]+%/, /^\*+$/,
  /change[-_]?me/i, /example/i, /placeholder/i, /your[-_]/i, /redacted/i, /x{6,}/i, /\.{3}/,
  /^test-(?:admin|gateway|metrics|token|secret)-/,
  /^(?:process\.env|os\.environ|os\.getenv|env\.|settings\.|self\.|args\.)/,
];
// Self-describing synthetic credentials ("...-server-FAKE-TEST-KEY-not-a-secret") are exempt ONLY in test
// files (tests/ or test/ directories, *.test.*, test_*.py, conftest.py). Anywhere else the same value is
// reported, so a real key cannot be smuggled into application code or docs by adding "fake" to it.
const TEST_ONLY_PLACEHOLDER = [
  /^(?:synthetic|fake|sample|dummy)[-_]/i, /(?:^|[-_])(?:fake|dummy)(?:[-_]|$)/i, /not[-_]a[-_]secret/i,
];
export function isTestFile(rel) {
  const p = rel.replace(/\\/g, "/");
  return /(^|\/)(tests?|__tests__)\//.test(p) || /\.test\.[cm]?[jt]sx?$/.test(p) || /(^|\/)(test_[^/]*\.py|conftest\.py)$/.test(p);
}
const MIN_ENTROPY = 3.3; // bits per character; identifiers like "secrets.token_urlsafe" score lower

function entropy(s) {
  const counts = new Map();
  for (const ch of s) counts.set(ch, (counts.get(ch) ?? 0) + 1);
  let h = 0;
  for (const n of counts.values()) {
    const p = n / s.length;
    h -= p * Math.log2(p);
  }
  return h;
}

/** Labels of the rules that report `line` (values are never returned). */
export function scanLine(line, testFile = false) {
  const hits = [];
  for (const [label, re, group, needsEntropy] of RULES) {
    const m = re.exec(line);
    if (!m) continue;
    const value = m[group] ?? m[0];
    if (PLACEHOLDER.some((p) => p.test(value))) continue;
    if (testFile && TEST_ONLY_PLACEHOLDER.some((p) => p.test(value))) continue;
    if (needsEntropy) {
      // Identifier-like values (dotted/called names, snake_case words) are code, not secrets.
      if (/^[A-Za-z_][\w.]*\(?$/.test(value) && !/\d/.test(value)) continue;
      // Lower-case word lists without digits (event names, i18n keys, routes) are not secrets.
      if (/^[a-z][a-z.:_/-]*$/.test(value)) continue;
      if (entropy(value) < MIN_ENTROPY) continue;
    }
    hits.push(label);
  }
  return hits;
}

function selfTest() {
  const j = (...parts) => parts.join("");
  const r = (n, alphabet = "aB3dE5gH7jK9mN1pQ") => Array.from({ length: n }, (_, i) => alphabet[(i * 7 + 3) % alphabet.length]).join("");
  const must = [
    ["AWS access key id", j("AKIA", "Z7Q4", "MNOP", "ABCD", "EF23")],
    ["Private key block", j("-----BEGIN ", "RSA PRIVATE KEY-----")],
    ["GitHub token", j("ghp", "_", r(36))],
    ["Stripe secret/restricted key", j("const k = 'sk", "_live_", r(24), "'")],
    ["Stripe webhook secret", j("whsec", "_", r(32))],
    ["Midtrans server key", j("SB-Mid", "-server-", r(24))],
    ["Xendit secret key", j("xnd", "_production_", r(40))],
    ["Google API key", j("AIza", r(35))],
    ["npm token", j("npm", "_", r(36))],
    ["Docker Hub token", j("dckr", "_pat_", r(27))],
    ["Render API key", j("rnd", "_", r(32))],
    ["JSON Web Token", j("ey", "J", r(20), ".ey", "J", r(20), ".", r(30))],
    ["Cloudflare API token/key assignment", j("CLOUDFLARE", "_API_TOKEN=", r(40))],
    ["Connection string with password", j("postgres", "ql://icp:", r(12), "@db.internal:5432/icp")],
    ["Credential assigned a quoted literal", j("gateway_", "secret = \"", r(40), "\"")],
    // a real value next to markup must still be reported (the old scanner exempted the whole line)
    ["Credential assigned a quoted literal", j("<Field token=\"", r(40), "\" />")],
    ["Credential assigned an unquoted value", j("METRICS", "_TOKEN=", r(40))],
  ];
  const mustNot = [
    "GATEWAY_SHARED_SECRET=<generate with icp-api new-token>",
    "BACKEND_URL: \"https://<staging-backend-origin>\"",
    "token = secrets.token_urlsafe(32)",
    "ADMIN_TOKEN = \"test-admin-token-0123456789abcdefghijklmnop\"",
    "password_hash = hash_password(password)",
    "METRICS_TOKEN=${METRICS_TOKEN}",
    "GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_ADMIN_PASSWORD:-change-me-local-only}",
    "const token = sessionStorage.getItem(\"icp.operatorToken\");",
    "API_KEY_PREFIX_LENGTH = 12",
    "export const PASSWORD_CHANGE_EVENT = \"icp:password-change-required\";",
    "\"ADMIN_API_TOKENS\": f\"tester:{ADMIN_TOKEN}\",",
  ];
  // Self-describing fake keys: exempt in test files only, reported everywhere else.
  const fakeKeys = [
    ["FAKE_KEY = \"SB-Mid", "-server-FAKE-TEST-KEY-not-a-secret\""].join(""),
    ["setenv(\"MIDTRANS_SERVER_KEY\", \"Mid", "-server-FAKE-production-looking\")"].join(""),
    ["key = \"SB-Mid", "-server-", r(24), "-fake\""].join(""),
  ];
  const failures = [];
  for (const [label, sample] of must) {
    if (!scanLine(sample).includes(label)) failures.push(`missed: ${label}`);
    if (!scanLine(sample, true).includes(label)) failures.push(`missed in a test file: ${label}`);
  }
  for (const sample of fakeKeys) {
    if (scanLine(sample, true).length) failures.push(`false positive on a synthetic key in a test file #${fakeKeys.indexOf(sample) + 1}`);
    if (!scanLine(sample, false).includes("Midtrans server key")) failures.push(`synthetic-looking key not reported outside tests #${fakeKeys.indexOf(sample) + 1}`);
  }
  const testPaths = [["backend/tests/test_x.py", true], ["apps/web/src/a.test.tsx", true], ["backend/tests/conftest.py", true],
    ["backend/icp_api/billing/config.py", false], ["docs/BILLING.md", false], ["apps/web/src/latest.ts", false]];
  for (const [p, expected] of testPaths) if (isTestFile(p) !== expected) failures.push(`test-file detection wrong for ${p}`);
  for (const sample of mustNot) {
    const hits = scanLine(sample);
    if (hits.length) failures.push(`false positive (${hits.join(", ")}) on a placeholder sample #${mustNot.indexOf(sample) + 1}`);
  }
  return failures;
}

function scanTree() {
  const findings = [];
  const walk = (dir) => {
    for (const name of readdirSync(dir)) {
      const full = path.join(dir, name);
      const rel = path.relative(ROOT, full);
      let st;
      try {
        st = statSync(full);
      } catch {
        continue;
      }
      if (st.isDirectory()) {
        if (SKIP_DIRS.has(name)) continue;
        if (name === "dist" && !SCAN_DIST.has(rel)) continue;
        walk(full);
        continue;
      }
      if (LOCAL_SECRET_FILE.test(name) && !LOCAL_SECRET_EXAMPLE.test(name)) continue; // never opened; gitignore checked below
      const ext = path.extname(name);
      if (!EXTS.has(ext) && !LOCAL_SECRET_EXAMPLE.test(name)) continue;
      if (st.size > MAX_BYTES) continue;
      const testFile = isTestFile(rel);
      readFileSync(full, "utf8").split(/\r?\n/).forEach((line, i) => {
        if (line.length > 20_000) line = line.slice(0, 20_000); // minified bundles: bounded work per line
        for (const label of scanLine(line, testFile)) findings.push(`${rel}:${i + 1}: ${label}`);
      });
    }
  };
  walk(ROOT);

  // Local secret files must be gitignored.
  const gitignore = existsSync(path.join(ROOT, ".gitignore")) ? readFileSync(path.join(ROOT, ".gitignore"), "utf8") : "";
  const lines = gitignore.split(/\r?\n/).map((l) => l.trim());
  for (const [required, alternatives] of [[".env", [".env"]], [".env.*", [".env.*"]], [".dev.vars", [".dev.vars", ".dev.vars*"]]]) {
    if (!alternatives.some((a) => lines.includes(a))) findings.push(`.gitignore does not ignore ${required}`);
  }
  return findings;
}

const failures = selfTest();
if (failures.length) {
  console.error("Secret scan self-test failed:\n" + failures.map((f) => `  ${f}`).join("\n"));
  process.exit(1);
}
if (process.argv.includes("--self-test")) {
  console.log(`Secret scan self-test: ${RULES.length} rules, all samples behave as expected.`);
  process.exit(0);
}
const findings = scanTree();
if (findings.length) {
  console.error("Potential secrets found (values not shown):\n" + findings.map((f) => `  ${f}`).join("\n"));
  process.exit(1);
}
console.log("Secret scan: no findings.");
