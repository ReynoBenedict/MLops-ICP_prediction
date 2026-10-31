#!/usr/bin/env node
/**
 * Staging backend on Vercel Hobby (free plan, no card): build the deploy folder and, when asked, deploy it.
 *
 *   node scripts/vercel-backend.mjs                       # build var/deploy/vercel-backend/ and print its size
 *                                                         # (local only: no network, no account)
 *   node scripts/vercel-backend.mjs --deploy --dry-run    # build, check the env file, copy the folder to the
 *                                                         # stage directory, print the remote steps
 *                                                         # (variable NAMES only); nothing remote runs
 *   node scripts/vercel-backend.mjs --deploy              # build, stage, link/create the Vercel project, check
 *                                                         # it has no Git connection, set its Production
 *                                                         # variables, deploy to production, check /health
 *   node scripts/vercel-backend.mjs --deploy --skip-env   # redeploy without touching the variables
 *   node scripts/vercel-backend.mjs --deploy --bundle full
 *                                                         # keep every dependency inside the function (Vercel's
 *                                                         # large-functions path, beta) instead of Vercel's
 *                                                         # default install at each cold start (docs/DEPLOYMENT.md)
 *   node scripts/vercel-backend.mjs --runtime-log         # read-only: the cold-start install lines of the last hour
 *                                                         # of production runtime logs (Hobby keeps 1 hour)
 *   node scripts/vercel-backend.mjs --write-gateway-url [<url>]
 *                                                         # put the production domain (default: the one saved
 *                                                         # by the last deploy) into env.staging.vars.BACKEND_URL
 *                                                         # of apps/gateway/wrangler.jsonc
 * Options: --project <name> (default icp-api-staging), --env-file <path> (default
 * var/secrets/staging.vercel.env), --token-file <path> (default var/secrets/vercel.token), --cli-login (use the
 * CLI's own stored `vercel login` instead of a token file), --stage-dir <path>
 * (default %LOCALAPPDATA%\icp-vercel-backend on Windows, else ~/.cache/icp-vercel-backend), --bundle
 * runtime-install|full (default: the mode of the last successful deploy, kept in var/deploy/vercel-bundle-mode.txt;
 * without one, runtime-install, Vercel's standard path).
 *
 * The Vercel CLI never runs inside the repository. The deploy folder is copied to a stage directory outside any
 * Git work tree (the script refuses one inside a work tree, and empties only a directory it marked itself or an
 * earlier stage copy), and the CLI runs there with Git discovery fenced at that directory (GIT_CEILING_DIRECTORIES). So `vercel link --yes` finds no repository to connect the new
 * project to (it would otherwise connect the GitHub `origin` and build on every push), and no Git command of the
 * CLI touches the repository. After linking, the script also checks that the project has no Git connection.
 *
 * Secrets never leave the files they live in except towards Vercel: the token is handed to the CLI in its
 * VERCEL_TOKEN environment variable (Vercel's recommendation: unlike --token it is not visible in the process
 * list), each variable's value is written to the CLI's stdin (`vercel env add`), and nothing secret is printed or
 * logged. `vercel link` itself writes a short-lived VERCEL_OIDC_TOKEN to <stage>/.env.local; the script deletes it
 * right after linking and again when it exits (.vercelignore would keep it out of the upload anyway). The deploy command's JSON answer
 * (deployment metadata, or the error) is saved to var/deploy/vercel-deploy-last.json with secret values redacted.
 * Before anything is linked or set, the script checks that the CLI's current scope is on the Hobby plan (free; it has
 * no payment method, so nothing can be billed) and stops otherwise.
 * The CLI version is pinned (VERCEL_CLI). See docs/DEPLOYMENT.md ("Backend on Vercel Hobby").
 */
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { cpSync, existsSync, mkdirSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const IS_WINDOWS = process.platform === "win32";
/** Pinned CLI: published 2026-10-09, i.e. at least two weeks old when this script was written (2026-10-27). */
export const VERCEL_CLI = "vercel@63.1.2";
const DEFAULT_PROJECT = "icp-api-staging";
const DEPLOY_ROOT = path.join(ROOT, "var", "deploy");
const OUT = path.join(DEPLOY_ROOT, "vercel-backend");
const MANIFEST = path.join(DEPLOY_ROOT, "vercel-backend.manifest.json");
const DEPLOY_LOG = path.join(DEPLOY_ROOT, "vercel-deploy-last.json");
/** Bundle mode of the last successful deploy: the default for the next one, so a redeploy keeps the packing. */
const BUNDLE_MODE_FILE = path.join(DEPLOY_ROOT, "vercel-bundle-mode.txt");
const URL_FILE = path.join(ROOT, "var", "vercel-backend-url.txt");
const WRANGLER = path.join(ROOT, "apps", "gateway", "wrangler.jsonc");

/** What the function needs, in the repository layout (icp_api.settings.PROJECT_ROOT = bundle root). */
const TREES = [
  ["backend/icp_api", "backend/icp_api"],
  ["pipeline/icp_ml", "pipeline/icp_ml"],
];
const FILES = [
  ["infra/vercel/index.py", "index.py"],
  ["infra/vercel/requirements.txt", "requirements.txt"],
  ["infra/vercel/.python-version", ".python-version"],
  ["infra/vercel/vercel.json", "vercel.json"],
  ["pipeline/configs/training.yaml", "pipeline/configs/training.yaml"], // ModelService reads the experiment/promotion config
  // The seeds are read only when a local (non-deployed) store is bootstrapped, e.g. to check the bundle locally.
  ["data/raw/dataset.csv", "data/raw/dataset.csv"],
];
const GLOBS = [
  ["infra/cloudflare/d1/migrations", ".sql"], // readiness compares the D1 schema with the migration count
  ["data/seed", ".csv"],
];
const SKIP_DIRS = new Set(["__pycache__", ".mypy_cache", ".pytest_cache", "tests"]);
const SKIP_FILE = /\.(pyc|pyo)$/;
/** Marks a stage directory made by this script, the only kind of non-empty directory it empties. Never uploaded. */
export const STAGE_MARKER = ".icp-vercel-stage";
const VERCELIGNORE = [".env", ".env.*", ".vercel", "__pycache__", "*.pyc", STAGE_MARKER, ""].join("\n");

/**
 * How Vercel packs the Python function (docs/DEPLOYMENT.md "Size of the function"). Vercel's builder keeps at
 * most 225 MiB of a Python function in the function itself; for a larger dependency set (ours: 356.1 MB) its
 * default ("runtime-install") installs the remaining packages from PyPI into /tmp at every cold start. "full"
 * switches that install off for the deployment, so every dependency is bundled and the function runs on the
 * large-functions path (beta, up to 5 GB; new projects are enrolled by default). Passed per deployment as
 * build-time variables (`vercel deploy --build-env`); the project's own variables are never touched for this.
 */
export const BUNDLE_MODES = {
  "runtime-install": [],
  full: ["VERCEL_PYTHON_DISABLE_RUNTIME_INSTALL=1", "VERCEL_SUPPORT_LARGE_FUNCTIONS=1"],
};

/** Variables the staging backend refuses to start without (backend/icp_api/settings.py, docs/ENVIRONMENTS.md). */
const REQUIRED = {
  APP_ENV: (v) => v === "staging",
  METADATA_BACKEND: (v) => v === "d1_gateway",
  OBJECT_STORE_BACKEND: (v) => v === "gateway",
  MODEL_SOURCE: (v) => v === "bundle",
  REQUIRE_GATEWAY_SECRET: (v) => v === "true",
  GATEWAY_INTERNAL_URL: (v) => /^https:\/\/[^\s/<>]+\/?$/.test(v),
  GATEWAY_SHARED_SECRET: (v) => v.length >= 32,
  ADMIN_API_TOKENS: (v) => v.split(",").map((s) => s.trim()).filter(Boolean).every((e) => /^[^:\s]+:\S{32,}$/.test(e)),
  METRICS_TOKEN: (v) => v.length >= 32,
  CORS_ALLOWED_ORIGINS: (v) => v !== "" && !v.includes("*"),
  EMAIL_ENABLED: (v) => v === "false",
  BILLING_ENABLED: (v) => v === "false",
  API_INSTANCES: (v) => v === "1",
  LOG_FORMAT: (v) => v === "json",
};
/** Container-only settings (meaningless on Vercel) and names the platform reserves. */
const REFUSED = /^(PORT|RENDER|API_HOST|API_PORT|SQLITE_PATH|LOCAL_OBJECT_DIR|MLFLOW_.*|VERCEL_.*|AWS_.*|NOW_.*)$/;
/** Stored as Vercel "Secret" variables (write-only in the dashboard); everything else as "Config". */
const SENSITIVE_NAME = /(SECRETS?|TOKENS?|PASSWORD|_KEY|_KEY_ID)$/;
const NOT_SENSITIVE = new Set(["REQUIRE_GATEWAY_SECRET"]); // a true/false switch, not a credential
export const isSensitive = (name) => SENSITIVE_NAME.test(name) && !NOT_SENSITIVE.has(name);

function fail(message, code = 2) {
  console.error(`vercel-backend: ${message}`);
  process.exit(code);
}

/** Where the CLI runs: outside the repository, in the user's local application data (or cache) folder. */
export function defaultStageDir(env = process.env, home = os.homedir(), windows = IS_WINDOWS) {
  if (windows && env.LOCALAPPDATA) return path.join(env.LOCALAPPDATA, "icp-vercel-backend");
  return path.join(env.XDG_CACHE_HOME || path.join(home, ".cache"), "icp-vercel-backend");
}

function args() {
  const argv = process.argv.slice(2);
  const value = (flag, fallback) => {
    const i = argv.indexOf(flag);
    return i >= 0 && argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[i + 1] : fallback;
  };
  return {
    deploy: argv.includes("--deploy"),
    dryRun: argv.includes("--dry-run"),
    skipEnv: argv.includes("--skip-env"),
    writeGatewayUrl: argv.includes("--write-gateway-url"),
    gatewayUrl: value("--write-gateway-url", null),
    runtimeLog: argv.includes("--runtime-log"),
    project: value("--project", DEFAULT_PROJECT),
    envFile: path.resolve(ROOT, value("--env-file", "var/secrets/staging.vercel.env")),
    tokenFile: path.resolve(ROOT, value("--token-file", "var/secrets/vercel.token")),
    cliLogin: argv.includes("--cli-login"),
    stageDir: path.resolve(value("--stage-dir", defaultStageDir())),
    bundle: value("--bundle", savedBundleMode()),
    bundleGiven: argv.includes("--bundle"),
  };
}

function savedBundleMode() {
  const saved = existsSync(BUNDLE_MODE_FILE) ? readFileSync(BUNDLE_MODE_FILE, "utf8").trim() : "";
  return Object.hasOwn(BUNDLE_MODES, saved) ? saved : "runtime-install";
}

// ---------------------------------------------------------------------------------------------
// Build
// ---------------------------------------------------------------------------------------------

function copyTree(src, dest, files) {
  for (const entry of readdirSync(src, { withFileTypes: true })) {
    if (entry.isDirectory()) {
      if (!SKIP_DIRS.has(entry.name)) copyTree(path.join(src, entry.name), path.join(dest, entry.name), files);
    } else if (entry.isFile() && !SKIP_FILE.test(entry.name)) {
      copyFile(path.join(src, entry.name), path.join(dest, entry.name), files);
    }
  }
}

function copyFile(src, dest, files) {
  if (!existsSync(src)) fail(`missing ${path.relative(ROOT, src)}`);
  mkdirSync(path.dirname(dest), { recursive: true });
  cpSync(src, dest);
  files.push(dest);
}

function sha256(file) {
  return createHash("sha256").update(readFileSync(file)).digest("hex");
}

function build() {
  // The CLI never runs here (see stage()), so the folder holds exactly the files below and nothing else.
  rmSync(OUT, { recursive: true, force: true });
  mkdirSync(OUT, { recursive: true });

  const files = [];
  for (const [src, dest] of TREES) copyTree(path.join(ROOT, src), path.join(OUT, dest), files);
  for (const [src, dest] of FILES) copyFile(path.join(ROOT, src), path.join(OUT, dest), files);
  for (const [dir, ext] of GLOBS) {
    for (const name of readdirSync(path.join(ROOT, dir)).filter((n) => n.endsWith(ext)).sort()) {
      copyFile(path.join(ROOT, dir, name), path.join(OUT, dir, name), files);
    }
  }
  writeFileSync(path.join(OUT, ".vercelignore"), VERCELIGNORE);
  files.push(path.join(OUT, ".vercelignore"));

  // Nothing secret or stateful may ever be part of the bundle.
  const forbidden = files.filter((f) => /(^|[\\/])(\.env[^\\/]*|.*\.token|.*\.db|secrets)$/i.test(path.relative(OUT, f)));
  if (forbidden.length) fail(`refusing to build: ${forbidden.map((f) => path.relative(OUT, f)).join(", ")}`);

  const entries = files.map((f) => ({ path: path.relative(OUT, f).split(path.sep).join("/"), bytes: statSync(f).size, sha256: sha256(f) }));
  entries.sort((a, b) => a.path.localeCompare(b.path));
  const total = entries.reduce((n, e) => n + e.bytes, 0);
  const byTop = {};
  for (const e of entries) {
    const top = e.path.includes("/") ? e.path.split("/").slice(0, 2).join("/") : e.path;
    byTop[top] = (byTop[top] ?? 0) + e.bytes;
  }
  const requirements = readFileSync(path.join(OUT, "requirements.txt"), "utf8").split(/\r?\n/).filter((l) => /^[A-Za-z0-9]/.test(l));
  writeFileSync(MANIFEST, JSON.stringify({ built_at: new Date().toISOString(), cli: VERCEL_CLI, total_bytes: total, files: entries }, null, 2));

  console.log(`deploy folder: ${path.relative(ROOT, OUT)}  (${entries.length} files, ${(total / 1024).toFixed(1)} KiB source)`);
  for (const [top, bytes] of Object.entries(byTop).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${top.padEnd(42)} ${(bytes / 1024).toFixed(1).padStart(8)} KiB`);
  }
  console.log(`requirements.txt: ${requirements.length} pinned packages, installed by Vercel's build`);
  console.log("  (356.1 MB installed on Linux CPython 3.12, above the 225 MiB Vercel keeps inside a Python function: by default");
  console.log("   Vercel installs them from PyPI into /tmp at every cold start; --bundle full keeps them all in the function)");
  console.log(`manifest with sha256 per file: ${path.relative(ROOT, MANIFEST)}`);
}

// ---------------------------------------------------------------------------------------------
// Stage directory (local; the only place the Vercel CLI ever runs)
// ---------------------------------------------------------------------------------------------

/** Why `dir` must not host the CLI (inside the repository or under any Git work tree), or null. */
export function stageProblem(dir, root = ROOT) {
  const abs = path.resolve(dir);
  const rel = path.relative(root, abs);
  if (rel === "" || (!rel.startsWith("..") && !path.isAbsolute(rel))) return "inside the repository";
  for (let d = abs; ; d = path.dirname(d)) {
    if (existsSync(path.join(d, ".git"))) return `under a Git work tree (${path.join(d, ".git")})`;
    if (path.dirname(d) === d) return null;
  }
}

/**
 * Why the existing entries of a stage directory must not be deleted, or null. The directory is emptied before each
 * copy, so it must be empty, carry this script's marker, or hold nothing but what a stage copy holds (the deploy
 * folder's top-level names, `.vercel/` and what `vercel link` leaves): a mistyped --stage-dir is refused, not wiped.
 */
export function stageContentProblem(entries, expectedTop) {
  if (entries.length === 0 || entries.includes(STAGE_MARKER)) return null;
  const foreign = entries.filter((n) => n !== ".vercel" && !expectedTop.has(n) && linkLeftovers([n]).length === 0);
  if (!foreign.length) return null;
  return `not empty and not a stage directory of this script (${foreign.slice(0, 5).join(", ")}${foreign.length > 5 ? ", ..." : ""})`;
}

/** Copy the deploy folder to the stage directory, keeping only its `.vercel/` project link (ids, no secrets). */
function stage(stageDir) {
  const problem = stageProblem(stageDir);
  if (problem) fail(`stage directory ${stageDir} is ${problem}; choose another with --stage-dir`);
  const { files } = JSON.parse(readFileSync(MANIFEST, "utf8"));
  const expectedTop = new Set(files.map((e) => e.path.split("/")[0]));
  if (existsSync(stageDir)) {
    if (!statSync(stageDir).isDirectory()) fail(`stage directory ${stageDir} is not a directory; choose another with --stage-dir`);
    const contentProblem = stageContentProblem(readdirSync(stageDir), expectedTop);
    if (contentProblem) fail(`stage directory ${stageDir} is ${contentProblem}; choose another with --stage-dir`);
  }
  mkdirSync(stageDir, { recursive: true });
  for (const entry of readdirSync(stageDir)) {
    if (entry !== ".vercel" && entry !== STAGE_MARKER) rmSync(path.join(stageDir, entry), { recursive: true, force: true });
  }
  writeFileSync(path.join(stageDir, STAGE_MARKER), "Stage directory of scripts/vercel-backend.mjs (ICP repository); emptied before every copy.\n");
  cpSync(OUT, stageDir, { recursive: true });
  for (const entry of files) {
    const copy = path.join(stageDir, ...entry.path.split("/"));
    if (!existsSync(copy) || sha256(copy) !== entry.sha256) fail(`stage copy differs from the manifest: ${entry.path}`);
  }
  const extra = readdirSync(stageDir).filter((n) => n !== ".vercel" && n !== STAGE_MARKER && !expectedTop.has(n));
  if (extra.length) fail(`unexpected files in the stage directory: ${extra.join(", ")}`);
  console.log(`stage directory (outside the repository; the Vercel CLI runs only here): ${stageDir}  (${files.length} files)`);
}

/**
 * Environment of every CLI run: the token (none with --cli-login: the CLI then uses its own stored login), no
 * telemetry, and Git discovery fenced at the stage directory.
 */
export function cliEnv(base, token, stageDir) {
  const env = { ...base, VERCEL_TELEMETRY_DISABLED: "1", NO_COLOR: "1", FORCE_COLOR: "0" };
  if (token) env.VERCEL_TOKEN = token;
  else delete env.VERCEL_TOKEN;
  for (const key of Object.keys(env)) {
    if (/^GIT_(DIR|WORK_TREE|COMMON_DIR|INDEX_FILE|OBJECT_DIRECTORY|ALTERNATE_OBJECT_DIRECTORIES|NAMESPACE|CEILING_DIRECTORIES)$/i.test(key)) delete env[key];
  }
  // git stops looking for a repository at the stage directory's parent (it never "chdirs up" into it).
  env.GIT_CEILING_DIRECTORIES = path.dirname(path.resolve(stageDir));
  return env;
}

/** Files `vercel link` leaves in the stage directory besides `.vercel/`: the OIDC token file and a .gitignore. */
export function linkLeftovers(names) {
  return names.filter((n) => /^\.env/i.test(n) || n === ".gitignore");
}

/**
 * Outcome of `vercel git disconnect --yes` with the pinned CLI (63.1.2, read in its code): exit 1 with "No Git
 * repository connected" when there is none; exit 0 with "Disconnected <org>/<repo>." after removing one. Anything
 * else (an exit 0 without that line, such as "Canceled", or another error) is "unknown".
 */
export function gitConnectionCheck({ code, out = "", err = "" }) {
  const text = `${out}\n${err}`;
  if (code !== 0 && /No Git repository connected/i.test(text)) return "none";
  if (code === 0 && /\bDisconnected\b/.test(text)) return "removed";
  return "unknown";
}

/**
 * Plan of the CLI's current scope from `vercel teams ls` (the row marked with a check mark; the plan is its last
 * column), lower-cased, or null when no marked row is found.
 */
export function currentScopePlan(text) {
  const row = text.split(/\r?\n/).find((l) => /^\s*[√✔✓]\s+\S/.test(l));
  return row ? row.trim().split(/\s+/).pop().toLowerCase() : null;
}

/** The token from --token-file, or null with --cli-login (the CLI's own stored `vercel login`). */
function credential(opts) {
  if (opts.cliLogin) return null;
  if (!existsSync(opts.tokenFile)) fail(`missing ${path.relative(ROOT, opts.tokenFile)} (a Vercel access token; docs/DEPLOYMENT.md), or pass --cli-login after \`npx ${VERCEL_CLI} login\``);
  const token = readFileSync(opts.tokenFile, "utf8").trim();
  if (!token || /\s/.test(token)) fail(`${path.relative(ROOT, opts.tokenFile)} must contain exactly one token`);
  return token;
}

/** `text` with every secret value replaced; values shorter than 8 characters (switches such as `true`) are kept. */
export function redact(text, secrets) {
  let out = text;
  for (const secret of [...new Set(secrets)].filter((s) => typeof s === "string" && s.length >= 8).sort((a, b) => b.length - a.length)) {
    out = out.split(secret).join("<redacted>");
  }
  return out;
}

/** The token and the values (and their name:token parts) of sensitive variables, for redaction only. */
function secretValues(token, vars) {
  const values = [token];
  for (const [key, value] of vars) if (isSensitive(key)) values.push(value, ...value.split(/[,:]/).map((s) => s.trim()));
  return values;
}

/** Lines of the runtime log that show cold starts and Vercel's dependency install (default packing only). */
export function runtimeLogLines(log) {
  const pattern = /runtime dependenc|dependency installation/i;
  return log.split(/\r?\n/).map((l) => l.trim()).filter((l) => l && pattern.test(l)).map((l) => l.slice(0, 300)).slice(-30);
}

/** Lines of a build log that show how Vercel packed the Python function (docs/DEPLOYMENT.md "Size of the function"). */
export function buildDecisionLines(log) {
  const pattern = /bundle size|standard size|large functions|runtime (dependenc|install)|force-bundling|bytecode|LAMBDA_SIZE|exceeds the|RUNTIME_DEPENDENCY/i;
  return log.split(/\r?\n/).map((l) => l.trim()).filter((l) => l && pattern.test(l)).map((l) => l.slice(0, 300)).slice(0, 20);
}

// ---------------------------------------------------------------------------------------------
// Deploy (remote; only with --deploy)
// ---------------------------------------------------------------------------------------------

/** KEY=VALUE lines; values are kept in memory only. */
export function parseEnvFile(text) {
  const out = new Map();
  for (const raw of text.split(/\r?\n/)) {
    const line = raw.trim();
    if (!line || line.startsWith("#")) continue;
    const eq = line.indexOf("=");
    if (eq <= 0) continue;
    const key = line.slice(0, eq).trim();
    let value = line.slice(eq + 1).trim();
    if (value.length >= 2 && value[0] === value[value.length - 1] && (value[0] === '"' || value[0] === "'")) value = value.slice(1, -1);
    out.set(key, value);
  }
  return out;
}

/** Problems with the variable set, by NAME only (values are never echoed). */
export function envProblems(vars) {
  const problems = [];
  for (const [key, ok] of Object.entries(REQUIRED)) {
    if (!vars.has(key)) problems.push(`${key}: missing`);
    else if (!ok(vars.get(key))) problems.push(`${key}: value does not meet the staging rule (see docs/ENVIRONMENTS.md)`);
  }
  for (const key of vars.keys()) {
    if (!/^[A-Z][A-Z0-9_]*$/.test(key)) problems.push(`${key}: not a valid variable name`);
    else if (REFUSED.test(key)) problems.push(`${key}: container-only or platform-reserved; remove it from the file`);
  }
  return problems;
}

/** The pinned CLI, without a shell where possible (Windows: npm's own npx-cli.js under node). */
function cliCommand(cliArgs) {
  const npxCli = path.join(path.dirname(process.execPath), "node_modules", "npm", "bin", "npx-cli.js");
  if (IS_WINDOWS && existsSync(npxCli)) return { cmd: process.execPath, args: [npxCli, "--yes", VERCEL_CLI, ...cliArgs], shell: false };
  return { cmd: IS_WINDOWS ? "npx.cmd" : "npx", args: ["--yes", VERCEL_CLI, ...cliArgs], shell: IS_WINDOWS };
}

/**
 * Run the CLI in the stage directory. `stdin` (a secret value) is written to the child's stdin and never logged;
 * `capture`/`captureErr` collect stdout/stderr instead of showing them.
 */
function vercel(cliArgs, { token, stageDir, stdin = null, capture = false, captureErr = false }) {
  const { cmd, args: argv, shell } = cliCommand(cliArgs);
  return new Promise((resolve) => {
    const child = spawn(cmd, argv, {
      cwd: stageDir,
      env: cliEnv(process.env, token, stageDir),
      shell,
      stdio: [stdin === null ? "ignore" : "pipe", capture ? "pipe" : "inherit", captureErr ? "pipe" : "inherit"],
    });
    let out = "";
    let err = "";
    if (capture) child.stdout.on("data", (d) => { out += d.toString(); });
    if (captureErr) child.stderr.on("data", (d) => { err += d.toString(); });
    if (stdin !== null) child.stdin.end(stdin); // one chunk, no trailing newline; `vercel env add` reads the first chunk
    child.on("error", (error) => resolve({ code: 1, out, err, error: error.message }));
    child.on("close", (code) => resolve({ code: code ?? 1, out, err }));
  });
}

/** Production URL from `vercel deploy --non-interactive` output (JSON payload, or plain URLs as a fallback). */
export function productionUrlFrom(stdout) {
  const start = stdout.indexOf("{");
  if (start >= 0) {
    try {
      const payload = JSON.parse(stdout.slice(start));
      const d = payload.deployment ?? payload;
      return { productionUrl: d.productionUrl ?? null, deploymentUrl: d.url ?? null, protection: d.deploymentProtection ?? null };
    } catch { /* not JSON: fall through */ }
  }
  const urls = stdout.match(/https:\/\/[a-z0-9.-]+\.vercel\.app/g) ?? [];
  return { productionUrl: null, deploymentUrl: urls[0] ?? null, protection: null };
}

async function probe(url, attempts, delayMs) {
  const first = Date.now();
  for (let i = 1; i <= attempts; i++) {
    const started = Date.now();
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(30_000), headers: { accept: "application/json" } });
      const text = await res.text();
      let body = null;
      try { body = JSON.parse(text); } catch { /* HTML: e.g. a protection or error page */ }
      return { status: res.status, body, ms: Date.now() - started, totalMs: Date.now() - first, attempt: i };
    } catch (err) {
      if (i === attempts) return { status: 0, body: null, ms: Date.now() - started, totalMs: Date.now() - first, attempt: i, error: String(err?.name ?? err) };
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
  return { status: 0, body: null, ms: 0, totalMs: Date.now() - first, attempt: attempts };
}

async function deploy(opts) {
  if (!Object.hasOwn(BUNDLE_MODES, opts.bundle)) fail(`--bundle must be one of: ${Object.keys(BUNDLE_MODES).join(", ")}`);
  const bundleEnv = BUNDLE_MODES[opts.bundle];
  console.log(`bundle mode: ${opts.bundle} (${opts.bundleGiven ? "--bundle" : existsSync(BUNDLE_MODE_FILE) ? `last successful deploy, ${path.relative(ROOT, BUNDLE_MODE_FILE)}` : "default"})`);
  const vars = opts.skipEnv ? new Map() : (() => {
    if (!existsSync(opts.envFile)) fail(`missing ${path.relative(ROOT, opts.envFile)} (create it from var/secrets/staging.vercel.env.example; docs/DEPLOYMENT.md)`);
    return parseEnvFile(readFileSync(opts.envFile, "utf8"));
  })();
  if (!opts.skipEnv) {
    const problems = envProblems(vars);
    if (problems.length) fail(`fix ${path.relative(ROOT, opts.envFile)} first:\n  - ${problems.join("\n  - ")}`);
    console.log(`env file ok: ${vars.size} variables (${[...vars.keys()].join(", ")})`);
  }
  stage(opts.stageDir);
  const deployArgs = ["deploy", "--prod", "--yes", "--non-interactive", ...bundleEnv.flatMap((kv) => ["--build-env", kv])];
  const steps = [
    `vercel teams ls   (${opts.cliLogin ? "the CLI's own login" : `token from ${path.relative(ROOT, opts.tokenFile)}`}; the current scope must be on the Hobby plan, anything else stops the deploy)`,
    `vercel link --yes --project ${opts.project}   (creates the project on the first run; Standard Protection keeps the production domain public; no Git repository is visible from the stage directory, so none is connected)`,
    "delete <stage>/.env.local (the VERCEL_OIDC_TOKEN link writes) and the .gitignore link creates (again when the script exits)",
    "vercel git disconnect --yes   (expected: \"No Git repository connected\"; a connection that exists anyway is removed; any other answer stops the deploy)",
    ...[...vars.keys()].map((k) => `vercel env add ${k} production --force --yes ${isSensitive(k) ? "--sensitive" : "--no-sensitive"}   (value via stdin)`),
    `vercel ${deployArgs.join(" ")}   (bundle mode: ${opts.bundle}; JSON answer saved, redacted, to ${path.relative(ROOT, DEPLOY_LOG)})`,
    "vercel inspect <deployment URL> --logs   (only the lines that show how Vercel packed the function are printed)",
    "GET <production URL>/health/live, /health/ready; GET /api/v1/plans must answer 403 GATEWAY_REQUIRED",
  ];
  if (opts.dryRun) {
    console.log(`\n[dry run] ${VERCEL_CLI} would run in ${opts.stageDir} (Git discovery fenced at ${path.dirname(opts.stageDir)}):`);
    for (const s of steps) console.log(`  - ${s}`);
    return;
  }
  const token = credential(opts);
  // For redaction only (also with --skip-env): the values themselves are never printed.
  const secrets = secretValues(token, opts.skipEnv && existsSync(opts.envFile) ? parseEnvFile(readFileSync(opts.envFile, "utf8")) : vars);
  const cli = { token, stageDir: opts.stageDir };

  // `vercel link` writes .env.local (a short-lived VERCEL_OIDC_TOKEN) and a .gitignore into the stage directory:
  // removed right after linking and again whenever the script exits (also through fail()), so no credential stays.
  const removeLinkLeftovers = () => {
    const names = existsSync(opts.stageDir) ? linkLeftovers(readdirSync(opts.stageDir)) : [];
    for (const name of names) rmSync(path.join(opts.stageDir, name), { recursive: true, force: true });
    return names;
  };
  process.on("exit", removeLinkLeftovers);
  process.once("SIGINT", () => process.exit(130)); // Ctrl+C: exit normally so the cleanup above still runs

  // Install/resolve the pinned CLI once before any value is piped to it (npx then has nothing to fetch).
  if ((await vercel(["--version"], cli)).code !== 0) fail(`${VERCEL_CLI} could not be started`, 1);
  // Free plan only: a Hobby scope has no payment method, so nothing it runs can be billed. Anything else stops here.
  const teams = await vercel(["teams", "ls"], { ...cli, capture: true, captureErr: true });
  const plan = currentScopePlan(`${teams.out}\n${teams.err}`);
  if (teams.code !== 0 || plan !== "hobby") {
    fail(`the CLI's current Vercel scope is ${plan ? `on the "${plan}" plan` : "unknown"} (vercel teams ls, exit ${teams.code}); this script deploys only to a Hobby (free) scope`, 1);
  }
  console.log("vercel scope: Hobby plan (free, no payment method)");
  console.log(`\n${VERCEL_CLI}: linking ${opts.project}`);
  const linked = await vercel(["link", "--yes", "--project", opts.project], cli);
  const leftovers = removeLinkLeftovers();
  if (leftovers.length) console.log(`removed what link wrote into the stage directory: ${leftovers.join(", ")}`);
  if (linked.code !== 0) fail("vercel link failed (see above)", 1);

  const git = await vercel(["git", "disconnect", "--yes"], { ...cli, capture: true, captureErr: true });
  const gitState = gitConnectionCheck(git);
  if (gitState === "unknown") {
    console.error(redact(`${git.out}\n${git.err}`, secrets).trim());
    fail("could not confirm that the project has no Git connection (see above); check Project > Settings > Git, then re-run", 1);
  }
  console.log(gitState === "none"
    ? "git connection: none (as intended; pushes to the repository never build this project)"
    : "git connection: one existed and was REMOVED (pushes to the repository no longer build this project)");

  for (const [key, value] of vars) {
    const kind = isSensitive(key) ? "--sensitive" : "--no-sensitive";
    const res = await vercel(["env", "add", key, "production", "--force", "--yes", kind], { ...cli, stdin: value });
    if (res.code !== 0) fail(`vercel env add ${key} failed (exit ${res.code}); nothing was deployed`, 1);
    console.log(`set ${key} (${kind === "--sensitive" ? "secret" : "config"})`);
  }

  console.log(`\ndeploying to production, bundle mode ${opts.bundle} (Vercel installs requirements.txt and builds the Python function) ...`);
  const dep = await vercel(deployArgs, { ...cli, capture: true });
  const answer = redact(dep.out, secrets);
  mkdirSync(DEPLOY_ROOT, { recursive: true });
  writeFileSync(DEPLOY_LOG, answer || "(no output)\n");
  if (dep.code !== 0) {
    console.error(answer.trim().slice(0, 4000) || "(the CLI printed nothing on stdout)");
    fail(`vercel deploy failed (exit ${dep.code}); its answer is above and in ${path.relative(ROOT, DEPLOY_LOG)} (build logs: npx ${VERCEL_CLI} inspect <deployment URL> --logs)`, 1);
  }
  const { productionUrl, deploymentUrl, protection } = productionUrlFrom(dep.out);
  console.log(`deployment: ${deploymentUrl ?? "(not reported)"}`);
  console.log(`production domain: ${productionUrl ?? "(not reported: read it in the dashboard, Project > Domains)"}`);
  if (protection) console.log(`deployment protection: ${JSON.stringify(protection)} (Standard Protection leaves the production domain public)`);
  if (!productionUrl) process.exit(1);
  writeFileSync(URL_FILE, `${productionUrl}\n`);

  if (deploymentUrl) {
    const logs = await vercel(["inspect", deploymentUrl, "--logs"], { ...cli, capture: true, captureErr: true });
    const lines = buildDecisionLines(redact(`${logs.out}\n${logs.err}`, secrets));
    console.log(lines.length
      ? `build log, packing of the function (bundle mode ${opts.bundle}):\n  ${lines.join("\n  ")}`
      : `build log: no packing lines found (exit ${logs.code}); read it with npx ${VERCEL_CLI} inspect ${deploymentUrl} --logs`);
  }

  // The first answer after a deploy usually includes a cold start (with the default bundle mode: Vercel's install of
  // the dependencies) unless Vercel had already started an instance. Retry a few times anyway.
  const live = await probe(`${productionUrl}/health/live`, 6, 10_000);
  console.log(`GET /health/live  -> ${live.status} ${live.body ? JSON.stringify(live.body) : "(not JSON)"} in ${live.ms} ms (attempt ${live.attempt}, ${(live.totalMs / 1000).toFixed(1)} s in all)`);
  const ready = await probe(`${productionUrl}/health/ready`, 2, 5_000);
  console.log(`GET /health/ready -> ${ready.status} ${ready.body ? JSON.stringify(ready.body) : "(not JSON)"} in ${ready.ms} ms`);
  const direct = await probe(`${productionUrl}/api/v1/plans`, 2, 5_000);
  console.log(`GET /api/v1/plans without the gateway secret -> ${direct.status} ${direct.body?.error?.code ?? ""} (expected 403 GATEWAY_REQUIRED)`);
  const ok = live.status === 200 && live.body?.status === "ok" && direct.status === 403;
  if (ok) writeFileSync(BUNDLE_MODE_FILE, `${opts.bundle}\n`);
  if (ok && live.totalMs > 6_000 && opts.bundle === "runtime-install") {
    console.log(`\nfirst /health/live took ${(live.totalMs / 1000).toFixed(1)} s: compare --bundle full (docs/DEPLOYMENT.md, "Size of the function")`);
  }
  console.log(ok
    ? `\nbackend is up. Next: node scripts/vercel-backend.mjs --write-gateway-url, then npx wrangler deploy --env staging -c apps/gateway/wrangler.jsonc`
    : "\nbackend check FAILED: see docs/DEPLOYMENT.md (Troubleshooting the Vercel backend)");
  process.exit(ok ? 0 : 1);
}

/** Read-only: the cold-start install lines of the production runtime log (Hobby keeps it 1 hour). */
async function runtimeLog(opts) {
  if (stageProblem(opts.stageDir)) fail(`stage directory ${opts.stageDir} is ${stageProblem(opts.stageDir)}`);
  if (!existsSync(path.join(opts.stageDir, ".vercel", "project.json"))) fail(`no linked project in ${opts.stageDir}: run --deploy first`);
  const token = credential(opts);
  // Request logs (the pinned CLI's default page is 100 entries; no larger value is relied on).
  const res = await vercel(["logs", "--project", opts.project, "--environment", "production", "--since", "1h", "--limit", "100", "--json"],
    { token, stageDir: opts.stageDir, capture: true, captureErr: true });
  const lines = runtimeLogLines(redact(`${res.out}\n${res.err}`, secretValues(token, new Map())));
  if (res.code !== 0) fail(`vercel logs failed (exit ${res.code}); read the Logs page of the project in the dashboard`, 1);
  console.log(lines.length
    ? `cold-start install lines of the last hour (production):\n  ${lines.join("\n  ")}`
    : "no install line in the last hour of request logs: no cold start yet, an instance Vercel had already started, the full\n"
      + "bundle (no install at start-up), or a start-up line the request logs do not carry: check the project's Logs page in the dashboard");
}

// ---------------------------------------------------------------------------------------------
// Gateway configuration (local file edit, no remote call)
// ---------------------------------------------------------------------------------------------

/** Replace env.staging.vars.BACKEND_URL in wrangler.jsonc text; throws unless exactly one value is found. */
export function withStagingBackendUrl(text, url) {
  const start = text.indexOf('"staging": {');
  const end = text.indexOf('"production": {', start);
  if (start < 0 || end < 0) throw new Error("env.staging / env.production blocks not found in wrangler.jsonc");
  const block = text.slice(start, end);
  const re = /("BACKEND_URL":\s*")([^"]*)(")/g;
  const matches = [...block.matchAll(re)];
  if (matches.length !== 1) throw new Error(`expected one BACKEND_URL in env.staging, found ${matches.length}`);
  return text.slice(0, start) + block.replace(re, `$1${url}$3`) + text.slice(end);
}

function writeGatewayUrl(explicit) {
  const url = (explicit ?? (existsSync(URL_FILE) ? readFileSync(URL_FILE, "utf8").trim() : "")).replace(/\/+$/, "");
  if (!/^https:\/\/[a-z0-9.-]+$/.test(url)) fail("no valid https origin given and none saved by a deploy (var/vercel-backend-url.txt)");
  const before = readFileSync(WRANGLER, "utf8");
  const after = withStagingBackendUrl(before, url);
  if (after === before) {
    console.log(`env.staging.vars.BACKEND_URL already is ${url}`);
    return;
  }
  writeFileSync(WRANGLER, after);
  console.log(`apps/gateway/wrangler.jsonc: env.staging.vars.BACKEND_URL = ${url}`);
  console.log("deploy the Worker: npx wrangler deploy --env staging -c apps/gateway/wrangler.jsonc");
}

// ---------------------------------------------------------------------------------------------

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const opts = args();
  if (opts.runtimeLog) {
    await runtimeLog(opts);
  } else if (opts.writeGatewayUrl) {
    writeGatewayUrl(opts.gatewayUrl);
  } else {
    if (!existsSync(path.join(ROOT, "infra", "vercel", "index.py"))) fail("run from the repository (infra/vercel/ missing)");
    build();
    if (opts.deploy) await deploy(opts);
    else console.log("\nbuild only (nothing remote). Deploy: node scripts/vercel-backend.mjs --deploy  (docs/DEPLOYMENT.md)");
  }
}
