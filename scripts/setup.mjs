// One-time local setup: Python venv + dependencies, local secrets, database migrations, dataset seed,
// first model training/promotion, and a web build for `npm run dev:edge`.
//
//   npm install && npm run setup            (re-running is safe; existing .env/.dev.vars are kept)
//   npm run setup -- --skip-train           (skip the initial model training)
import { randomBytes } from "node:crypto";
import { copyFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { IS_WINDOWS, ROOT, npmCmd, run, venvPython } from "./lib.mjs";

const args = new Set(process.argv.slice(2));
const step = (msg) => console.log(`\n==> ${msg}`);

function findSystemPython() {
  const candidates = IS_WINDOWS ? [["py", ["-3.12"]], ["py", ["-3.11"]], ["py", ["-3.10"]], ["python", []]] : [["python3.12", []], ["python3.11", []], ["python3.10", []], ["python3", []]];
  for (const [cmd, pre] of candidates) {
    const r = spawnSync(cmd, [...pre, "-c", "import sys; assert (3,10) <= sys.version_info[:2] < (3,13); print(sys.version)"], { encoding: "utf8" });
    if (r.status === 0) return [cmd, pre];
  }
  console.error("Python 3.10-3.12 is required but was not found on PATH.");
  process.exit(1);
}

step("Python virtual environment (.venv)");
if (!venvPython()) {
  const [cmd, pre] = findSystemPython();
  run(cmd, [...pre, "-m", "venv", ".venv"]);
}
const py = venvPython();
run(py, ["-m", "pip", "install", "--upgrade", "pip"]);
const constraints = path.join(ROOT, "requirements", "constraints.txt");
run(py, ["-m", "pip", "install", "-e", ".[api,cloud,ingest,dev]", ...(existsSync(constraints) ? ["-c", constraints] : [])]);

step("Local secrets (.env, apps/gateway/.dev.vars) - generated once, never committed");
const envPath = path.join(ROOT, ".env");
if (!existsSync(envPath)) {
  copyFileSync(path.join(ROOT, ".env.example"), envPath);
  let text = readFileSync(envPath, "utf8");
  text = text.replace(/^ADMIN_API_TOKENS=.*$/m, `ADMIN_API_TOKENS=local-admin:${randomBytes(24).toString("base64url")}`);
  text = text.replace(/^GATEWAY_SHARED_SECRET=.*$/m, `GATEWAY_SHARED_SECRET=${randomBytes(32).toString("base64url")}`);
  writeFileSync(envPath, text);
  console.log("Created .env with a generated local admin token (see ADMIN_API_TOKENS in .env).");
} else {
  console.log(".env exists - kept as is.");
}
const secret = /^GATEWAY_SHARED_SECRET=(.*)$/m.exec(readFileSync(envPath, "utf8"))?.[1]?.trim();
const devVars = path.join(ROOT, "apps", "gateway", ".dev.vars");
if (!existsSync(devVars) && secret) {
  writeFileSync(devVars, `GATEWAY_SHARED_SECRET=${secret}\n`);
  console.log("Created apps/gateway/.dev.vars (same gateway secret as .env).");
}

step("Database migrations, dataset seed" + (args.has("--skip-train") ? "" : ", first model training and promotion"));
if (!existsSync(path.join(ROOT, "data", "raw", "dataset.csv"))) {
  console.warn("data/raw/dataset.csv is missing. Restore it with `dvc pull` (or copy the dataset) and re-run setup.");
} else if (args.has("--skip-train")) {
  run(py, ["-m", "icp_api.cli", "migrate"]);
  run(py, ["-m", "icp_api.cli", "seed"]);
} else {
  run(py, ["-m", "icp_api.cli", "bootstrap"]);
}

step("API contract types and web build");
run(npmCmd(), ["run", "contracts"], { shell: IS_WINDOWS });
run(npmCmd(), ["run", "build", "-w", "@icp/web"], { shell: IS_WINDOWS });

console.log("\nSetup complete. Start the app with `npm run dev` (http://localhost:5173) or `npm run dev:edge` (http://localhost:8787).");
