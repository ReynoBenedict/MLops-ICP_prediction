// Lightweight secret scan of tracked-type source files (no external tool required).
// Flags high-confidence credential patterns and committed local secret files. Exit 1 on findings.
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";
import { ROOT } from "./lib.mjs";

const SKIP_DIRS = new Set([".git", "node_modules", ".venv", "var", "dist", ".wrangler", "legacy", "data", ".dvc",
  "__pycache__", ".pytest_cache", ".ruff_cache", ".mypy_cache", ".idea", "notebooks"]);
const EXTS = new Set([".py", ".ts", ".tsx", ".js", ".mjs", ".json", ".jsonc", ".toml", ".yaml", ".yml", ".md", ".html",
  ".css", ".sql", ".txt", ".example", ".cfg", ".ini", ""]);
const PATTERNS = [
  ["AWS access key", /AKIA[0-9A-Z]{16}/],
  ["Private key block", /-----BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY-----/],
  ["GitHub token", /gh[pousr]_[A-Za-z0-9]{36,}/],
  ["Slack token", /xox[baprs]-[A-Za-z0-9-]{10,}/],
  ["Generic credential assignment", /(?:SECRET|TOKEN|PASSWORD|API_KEY)\s*[:=]\s*["']?[A-Za-z0-9_\-+/]{24,}["']?/],
  ["Connection string with password", /postgres(?:ql)?(?:\+\w+)?:\/\/[^:\s<]+:[^@\s<]{6,}@/],
];
const ALLOW = [/<[^>]+>/, /test-(admin|gateway)-/, /"s"\.repeat/, /change-me/];

const findings = [];
function walk(dir) {
  for (const name of readdirSync(dir)) {
    const full = path.join(dir, name);
    const st = statSync(full);
    if (st.isDirectory()) {
      if (!SKIP_DIRS.has(name)) walk(full);
      continue;
    }
    if (name === ".env" || name === ".dev.vars") continue; // gitignored local secrets, checked below
    if (!EXTS.has(path.extname(name)) || st.size > 2_000_000) continue;
    readFileSync(full, "utf8").split(/\r?\n/).forEach((line, i) => {
      for (const [label, re] of PATTERNS) {
        if (re.test(line) && !ALLOW.some((a) => a.test(line))) findings.push(`${path.relative(ROOT, full)}:${i + 1}: ${label}`);
      }
    });
  }
}
walk(ROOT);

// Local secret files must be gitignored.
const gitignore = existsSync(path.join(ROOT, ".gitignore")) ? readFileSync(path.join(ROOT, ".gitignore"), "utf8") : "";
for (const required of [".env", ".dev.vars"]) {
  if (!gitignore.split(/\r?\n/).some((l) => l.trim() === required || l.trim() === `${required}*`)) {
    findings.push(`.gitignore does not ignore ${required}`);
  }
}

if (findings.length) {
  console.error("Potential secrets found:\n" + findings.map((f) => `  ${f}`).join("\n"));
  process.exit(1);
}
console.log("Secret scan: no findings.");
