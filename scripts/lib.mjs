// Shared helpers for the cross-platform developer scripts (Windows, macOS, Linux).
import { existsSync } from "node:fs";
import { spawn, spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
export const IS_WINDOWS = process.platform === "win32";

/** Python interpreter of the project virtualenv (.venv), or null when it does not exist yet. */
export function venvPython() {
  const p = IS_WINDOWS ? path.join(ROOT, ".venv", "Scripts", "python.exe") : path.join(ROOT, ".venv", "bin", "python");
  return existsSync(p) ? p : null;
}

export function requirePython() {
  const py = venvPython();
  if (!py) {
    console.error("Python virtualenv not found (.venv). Run `npm run setup` first.");
    process.exit(1);
  }
  return py;
}

/** Run a command synchronously, inheriting stdio; exits the process on failure. */
export function run(cmd, args, opts = {}) {
  const res = spawnSync(cmd, args, { stdio: "inherit", cwd: ROOT, shell: false, ...opts });
  if (res.error) throw res.error;
  if (res.status !== 0) process.exit(res.status ?? 1);
}

export function npmCmd() {
  return IS_WINDOWS ? "npm.cmd" : "npm";
}

/** Start a long-running child process with prefixed output. */
export function start(name, cmd, args, opts = {}) {
  const child = spawn(cmd, args, { cwd: ROOT, shell: IS_WINDOWS && cmd.endsWith(".cmd"), ...opts });
  const prefix = `[${name}]`;
  const pipe = (stream, out) => {
    let buf = "";
    stream.on("data", (chunk) => {
      buf += chunk.toString();
      const lines = buf.split(/\r?\n/);
      buf = lines.pop() ?? "";
      for (const line of lines) out.write(`${prefix} ${line}\n`);
    });
  };
  pipe(child.stdout, process.stdout);
  pipe(child.stderr, process.stderr);
  return child;
}
