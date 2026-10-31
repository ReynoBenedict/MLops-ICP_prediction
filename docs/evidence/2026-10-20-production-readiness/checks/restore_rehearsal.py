"""OPS-01 restore rehearsal on a COPY of a backup (never touches the live var/ beyond reading the backup).

Usage: python restore_rehearsal.py <repo_root> <backup_dir> <work_dir> <port>
Prints a JSON report; secrets (throwaway token) are generated in memory and never printed.
"""
import hashlib, json, os, secrets, shutil, sqlite3, subprocess, sys, time, urllib.request, urllib.error
from pathlib import Path

repo, backup, work, port = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]), int(sys.argv[4])
report = {"backup": str(backup), "work_dir": str(work), "port": port, "steps": []}
def step(name, **kw): report["steps"].append({"step": name, **kw}); print(f"[{name}] {kw}", file=sys.stderr, flush=True)
def sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""): h.update(b)
    return h.hexdigest()
def tree(p):
    files = [f for f in Path(p).rglob("*") if f.is_file()]
    return len(files), sum(f.stat().st_size for f in files)

t0 = time.time()
src_sha_before = sha(backup / "metadata.db")
src_tree_before = tree(backup)
assert not work.exists(), "work dir must be fresh"
data = work / "data"
shutil.copytree(backup, data)
step("copy", seconds=round(time.time() - t0, 2), files=tree(data)[0], bytes=tree(data)[1],
     metadata_sha_equal=sha(data / "metadata.db") == src_sha_before)

con = sqlite3.connect(f"file:{(data / 'metadata.db').as_posix()}?mode=ro", uri=True)
integ = con.execute("PRAGMA integrity_check").fetchone()[0]
schema = con.execute("SELECT MAX(version) FROM app_schema").fetchone()[0]
tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
counts = {t: con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0] for t in tables}
con.close()
ml = sqlite3.connect(f"file:{(data / 'mlflow.db').as_posix()}?mode=ro", uri=True)
ml_integ = ml.execute("PRAGMA integrity_check").fetchone()[0]
locs = [r[0] for r in ml.execute("SELECT artifact_location FROM experiments")]
live_var = str((repo / "var").resolve()).replace("\\", "/").lower()
ml.close()
step("integrity", metadata=integ, schema_version=schema, mlflow=ml_integ, row_counts=counts,
     mlflow_experiments=len(locs),
     mlflow_artifact_locations_point_into_live_var=sum(1 for l in locs if live_var in (l or "").replace("\\", "/").lower()))

token = "rehearsal-" + secrets.token_urlsafe(32)
env = {k: v for k, v in os.environ.items() if not k.startswith(("SQLITE_", "LOCAL_OBJECT", "MLFLOW_", "ADMIN_API", "GATEWAY_", "MODEL_", "SEED_", "SOURCE_", "API_", "APP_ENV", "METADATA_", "OBJECT_STORE"))}
env.update({
    "APP_ENV": "test", "API_HOST": "127.0.0.1", "API_PORT": str(port), "PORT": str(port), "LOG_LEVEL": "WARNING",
    "SQLITE_PATH": str(data / "metadata.db"), "LOCAL_OBJECT_DIR": str(data / "objects"),
    "MLFLOW_TRACKING_URI": "sqlite:///" + (data / "mlflow.db").as_posix(), "MLFLOW_ARTIFACT_ROOT": str(data / "mlflow-artifacts"),
    "MODEL_SOURCE": "bundle", "SEED_FROM_REPOSITORY": "false",
    "SOURCE_CACHE_DIR": str(work / "cache"), "SOURCE_STATE_DIR": str(work / "sources"),
    "ADMIN_API_TOKENS": f"rehearsal:{token}", "PYTHONDONTWRITEBYTECODE": "1",
})
t1 = time.time()
log = open(work / "server.log", "w", encoding="utf-8")
proc = subprocess.Popen([str(repo / ".venv" / "Scripts" / "python.exe"), "-m", "icp_api.cli", "serve"], cwd=str(repo), env=env, stdout=log, stderr=subprocess.STDOUT)
base = f"http://127.0.0.1:{port}"
def get(path, auth=True):
    req = urllib.request.Request(base + path, headers={"Authorization": f"Bearer {token}"} if auth else {})
    try:
        with urllib.request.urlopen(req, timeout=60) as r: return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        body = e.read()
        try: return e.code, json.loads(body)
        except Exception: return e.code, None
try:
    for _ in range(240):
        try:
            if get("/health/live", auth=False)[0] == 200: break
        except Exception: time.sleep(0.5)
    else:
        raise SystemExit("server did not start")
    step("start", seconds_to_live=round(time.time() - t1, 2), pid=proc.pid)
    st, ready = get("/health/ready")
    step("health_ready", status=st, overall=(ready or {}).get("status"),
         checks={c["name"]: c["status"] for c in (ready or {}).get("checks", [])})
    st, sysst = get("/api/v1/system/status")
    step("system_status", status=st, schema_version=(sysst or {}).get("schema_version"),
         expected=(sysst or {}).get("expected_schema_version"), model_source=(sysst or {}).get("model_source"),
         metadata_backend=(sysst or {}).get("metadata_backend"))
    st, snap = get("/api/v1/public/market-snapshot", auth=False)
    icp = (snap or {}).get("icp") or snap
    step("public_market_snapshot", status=st, icp_period=(snap or {}).get("icp_period") or (icp or {}).get("period"),
         icp_usd=(snap or {}).get("icp_usd") or (icp or {}).get("value"))
    try:
        with urllib.request.urlopen("http://127.0.0.1:8000/api/v1/public/market-snapshot", timeout=30) as r:
            live = json.loads(r.read())
        step("compare_live_snapshot", identical=live == snap)
    except Exception as e:
        step("compare_live_snapshot", error=type(e).__name__)
    for path in ("/api/v1/data/summary", "/api/v1/datasets/active", "/api/v1/models", "/api/v1/forecasts/latest", "/api/v1/data/verification"):
        st, body = get(path)
        summary = None
        if isinstance(body, dict): summary = sorted(body.keys())[:8]
        elif isinstance(body, list): summary = f"list[{len(body)}]"
        step("read " + path, status=st, shape=summary)
    st, fc = get("/api/v1/forecasts/latest")
    if isinstance(fc, dict):
        step("latest_forecast_detail", id=fc.get("id"), model_version=fc.get("model_version"), origin=fc.get("origin_period") or fc.get("origin"))
finally:
    proc.terminate()
    try: proc.wait(15)
    except subprocess.TimeoutExpired: proc.kill(); proc.wait()
    log.close()
    step("stop", returncode=proc.returncode, total_seconds=round(time.time() - t0, 2))
report["source_backup_unchanged"] = sha(backup / "metadata.db") == src_sha_before and tree(backup) == src_tree_before
print(json.dumps(report, indent=2, default=str))
