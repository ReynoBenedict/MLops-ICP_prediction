"""Wave F independent spot checks (QA). Runs the real app in-process (TestClient) on a temp SQLite DB.

Usage: python spotcheck.py <repo_root> <tmp_dir>. Prints a JSON verdict list; no secret values printed.
"""

import hashlib
import io
import json
import os
import secrets
import sqlite3
import sys
import zipfile
from pathlib import Path

repo, tmp = Path(sys.argv[1]), Path(sys.argv[2])
tmp.mkdir(parents=True, exist_ok=False)
ADMIN = "qa-op-" + secrets.token_urlsafe(32)
SB_KEY = "SB-Mid-server-" + secrets.token_urlsafe(24)
PREFIXES = ("SQLITE_", "LOCAL_OBJECT", "MLFLOW_", "ADMIN_API", "GATEWAY_", "MODEL_", "SEED_", "BILLING_",
            "MIDTRANS_", "EMAIL_", "SESSION_", "TEMP_PASSWORD", "API_", "METADATA_", "OBJECT_STORE")
for k in list(os.environ):
    if k.startswith(PREFIXES):
        del os.environ[k]
os.environ.update({
    "APP_ENV": "test", "LOG_LEVEL": "ERROR", "SQLITE_PATH": str(tmp / "m.db"), "LOCAL_OBJECT_DIR": str(tmp / "obj"),
    "MLFLOW_TRACKING_URI": "sqlite:///" + (tmp / "ml.db").as_posix(), "MLFLOW_ARTIFACT_ROOT": str(tmp / "art"),
    "SEED_ICP_PATH": str(repo / "data/raw/dataset.csv"), "SEED_WTI_PATH": str(repo / "data/seed/wti.csv"),
    "SEED_DIR": str(repo / "data/seed"), "ADMIN_API_TOKENS": f"qaoperatorzz:{ADMIN}",
    "RATE_LIMIT_AUTH_PER_MINUTE": "1000", "RATE_LIMIT_DEFAULT_PER_MINUTE": "1000",
    "RATE_LIMIT_ADMIN_PER_MINUTE": "1000", "SESSION_IDLE_TIMEOUT_HOURS": "168",
    "BILLING_ENABLED": "true", "MIDTRANS_ENVIRONMENT": "sandbox", "MIDTRANS_SERVER_KEY": SB_KEY,
    "SOURCE_CACHE_DIR": str(tmp / "cache"), "SOURCE_STATE_DIR": str(tmp / "src"),
})
sys.path[:0] = [str(repo / "backend"), str(repo / "pipeline")]

from fastapi.testclient import TestClient  # noqa: E402

from icp_api.main import create_app  # noqa: E402
from icp_api.settings import load_settings  # noqa: E402

results = []


def check(name, ok, detail=""):
    results.append({"check": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " " + json.dumps(detail, default=str), file=sys.stderr)


op = {"Authorization": f"Bearer {ADMIN}"}


def H(t):
    return {"Authorization": f"Bearer {t}"}


def pw():
    return "Qa-" + secrets.token_urlsafe(16)


A = "/api/v1"
app = create_app(load_settings(), enable_jobs=False)
with TestClient(app) as c:
    owner_pw = pw()
    r = c.post(f"{A}/auth/register", json={"organization_name": "QA Wave F (uji lokal v7)",
                                           "email": "owner-f@uji-lokal.test", "name": "Owner F",
                                           "password": owner_pw, "accept_terms": True})
    assert r.status_code == 201, r.text
    owner = r.json()["token"]
    org_id = c.get(f"{A}/org", headers=H(owner)).json()["id"]
    r = c.post(f"{A}/admin/organizations/{org_id}/plan", headers=op, json={"plan": "professional", "reason": "qa seats"})
    check("operator raises plan for seats", r.status_code == 200, r.status_code)

    def invite(email, role):
        r = c.post(f"{A}/org/members", headers=H(owner), json={"email": email, "name": email.split("@")[0], "role": role})
        assert r.status_code in (200, 201), r.text
        body = r.json()
        tmp_pw = body["temporary_password"]
        uid = (body.get("member") or body.get("user") or body).get("id")
        lt = c.post(f"{A}/auth/login", json={"email": email, "password": tmp_pw}).json()["token"]
        new = pw()
        rc = c.post(f"{A}/auth/password", headers=H(lt), json={"current_password": tmp_pw, "new_password": new})
        assert rc.status_code in (200, 204), rc.text
        tok = c.post(f"{A}/auth/login", json={"email": email, "password": new}).json()["token"]
        return uid, tok, body

    a1_id, a1, inv1 = invite("admin1-f@uji-lokal.test", "admin")
    a2_id, a2, _ = invite("admin2-f@uji-lokal.test", "admin")
    m_id, mem, _ = invite("viewer-f@uji-lokal.test", "viewer")
    exp = inv1.get("expires_at") or (inv1.get("member") or {}).get("temporary_login_expires_at")
    check("temporary password expiry is returned with the invitation", bool(exp), {"top": sorted(inv1.keys()), "member_has_expiry": bool((inv1.get("member") or {}).get("temporary_login_expires_at"))})

    # 1. admin vs peer admin
    r1 = c.post(f"{A}/org/members/{a2_id}/reset-password", headers=H(a1))
    r2 = c.post(f"{A}/org/members/{a2_id}", headers=H(a1), json={"role": "viewer"})
    r3 = c.post(f"{A}/org/members/{a2_id}", headers=H(a1), json={"status": "disabled"})
    check("admin cannot reset/demote/disable a peer admin",
          (r1.status_code, r2.status_code, r3.status_code) == (403, 403, 403),
          [r1.status_code, r2.status_code, r3.status_code])
    r4 = c.post(f"{A}/org/members/{m_id}/reset-password", headers=H(a1))
    check("admin can still reset a viewer", r4.status_code in (200, 201), r4.status_code)
    r5 = c.post(f"{A}/org/members/{a2_id}/reset-password", headers=H(owner))
    check("owner can reset an admin", r5.status_code in (200, 201), r5.status_code)
    vtmp = r4.json()["temporary_password"]
    vt = c.post(f"{A}/auth/login", json={"email": "viewer-f@uji-lokal.test", "password": vtmp}).json()["token"]
    vnew = pw()
    c.post(f"{A}/auth/password", headers=H(vt), json={"current_password": vtmp, "new_password": vnew})
    mem = c.post(f"{A}/auth/login", json={"email": "viewer-f@uji-lokal.test", "password": vnew}).json()["token"]

    # 2. deletion-request withdrawal
    r = c.post(f"{A}/org/deletion-requests", headers=H(mem), json={"scope": "account", "reason": "qa"})
    rid = r.json().get("id")
    w_owner = c.post(f"{A}/org/deletion-requests/{rid}/withdraw", headers=H(owner))
    w_admin = c.post(f"{A}/org/deletion-requests/{rid}/withdraw", headers=H(a1))
    w_self = c.post(f"{A}/org/deletion-requests/{rid}/withdraw", headers=H(mem))
    check("only the requester withdraws a deletion request",
          (w_owner.status_code, w_admin.status_code, w_self.status_code) == (404, 404, 200),
          [r.status_code, w_owner.status_code, w_admin.status_code, w_self.status_code])

    # 3. export: only the owner's own sessions, no hashes
    r = c.get(f"{A}/org/export", headers=H(owner))
    detail = {"status": r.status_code}
    ok = False
    if r.status_code == 200:
        z = zipfile.ZipFile(io.BytesIO(r.content))
        names = z.namelist()
        sess_name = next((n for n in names if n.endswith("sessions.json")), None)
        sessions = json.loads(z.read(sess_name)) if sess_name else []
        members = c.get(f"{A}/org/members", headers=H(owner)).json()
        members = members.get("items", members) if isinstance(members, dict) else members
        owner_uid = [m["id"] for m in members if m["email"] == "owner-f@uji-lokal.test"]
        users_in = sorted({s.get("user_id") for s in sessions})
        blob = b"".join(z.read(n) for n in names)
        detail = {"session_users": len(users_in), "only_owner": users_in == owner_uid,
                  "scrypt_hash_present": b"scrypt$" in blob, "token_hash_key_present": b"token_hash" in blob,
                  "operator_name_present": b"qaoperatorzz" in blob, "files": len(names)}
        leaks = []
        for n in names:
            data = z.read(n)
            i = data.find(b"qaoperatorzz")
            if i >= 0:
                leaks.append({"file": n, "context": data[max(0, i - 120): i + 40].decode("utf-8", "replace")})
        detail["operator_name_locations"] = leaks
        ok = users_in == owner_uid and not detail["scrypt_hash_present"] and not detail["token_hash_key_present"] \
            and not detail["operator_name_present"]
    check("export holds only the owner's sessions, no hashes, no operator name", ok, detail)

    # 4. idle timeout (age admin1's session beyond 168 h directly in the temp DB)
    db = sqlite3.connect(str(tmp / "m.db"))
    th = hashlib.sha256(a1.encode()).hexdigest()
    row = db.execute("SELECT id FROM sessions WHERE token_hash = ?", (th,)).fetchone()
    db.execute("UPDATE sessions SET last_seen_at = '2026-01-01T00:00:00.000Z' WHERE token_hash = ?", (th,))
    db.commit()
    ro = c.get(f"{A}/org", headers=H(a1))
    after = db.execute("SELECT revoked_at FROM sessions WHERE token_hash = ?", (th,)).fetchone()
    check("idle session is refused and revoked server-side",
          row is not None and ro.status_code == 401 and after is not None and after[0] is not None,
          {"status": ro.status_code, "revoked": bool(after and after[0])})

    s_list = c.get(f"{A}/auth/sessions", headers=H(owner))
    other = c.post(f"{A}/auth/login", json={"email": "owner-f@uji-lokal.test", "password": owner_pw}).json()["token"]
    rv = c.post(f"{A}/auth/sessions/revoke-others", headers=H(owner))
    check("sign out everywhere else ends the other session and keeps the current one",
          rv.status_code == 200 and c.get(f"{A}/org", headers=H(other)).status_code == 401
          and c.get(f"{A}/org", headers=H(owner)).status_code == 200,
          rv.json() if rv.status_code == 200 else rv.status_code)
    keys = sorted(s_list.json()[0].keys()) if s_list.status_code == 200 and s_list.json() else None
    check("session list exposes no token material or raw user agent",
          keys is not None and not any(k in ("token", "token_hash", "user_agent") for k in keys), keys)

    # 5. billing webhook (test-only config: BILLING_ENABLED=true, random sandbox-format key)
    class FakeSnap:
        def create_transaction(self, order):
            return {"token": "t", "redirect_url": "https://app.sandbox.midtrans.com/snap/v4/redirection/qa"}

        def status(self, order_id):
            return {}

    app.state.billing_client = FakeSnap()
    o = c.post(f"{A}/billing/orders", headers=op, json={"org_id": org_id, "amount": 150000, "currency": "IDR",
                                                       "description": "QA sandbox order"})
    check("operator creates an order", o.status_code == 201, o.status_code)
    oid = o.json()["id"]
    ck = c.post(f"{A}/billing/orders/{oid}/checkout", headers=H(owner))
    check("checkout moves the order to pending", ck.status_code == 200 and ck.json().get("status") == "pending",
          ck.status_code)

    def sig(order_id, sc, ga, key=SB_KEY):
        return hashlib.sha512((order_id + sc + ga + key).encode()).hexdigest()

    base = {"order_id": oid, "status_code": "200", "gross_amount": "150000.00", "currency": "IDR",
            "transaction_status": "settlement", "fraud_status": "accept", "transaction_id": "qa-tx-1",
            "payment_type": "bank_transfer"}
    bad = dict(base, signature_key=sig(oid, "200", "150000.00", key="SB-Mid-server-wrong"))
    rb = c.post(f"{A}/billing/webhooks/midtrans", json=bad)
    amt = dict(base, gross_amount="1.00")
    amt["signature_key"] = sig(oid, "200", "1.00")
    ra = c.post(f"{A}/billing/webhooks/midtrans", json=amt)
    db.execute("UPDATE billing_orders SET environment='production' WHERE id=?", (oid,))
    db.commit()
    good = dict(base, signature_key=sig(oid, "200", "150000.00"))
    re_ = c.post(f"{A}/billing/webhooks/midtrans", json=good)
    db.execute("UPDATE billing_orders SET environment='sandbox' WHERE id=?", (oid,))
    db.commit()
    st_before = c.get(f"{A}/billing/orders/{oid}", headers=H(owner)).json()["status"]
    check("webhook rejects bad signature, bad amount and wrong environment; order untouched",
          (rb.status_code, ra.status_code, re_.status_code) == (401, 409, 409) and st_before == "pending",
          [rb.status_code, ra.status_code, re_.status_code, st_before])
    g1 = c.post(f"{A}/billing/webhooks/midtrans", json=good)
    g2 = c.post(f"{A}/billing/webhooks/midtrans", json=good)
    ev = db.execute("SELECT COUNT(*) FROM billing_events WHERE order_id=?", (oid,)).fetchone()[0]
    st = c.get(f"{A}/billing/orders/{oid}", headers=H(owner)).json()["status"]
    stored = db.execute("SELECT payload_json FROM billing_events WHERE order_id=?", (oid,)).fetchone()[0]
    check("valid notification applies once; replay is idempotent; signature not stored",
          g1.status_code == 200 and g2.status_code == 200 and g2.json().get("duplicate") is True and ev == 1
          and st == "paid" and "signature_key" not in stored,
          {"replay_duplicate": g2.json().get("duplicate"), "events": ev, "status": st})
    late = dict(base, transaction_status="pending", status_code="201", transaction_id="qa-tx-2")
    late["signature_key"] = sig(oid, "201", "150000.00")
    rl = c.post(f"{A}/billing/webhooks/midtrans", json=late)
    st2 = c.get(f"{A}/billing/orders/{oid}", headers=H(owner)).json()["status"]
    check("late 'pending' after 'paid' is ignored (200, no change)", rl.status_code == 200 and st2 == "paid",
          [rl.status_code, st2])
    plan_now = c.get(f"{A}/org", headers=H(owner)).json().get("plan")
    check("no automatic plan change on paid", plan_now == "professional", plan_now)
    db.close()

# 6. EMAIL_ENABLED=true is refused at start-up
os.environ["EMAIL_ENABLED"] = "true"
try:
    load_settings()
    refused, msg = False, ""
except Exception as e:  # noqa: BLE001
    refused, msg = True, type(e).__name__ + ": " + str(e)[:200]
check("EMAIL_ENABLED=true is refused at start-up", refused, msg)
print(json.dumps(results, indent=2, default=str))
