"""Operations & commercial readiness v6 (Agent S): API quotas, entitlement consistency, data export,
deletion requests, audit retention, migrations, single-instance enforcement."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from icp_api.entitlements import PLANS

API = "/api/v1"
PW = "Operasi-Uji-v6-2026!"


def _register(client: Any, slug: str) -> dict[str, Any]:
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": f"PT Operasi {slug}",
            "email": f"owner@{slug}.operasi.test",
            "name": f"Owner {slug}",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    return {
        "h": {"Authorization": f"Bearer {body['token']}"},
        "org": body["organization"]["id"],
        "user": body["user"]["id"],
        "email": f"owner@{slug}.operasi.test",
    }


def _admin(client: Any) -> dict[str, str]:
    return {"Authorization": f"Bearer {client.app.state.container.settings.admin_tokens['tester']}"}


def _set_plan(client: Any, org: str, plan_code: str) -> None:
    r = client.post(
        f"{API}/admin/organizations/{org}/plan",
        json={"plan": plan_code, "reason": "uji operasi"},
        headers=_admin(client),
    )
    assert r.status_code == 200, r.text


# -- API quota -----------------------------------------------------------------------------------
def test_api_key_monthly_quota(client, monkeypatch):
    from icp_api import entitlements

    org = _register(client, "kuota")
    _set_plan(client, org["org"], "professional")
    key = client.post(f"{API}/org/api-keys", json={"name": "kuota"}, headers=org["h"]).json()
    kh = {"Authorization": f"Bearer {key['key']}"}
    small = replace(PLANS["professional"], api_requests_per_month=3)
    monkeypatch.setitem(entitlements.PLANS, "professional", small)
    codes = []
    for _ in range(5):
        r = client.get(f"{API}/data/summary", headers=kh)
        codes.append(r.status_code)
        if r.status_code == 200:
            assert r.headers["x-quota-limit"] == "3"
            assert int(r.headers["x-quota-remaining"]) >= 0 and r.headers["x-quota-reset"].endswith(
                "T00:00:00Z"
            )
    assert codes == [200, 200, 200, 429, 429]
    err = client.get(f"{API}/data/summary", headers=kh)
    body = err.json()["error"]
    assert body["code"] == "QUOTA_EXCEEDED"
    assert body["details"]["limit"] == 3 and body["details"]["used"] == 3
    assert body["details"]["reset_at"].endswith("-01T00:00:00Z") and body["details"]["plan"] == "professional"
    assert int(err.headers["retry-after"]) > 0
    # rejected requests are not counted; interactive sessions are never quota-limited
    usage = client.get(f"{API}/org", headers=org["h"]).json()["usage_this_month"]
    assert usage["api.request"] == 3
    assert client.get(f"{API}/data/summary", headers=org["h"]).status_code == 200


# -- entitlement consistency ---------------------------------------------------------------------
PRESENTATION_FIELDS = {"code", "label_id", "label_en", "price_note_id", "price_note_en"}


def _flip(monkeypatch: pytest.MonkeyPatch, **changes: Any) -> None:
    from icp_api import entitlements

    monkeypatch.setitem(entitlements.PLANS, "trial", replace(PLANS["trial"], **changes))


def _fresh(client: Any, slug: str) -> dict[str, Any]:
    return _register(client, f"ent-{slug}")


def _seats(client: Any, mp: Any) -> None:
    _flip(mp, seats=2)
    o = _fresh(client, "seats")
    ok = client.post(
        f"{API}/org/members",
        json={"email": "a@ent-seats.test", "name": "A", "role": "viewer"},
        headers=o["h"],
    )
    over = client.post(
        f"{API}/org/members",
        json={"email": "b@ent-seats.test", "name": "B", "role": "viewer"},
        headers=o["h"],
    )
    assert ok.status_code == 201 and over.json()["error"]["code"] == "SEAT_LIMIT"


def _horizon(client: Any, mp: Any) -> None:
    _flip(mp, forecast_horizon_max=2)
    o = _fresh(client, "horizon")
    r = client.post(f"{API}/forecasts", json={"horizon": 3}, headers=o["h"])
    assert r.status_code == 403 and r.json()["error"]["code"] == "PLAN_LIMIT"
    assert client.get(f"{API}/auth/whoami", headers=o["h"]).json()["horizon_max"] == 2


def _scenarios(client: Any, mp: Any) -> None:
    _flip(mp, scenarios=False)
    o = _fresh(client, "scen")
    assert client.get(f"{API}/scenarios", headers=o["h"]).json()["error"]["code"] == "FEATURE_NOT_IN_PLAN"
    r = client.post(f"{API}/scenarios", json={"name": "S", "horizon": 1}, headers=o["h"])
    assert r.json()["error"]["code"] == "FEATURE_NOT_IN_PLAN"


def _saved_scenarios(client: Any, mp: Any) -> None:
    _flip(mp, saved_scenarios_max=1)
    o = _fresh(client, "saved")
    assert (
        client.post(f"{API}/scenarios", json={"name": "S1", "horizon": 1}, headers=o["h"]).status_code == 201
    )
    r = client.post(f"{API}/scenarios", json={"name": "S2", "horizon": 1}, headers=o["h"])
    assert r.json()["error"]["code"] == "PLAN_LIMIT"


def _reports_pdf(client: Any, mp: Any) -> None:
    _flip(mp, reports_pdf=False)
    o = _fresh(client, "pdf")
    r = client.post(f"{API}/reports", json={"kind": "data_provenance", "formats": ["pdf"]}, headers=o["h"])
    assert r.json()["error"]["code"] == "FEATURE_NOT_IN_PLAN"


def _reports_excel(client: Any, mp: Any) -> None:
    _flip(mp, reports_excel=False)
    o = _fresh(client, "xlsx")
    r = client.post(
        f"{API}/reports", json={"kind": "data_provenance", "formats": ["xlsx", "csv"]}, headers=o["h"]
    )
    assert r.status_code == 201 and r.json()["formats"] == ["csv"]


def _alerts(client: Any, mp: Any) -> None:
    _flip(mp, alerts=False)
    o = _fresh(client, "alerts")
    assert client.get(f"{API}/alerts/rules", headers=o["h"]).json()["error"]["code"] == "FEATURE_NOT_IN_PLAN"
    r = client.post(f"{API}/alerts/rules", json={"kind": "icp_published"}, headers=o["h"])
    assert r.json()["error"]["code"] == "FEATURE_NOT_IN_PLAN"


def _alert_rules(client: Any, mp: Any) -> None:
    _flip(mp, alert_rules_max=1)
    o = _fresh(client, "rules")
    assert (
        client.post(f"{API}/alerts/rules", json={"kind": "icp_published"}, headers=o["h"]).status_code == 201
    )
    r = client.post(f"{API}/alerts/rules", json={"kind": "data_stale"}, headers=o["h"])
    assert r.status_code == 403 and r.json()["error"]["code"] == "PLAN_LIMIT"


def _api_access(client: Any, mp: Any) -> None:
    _flip(mp, api_access=False)
    o = _fresh(client, "api")
    r = client.post(f"{API}/org/api-keys", json={"name": "k"}, headers=o["h"])
    assert r.json()["error"]["code"] == "FEATURE_NOT_IN_PLAN"


def _api_requests(client: Any, mp: Any) -> None:
    _flip(mp, api_access=True, api_requests_per_month=1)
    o = _fresh(client, "quota")
    kh = {
        "Authorization": f"Bearer {client.post(f'{API}/org/api-keys', json={'name': 'k'}, headers=o['h']).json()['key']}"
    }
    assert client.get(f"{API}/data/summary", headers=kh).status_code == 200
    assert client.get(f"{API}/data/summary", headers=kh).json()["error"]["code"] == "QUOTA_EXCEEDED"


def _history(client: Any, mp: Any) -> None:
    _flip(mp, history_years=1)
    o = _fresh(client, "hist")
    body = client.get(f"{API}/data/history", headers=o["h"]).json()
    floor = body["earliest_period"]
    assert body["history_years"] == 1 and floor and all(p["period"] >= floor for p in body["points"])
    assert len([p for p in body["points"] if p.get("icp_price") is not None]) <= 12


def _insights(client: Any, mp: Any) -> None:
    _flip(mp, insights=False)
    o = _fresh(client, "ins")
    assert client.get(f"{API}/insights", headers=o["h"]).json()["error"]["code"] == "FEATURE_NOT_IN_PLAN"


ENFORCEMENT = {
    "seats": _seats,
    "forecast_horizon_max": _horizon,
    "scenarios": _scenarios,
    "saved_scenarios_max": _saved_scenarios,
    "reports_pdf": _reports_pdf,
    "reports_excel": _reports_excel,
    "alerts": _alerts,
    "alert_rules_max": _alert_rules,
    "api_access": _api_access,
    "api_requests_per_month": _api_requests,
    "history_years": _history,
    "insights": _insights,
}


def test_every_plan_field_has_an_enforcement_check(client):
    """/plans is generated from entitlements.PLANS (single source) and every non-presentational field
    has a server-side enforcement test below. A new plan field fails here until it is enforced."""
    published = client.get(f"{API}/plans").json()
    assert published == [p.to_dict() for p in PLANS.values()]
    fields = set(published[0]) - PRESENTATION_FIELDS
    assert fields == set(ENFORCEMENT), sorted(fields ^ set(ENFORCEMENT))


@pytest.mark.parametrize("field", sorted(ENFORCEMENT))
def test_plan_field_is_enforced_server_side(client, monkeypatch, field):
    ENFORCEMENT[field](client, monkeypatch)


# -- organisation data export --------------------------------------------------------------------
def _invite(client: Any, owner_h: dict[str, str], email: str, role: str) -> dict[str, Any]:
    inv = client.post(
        f"{API}/org/members", json={"email": email, "name": role, "role": role}, headers=owner_h
    )
    assert inv.status_code == 201, inv.text
    temp = inv.json()["temporary_password"]
    tok = client.post(f"{API}/auth/login", json={"email": email, "password": temp}).json()["token"]
    h = {"Authorization": f"Bearer {tok}"}
    client.post(f"{API}/auth/password", json={"current_password": temp, "new_password": PW}, headers=h)
    tok = client.post(f"{API}/auth/login", json={"email": email, "password": PW}).json()["token"]
    return {"h": {"Authorization": f"Bearer {tok}"}, "id": inv.json()["member"]["id"], "email": email}


EXPORT_FILES = (
    "manifest.json",
    "organization.json",
    "members.json",
    "members.csv",
    "sessions.json",
    "api_keys.json",
    "scenarios.json",
    "scenarios.csv",
    "alert_rules.json",
    "alert_events.json",
    "reports.json",
    "forecasts.json",
    "usage_events.json",
    "usage_events.csv",
    "plan_requests.json",
    "deletion_requests.json",
    "audit_events.json",
)


def test_owner_exports_only_their_own_org_data(client):
    import io
    import json
    import zipfile

    a, b = _register(client, "ekspor-a"), _register(client, "ekspor-b")
    _set_plan(client, a["org"], "professional")
    admin_m = _invite(client, a["h"], "admin@ekspor-a.operasi.test", "admin")
    client.post(f"{API}/scenarios", json={"name": '=HYPERLINK("x")', "horizon": 1}, headers=a["h"])
    client.post(f"{API}/scenarios", json={"name": "Milik B", "horizon": 1}, headers=b["h"])
    key = client.post(f"{API}/org/api-keys", json={"name": "ekspor"}, headers=a["h"]).json()
    # who may export
    assert client.get(f"{API}/org/export", headers=admin_m["h"]).status_code == 403
    assert (
        client.get(f"{API}/org/export", headers={"Authorization": f"Bearer {key['key']}"}).status_code == 403
    )
    assert client.get(f"{API}/org/export", headers=_admin(client)).status_code == 403
    assert client.get(f"{API}/org/export").status_code == 401
    r = client.get(f"{API}/org/export", headers=a["h"])
    assert r.status_code == 200 and r.headers["content-type"] == "application/zip"
    assert r.headers["cache-control"] == "private, no-store"
    assert f"org-export_{a['org']}_" in r.headers["content-disposition"]
    z = zipfile.ZipFile(io.BytesIO(r.content))
    names = set(z.namelist())
    assert names == set(EXPORT_FILES)
    blob = b"".join(z.read(n) for n in names).decode("utf-8")
    assert a["org"] in blob and b["org"] not in blob and "Milik B" not in blob
    for forbidden in ("password_hash", "token_hash", "key_hash", "scrypt$", key["key"], PW):
        assert forbidden not in blob, forbidden
    members = json.loads(z.read("members.json"))
    assert {m["email"] for m in members} == {a["email"], admin_m["email"]}
    assert "'=HYPERLINK" in z.read("scenarios.csv").decode("utf-8-sig")  # CSV cells neutralised
    manifest = json.loads(z.read("manifest.json"))
    assert manifest["org_id"] == a["org"] and manifest["format_version"] == 1
    # the export is audited
    audit = client.get(f"{API}/audit", params={"limit": 50}, headers=_admin(client)).json()["items"]
    assert any(e["action"] == "org.export" and e["target"] == a["org"] for e in audit)


# -- deletion requests ---------------------------------------------------------------------------
def test_account_deletion_request_flow(client):
    o = _register(client, "hapus-akun")
    m = _invite(client, o["h"], "analis@hapus-akun.operasi.test", "analyst")
    # the owner cannot delete just their account
    r = client.post(f"{API}/org/deletion-requests", json={"scope": "account"}, headers=o["h"])
    assert r.status_code == 409 and r.json()["error"]["code"] == "OWNER_ACCOUNT"
    # only the owner may request organisation deletion
    r = client.post(f"{API}/org/deletion-requests", json={"scope": "organization"}, headers=m["h"])
    assert r.status_code == 403
    r = client.post(
        f"{API}/org/deletion-requests", json={"scope": "account", "reason": "pindah kerja"}, headers=m["h"]
    )
    assert r.status_code == 201, r.text
    req = r.json()
    assert req["status"] == "pending" and req["user_id"] == m["id"] and req["scope"] == "account"
    dup = client.post(f"{API}/org/deletion-requests", json={"scope": "account"}, headers=m["h"])
    assert dup.status_code == 409
    listed = client.get(f"{API}/org/deletion-requests", headers=o["h"]).json()["items"]
    assert req["id"] in {i["id"] for i in listed}
    admin = _admin(client)
    pending = client.get(f"{API}/admin/deletion-requests", params={"status": "pending"}, headers=admin).json()
    assert req["id"] in {i["id"] for i in pending["items"]}
    done = client.post(
        f"{API}/admin/deletion-requests/{req['id']}/resolve",
        json={"decision": "execute", "note": "ok"},
        headers=admin,
    )
    assert done.status_code == 200 and done.json()["status"] == "executed"
    # the account is anonymised, signed out and cannot sign in again
    assert client.get(f"{API}/auth/whoami", headers=m["h"]).status_code == 401
    assert client.post(f"{API}/auth/login", json={"email": m["email"], "password": PW}).status_code == 401
    members = client.get(f"{API}/org/members", headers=o["h"]).json()
    gone = next(x for x in members if x["id"] == m["id"])
    assert gone["email"].endswith("@invalid.local") and gone["status"] == "disabled"
    assert m["email"] not in str(members)
    again = client.post(
        f"{API}/admin/deletion-requests/{req['id']}/resolve", json={"decision": "execute"}, headers=admin
    )
    assert again.status_code == 409


def test_organization_deletion_request_flow_keeps_official_data(client):
    o = _register(client, "hapus-org")
    other = _register(client, "hapus-lain")
    _set_plan(client, o["org"], "professional")
    m = _invite(client, o["h"], "lihat@hapus-org.operasi.test", "viewer")
    key = client.post(f"{API}/org/api-keys", json={"name": "k"}, headers=o["h"]).json()
    sc = client.post(f"{API}/scenarios", json={"name": "Rahasia", "horizon": 1}, headers=o["h"]).json()["id"]
    datasets_before = client.get(f"{API}/datasets/active", headers=o["h"]).json()
    r = client.post(f"{API}/org/deletion-requests", json={"scope": "organization"}, headers=o["h"])
    assert r.status_code == 201
    rid = r.json()["id"]
    # another tenant cannot see or withdraw it; customers cannot execute
    assert client.post(f"{API}/org/deletion-requests/{rid}/withdraw", headers=other["h"]).status_code == 404
    assert rid not in str(client.get(f"{API}/org/deletion-requests", headers=other["h"]).json())
    assert (
        client.post(
            f"{API}/admin/deletion-requests/{rid}/resolve", json={"decision": "execute"}, headers=o["h"]
        ).status_code
        == 403
    )
    admin = _admin(client)
    done = client.post(
        f"{API}/admin/deletion-requests/{rid}/resolve", json={"decision": "execute"}, headers=admin
    )
    assert done.status_code == 200 and done.json()["status"] == "executed"
    for h in (o["h"], m["h"], {"Authorization": f"Bearer {key['key']}"}):
        assert client.get(f"{API}/auth/whoami", headers=h).status_code == 401
    c = client.app.state.container
    org = c.repo.get_organization(o["org"])
    assert org["status"] == "suspended" and org["deleted_at"] and org["name"] == "Organisasi dihapus"
    users = c.repo.list_users(o["org"])
    assert all(u["email"].endswith("@invalid.local") and u["status"] == "disabled" for u in users)
    scen = c.repo.get_scenario(o["org"], sc)
    assert scen["archived_at"] and "Rahasia" not in str(scen)
    assert all(k["revoked_at"] for k in c.repo.list_api_keys(o["org"]))
    # official datasets untouched; the other tenant unaffected; audit trail kept
    assert client.get(f"{API}/datasets/active", headers=other["h"]).json() == datasets_before
    assert client.get(f"{API}/auth/whoami", headers=other["h"]).status_code == 200
    audit = client.get(f"{API}/audit", params={"limit": 100}, headers=admin).json()["items"]
    assert any(e["action"] == "org.deletion.execute" and e["target"] == o["org"] for e in audit)


def test_deletion_request_withdraw_and_decline(client):
    o = _register(client, "hapus-tarik")
    r = client.post(f"{API}/org/deletion-requests", json={"scope": "organization"}, headers=o["h"]).json()
    w = client.post(f"{API}/org/deletion-requests/{r['id']}/withdraw", headers=o["h"])
    assert w.status_code == 200 and w.json()["status"] == "withdrawn"
    assert client.post(f"{API}/org/deletion-requests/{r['id']}/withdraw", headers=o["h"]).status_code == 409
    r2 = client.post(f"{API}/org/deletion-requests", json={"scope": "organization"}, headers=o["h"]).json()
    d = client.post(
        f"{API}/admin/deletion-requests/{r2['id']}/resolve",
        json={"decision": "decline", "note": "kontrak"},
        headers=_admin(client),
    )
    assert (
        d.status_code == 200 and d.json()["status"] == "declined" and d.json()["resolution_note"] == "kontrak"
    )
    assert client.get(f"{API}/auth/whoami", headers=o["h"]).status_code == 200


# -- audit-log retention CLI ---------------------------------------------------------------------
def test_audit_prune_is_dry_run_by_default_and_never_automatic(client, capsys, monkeypatch):
    import json

    from icp_api.cli import main
    from icp_api.storage.repository import new_id

    c = client.app.state.container
    old_ids = [new_id("aud") for _ in range(3)]
    for i in old_ids:
        c.repo.db.execute(
            "INSERT INTO audit_events (id, created_at, actor, action, outcome) VALUES (?, ?, 'test', 'old', 'success')",
            (i, "2001-01-01T00:00:00.000Z"),
        )

    def old_left() -> int:
        return len(c.repo.db.query("SELECT id FROM audit_events WHERE action = 'old'"))

    # retention 0 (default): nothing to do, even with --apply
    assert main(["audit-prune", "--apply"]) == 0
    assert json.loads(capsys.readouterr().out)["retention_days"] == 0 and old_left() == 3
    # dry run (default) only counts
    assert main(["audit-prune", "--older-than-days", "365"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["dry_run"] is True and out["would_delete"] >= 3 and old_left() == 3
    # --apply deletes only rows older than the cutoff and records the prune itself
    recent = c.repo.db.query("SELECT COUNT(*) AS n FROM audit_events WHERE created_at >= '2020-01-01'")[0][
        "n"
    ]
    assert main(["audit-prune", "--older-than-days", "365", "--apply"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["deleted"] >= 3 and old_left() == 0
    after = c.repo.db.query("SELECT COUNT(*) AS n FROM audit_events WHERE created_at >= '2020-01-01'")[0]["n"]
    assert after == recent + 1  # nothing recent removed; the prune is audited
    # AUDIT_RETENTION_DAYS is the default period
    monkeypatch.setenv("AUDIT_RETENTION_DAYS", "400")
    assert main(["audit-prune"]) == 0
    assert json.loads(capsys.readouterr().out)["retention_days"] == 400
    with pytest.raises(SystemExit):
        main(["audit-prune", "--older-than-days", "-1"])


# -- migration safety ----------------------------------------------------------------------------
def test_migrations_apply_in_order_to_an_empty_database_and_are_rerun_safe(tmp_path):
    import re

    from icp_api.storage.migrations import apply_local, current_version, discover, expected_version
    from icp_api.storage.sql import SqliteExecutor

    db = SqliteExecutor(tmp_path / "fresh.db")
    applied = apply_local(db)
    migs = discover()
    assert applied == [m.name for m in migs]  # every file, in numeric order
    assert current_version(db) == expected_version() == len(migs) >= 6
    rows = db.query("SELECT version, name FROM app_schema ORDER BY version")
    assert [(r["version"], r["name"]) for r in rows] == [(m.version, m.name) for m in migs]
    assert apply_local(db) == []  # re-running is a no-op (no duplicate schema rows, no errors)
    assert len(db.query("SELECT version FROM app_schema")) == len(migs)
    # from 0006 on, migrations are additive: no DROP / DELETE / data-rewriting UPDATE
    for m in migs:
        if m.version >= 6:
            sql = re.sub(r"--[^\n]*", "", m.path.read_text(encoding="utf-8")).upper()
            for word in ("DROP ", "DELETE ", "UPDATE "):
                assert word not in sql, (m.name, word)


def test_migration_0006_preserves_existing_rows(tmp_path):
    from icp_api.storage.migrations import MIGRATIONS_DIR, discover
    from icp_api.storage.sql import SqliteExecutor

    db = SqliteExecutor(tmp_path / "upgrade.db")
    for m in discover():
        if m.version <= 5:
            db.executescript(m.path.read_text(encoding="utf-8"))
    db.execute(
        "INSERT INTO organizations (id, name, slug, plan, status, created_at, updated_at) "
        "VALUES ('org_x', 'PT Lama', 'pt-lama', 'trial', 'active', '2026-01-01', '2026-01-01')"
    )
    db.executescript((MIGRATIONS_DIR / "0006_deletion_requests.sql").read_text(encoding="utf-8"))
    row = db.query("SELECT * FROM organizations WHERE id = 'org_x'")[0]
    assert row["name"] == "PT Lama" and row["deleted_at"] is None


def test_single_instance_is_enforced_by_configuration(monkeypatch):
    from icp_api.settings import ConfigError, load_settings

    monkeypatch.setenv("API_INSTANCES", "2")
    with pytest.raises(ConfigError, match="API_INSTANCES"):
        load_settings()


def test_serve_always_runs_one_worker_process(monkeypatch):
    """uvicorn would honour a platform-provided WEB_CONCURRENCY and fork several workers, splitting the
    in-memory rate limits and sign-in throttles; `icp-api serve` pins one process."""
    import uvicorn

    from icp_api.cli import main

    seen: dict[str, Any] = {}
    monkeypatch.setattr(uvicorn, "run", lambda *a, **kw: seen.update(kw))
    monkeypatch.setenv("WEB_CONCURRENCY", "4")
    assert main(["serve"]) == 0
    assert seen["workers"] == 1


def test_market_report_embeds_insights_only_when_the_plan_includes_them(client, monkeypatch):
    """Same rule as GET /insights: a plan without `insights` gets the report without the insight section."""
    from icp_api.services.reports import ReportService

    captured: list[Any] = []
    real_csv = ReportService._csv

    def spy(self: Any, content: dict[str, Any]) -> bytes:
        captured.append(content.get("insights"))
        return real_csv(self, content)

    monkeypatch.setattr(ReportService, "_csv", spy)
    o = _register(client, "ins-lapor")
    _flip(monkeypatch, insights=False)
    r = client.post(f"{API}/reports", json={"kind": "market_monthly", "formats": ["csv"]}, headers=o["h"])
    assert r.status_code == 201, r.text
    assert not captured[-1]
    _flip(monkeypatch, insights=True)
    r = client.post(f"{API}/reports", json={"kind": "market_monthly", "formats": ["csv"]}, headers=o["h"])
    assert r.status_code == 201, r.text
    assert captured[-1]  # insights included when the plan has them
