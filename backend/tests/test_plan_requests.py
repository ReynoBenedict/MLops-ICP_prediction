"""Plan change requests (migration 0005): customer requests, operator decisions, authorisation."""

from __future__ import annotations

from typing import Any

API = "/api/v1"
PW = "Paket-Minta-2026!"


def _owner(client: Any, slug: str) -> dict[str, Any]:
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": f"PT Paket {slug}",
            "email": f"owner@{slug}.paket.test",
            "name": f"Owner {slug}",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert r.status_code == 201, r.text
    return {"h": {"Authorization": f"Bearer {r.json()['token']}"}, "org": r.json()["organization"]["id"]}


def _viewer(client: Any, owner_h: dict[str, str], email: str) -> dict[str, str]:
    inv = client.post(
        f"{API}/org/members", json={"email": email, "name": "V", "role": "viewer"}, headers=owner_h
    )
    assert inv.status_code == 201, inv.text
    temp = inv.json()["temporary_password"]
    tok = client.post(f"{API}/auth/login", json={"email": email, "password": temp}).json()["token"]
    h = {"Authorization": f"Bearer {tok}"}
    assert (
        client.post(
            f"{API}/auth/password", json={"current_password": temp, "new_password": PW}, headers=h
        ).status_code
        == 204
    )
    tok = client.post(f"{API}/auth/login", json={"email": email, "password": PW}).json()["token"]
    return {"Authorization": f"Bearer {tok}"}


def test_plan_request_lifecycle_and_authorisation(client: Any, admin: dict[str, str]):
    a, b = _owner(client, "alfa"), _owner(client, "bravo")
    viewer = _viewer(client, a["h"], "viewer@alfa.paket.test")

    # validation and roles
    assert (
        client.post(f"{API}/org/plan-requests", json={"requested_plan": "trial"}, headers=a["h"]).status_code
        == 422
    )
    assert (
        client.post(f"{API}/org/plan-requests", json={"requested_plan": "gold"}, headers=a["h"]).status_code
        == 422
    )
    assert (
        client.post(
            f"{API}/org/plan-requests", json={"requested_plan": "starter"}, headers=viewer
        ).status_code
        == 403
    )
    assert client.post(f"{API}/org/plan-requests", json={"requested_plan": "starter"}).status_code == 401
    assert (
        client.post(f"{API}/org/plan-requests", json={"requested_plan": "starter"}, headers=admin).status_code
        == 403
    )
    long_note = {"requested_plan": "starter", "note": "x" * 501}
    assert client.post(f"{API}/org/plan-requests", json=long_note, headers=a["h"]).status_code == 422

    r = client.post(
        f"{API}/org/plan-requests",
        json={"requested_plan": "professional", "note": "Tim analis bertambah"},
        headers=a["h"],
    )
    assert r.status_code == 201, r.text
    req = r.json()
    assert (
        req["status"] == "pending" and req["previous_plan"] == "trial" and req["org_name"] == "PT Paket alfa"
    )
    # one pending request per organisation
    dup = client.post(f"{API}/org/plan-requests", json={"requested_plan": "starter"}, headers=a["h"])
    assert dup.status_code == 409

    # visibility: own organisation (any member) only
    assert [i["id"] for i in client.get(f"{API}/org/plan-requests", headers=viewer).json()["items"]] == [
        req["id"]
    ]
    assert client.get(f"{API}/org/plan-requests", headers=b["h"]).json()["items"] == []
    assert client.post(f"{API}/org/plan-requests/{req['id']}/withdraw", headers=b["h"]).status_code == 404

    # only operators decide
    decide = {"decision": "approve", "note": "disetujui"}
    assert (
        client.post(f"{API}/admin/plan-requests/{req['id']}/resolve", json=decide, headers=a["h"]).status_code
        == 403
    )
    assert client.get(f"{API}/admin/plan-requests", headers=a["h"]).status_code == 403
    pending = client.get(f"{API}/admin/plan-requests", params={"status": "pending"}, headers=admin).json()[
        "items"
    ]
    assert req["id"] in {i["id"] for i in pending}
    done = client.post(f"{API}/admin/plan-requests/{req['id']}/resolve", json=decide, headers=admin)
    assert done.status_code == 200 and done.json()["status"] == "approved" and done.json()["resolved_by"]
    assert client.get(f"{API}/org", headers=a["h"]).json()["plan"] == "professional"
    again = client.post(f"{API}/admin/plan-requests/{req['id']}/resolve", json=decide, headers=admin)
    assert again.status_code == 409
    audit = client.get(f"{API}/audit", params={"action": "org.plan.set"}, headers=admin).json()["items"]
    assert any(e["target"] == a["org"] for e in audit)

    # decline and withdraw paths
    r2 = client.post(f"{API}/org/plan-requests", json={"requested_plan": "enterprise"}, headers=a["h"]).json()
    dec = client.post(
        f"{API}/admin/plan-requests/{r2['id']}/resolve",
        json={"decision": "decline", "note": "hubungi sales"},
        headers=admin,
    ).json()
    assert dec["status"] == "declined" and dec["resolution_note"] == "hubungi sales"
    assert client.get(f"{API}/org", headers=a["h"]).json()["plan"] == "professional"
    r3 = client.post(f"{API}/org/plan-requests", json={"requested_plan": "starter"}, headers=b["h"]).json()
    w = client.post(f"{API}/org/plan-requests/{r3['id']}/withdraw", headers=b["h"])
    assert w.status_code == 200 and w.json()["status"] == "withdrawn"
    assert client.post(f"{API}/org/plan-requests/{r3['id']}/withdraw", headers=b["h"]).status_code == 409
    assert (
        client.post(f"{API}/admin/plan-requests/nope/resolve", json=decide, headers=admin).status_code == 404
    )
