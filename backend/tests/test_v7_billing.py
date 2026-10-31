"""Billing (audit v7 Wave E, SANDBOX ONLY): state machine, Midtrans signature/adapter contract with
synthetic payloads, idempotent notifications, tenant isolation, BILLING_DISABLED, dry-run reconciliation.

No network: the Snap client is a fake or uses httpx.MockTransport. Secrets below are obviously fake."""

from __future__ import annotations

import base64
import hashlib
import json
from typing import Any

import httpx
import pytest

from icp_api.billing import midtrans
from icp_api.billing.config import BillingConfig
from icp_api.billing.domain import TRANSITIONS, IllegalTransition, next_status

FAKE_KEY = "SB-Mid-server-FAKE-TEST-KEY-not-a-secret"
API = "/api/v1/billing"
PW = "Kata-sandi-uji-v7-billing-01"


# --- domain -------------------------------------------------------------------------------------------
def test_state_machine_is_finite_and_rejects_illegal_moves():
    assert set(TRANSITIONS) == {"created", "pending", "paid", "failed", "expired", "cancelled", "refunded"}
    assert next_status("created", "pending") == ("pending", True)
    assert next_status("pending", "paid") == ("paid", True)
    assert next_status("paid", "refunded") == ("refunded", True)
    assert next_status("paid", "paid") == ("paid", False)  # replay = no-op
    for current, target in [
        ("created", "paid"),
        ("paid", "pending"),
        ("failed", "paid"),
        ("refunded", "paid"),
        ("expired", "pending"),
        ("pending", "refunded"),
        ("pending", "bogus"),
    ]:
        with pytest.raises(IllegalTransition):
            next_status(current, target)


# --- Midtrans signature and mapping ----------------------------------------------------------------
def _sig(order_id: str, status_code: str, gross: str, key: str = FAKE_KEY) -> str:
    return hashlib.sha512(f"{order_id}{status_code}{gross}{key}".encode()).hexdigest()


def test_signature_matches_the_documented_formula_and_is_compared_in_constant_time(monkeypatch):
    payload = {"order_id": "ord_1", "status_code": "200", "gross_amount": "150000.00"}
    payload["signature_key"] = _sig("ord_1", "200", "150000.00")
    calls = []
    real = midtrans.hmac.compare_digest
    monkeypatch.setattr(midtrans.hmac, "compare_digest", lambda a, b: calls.append(1) or real(a, b))
    assert midtrans.signature_valid(payload, FAKE_KEY)
    assert calls, "must use hmac.compare_digest"
    assert not midtrans.signature_valid({**payload, "gross_amount": "150001.00"}, FAKE_KEY)
    assert not midtrans.signature_valid({**payload, "signature_key": "0" * 128}, FAKE_KEY)
    assert not midtrans.signature_valid(payload, "SB-Mid-server-OTHER-FAKE-KEY")
    assert not midtrans.signature_valid({k: v for k, v in payload.items() if k != "signature_key"}, FAKE_KEY)
    assert not midtrans.signature_valid(payload, "")


@pytest.mark.parametrize(
    ("status", "fraud", "expected"),
    [
        ("settlement", None, "paid"),
        ("capture", "accept", "paid"),
        ("capture", "challenge", "pending"),
        ("capture", "deny", "failed"),
        ("pending", None, "pending"),
        ("deny", None, "failed"),
        ("failure", None, "failed"),
        ("cancel", None, "cancelled"),
        ("expire", None, "expired"),
        ("refund", None, "refunded"),
        ("partial_refund", None, None),
        ("something_new", None, None),
    ],
)
def test_provider_status_mapping(status, fraud, expected):
    assert midtrans.map_status(status, fraud) == expected


# --- configuration ---------------------------------------------------------------------------------
def test_configuration_defaults_to_disabled_and_refuses_production_and_key_mismatch(monkeypatch):
    for name in ("BILLING_ENABLED", "MIDTRANS_ENVIRONMENT", "MIDTRANS_SERVER_KEY"):
        monkeypatch.delenv(name, raising=False)
    assert BillingConfig.from_env().enabled is False
    assert BillingConfig(enabled=True, server_key=FAKE_KEY).problems() == []
    assert BillingConfig(enabled=True, server_key="").problems()
    assert any("sandbox" in p for p in BillingConfig(enabled=True, server_key="Mid-server-FAKE").problems())
    assert any(
        "production" in p
        for p in BillingConfig(
            enabled=True, environment="production", server_key="Mid-server-FAKE"
        ).problems()
    )


# --- Snap adapter contract (request building only) ---------------------------------------------------
def test_snap_client_builds_the_documented_requests_from_the_stored_order():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.method == "POST":
            return httpx.Response(
                201,
                json={
                    "token": "tok-fake",
                    "redirect_url": "https://app.sandbox.midtrans.com/snap/v4/redirection/tok-fake",
                },
            )
        return httpx.Response(
            200, json={"status_code": "200", "transaction_status": "settlement", "gross_amount": "150000.00"}
        )

    client = midtrans.SnapClient(FAKE_KEY, http=httpx.Client(transport=httpx.MockTransport(handler)))
    order = {"id": "ord_abc", "amount": 150000, "description": "Langganan uji (sandbox)"}
    tx = client.create_transaction(order)
    assert tx["token"] == "tok-fake"
    post = seen[0]
    assert str(post.url) == "https://app.sandbox.midtrans.com/snap/v1/transactions"
    assert post.headers["authorization"] == "Basic " + base64.b64encode(f"{FAKE_KEY}:".encode()).decode()
    body = json.loads(post.content)
    assert body["transaction_details"] == {"order_id": "ord_abc", "gross_amount": 150000}
    client.status("ord_abc")
    assert str(seen[1].url) == "https://api.sandbox.midtrans.com/v2/ord_abc/status"


def test_snap_client_errors_and_production_endpoint_are_refused():
    bad = midtrans.SnapClient(
        FAKE_KEY, http=httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(401, json={})))
    )
    with pytest.raises(midtrans.ProviderError):
        bad.create_transaction({"id": "o", "amount": 1, "description": "x"})
    with pytest.raises(midtrans.ProviderError):
        midtrans.SnapClient(FAKE_KEY, environment="production")


# --- API ---------------------------------------------------------------------------------------------
class FakeSnap:
    def __init__(self) -> None:
        self.created: list[str] = []
        self.statuses: dict[str, dict[str, Any]] = {}

    def create_transaction(self, order: dict[str, Any]) -> dict[str, str]:
        self.created.append(order["id"])
        return {
            "token": f"tok-{order['id']}",
            "redirect_url": f"https://app.sandbox.midtrans.com/snap/v4/redirection/tok-{order['id']}",
        }

    def status(self, order_id: str) -> dict[str, Any]:
        return self.statuses.get(order_id, {"status_code": "404"})


@pytest.fixture(scope="module")
def billing_app(client: Any) -> Any:
    from icp_api.billing.router import router

    app = client.app
    if not any(getattr(r, "path", "").startswith(API) for r in app.routes):
        app.include_router(router)  # main.py wiring is done by the coordinator
    app.state.billing_client = FakeSnap()
    yield app
    app.state.billing_client = None


def _register(client: Any, slug: str) -> dict[str, str]:
    r = client.post(
        "/api/v1/auth/register",
        json={
            "organization_name": f"PT Tagihan {slug} (uji lokal v7)",
            "email": f"owner@{slug}.uji-lokal.test",
            "name": "Pemilik",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert r.status_code == 201, r.text
    return {"Authorization": f"Bearer {r.json()['token']}", "org": r.json()["organization"]["id"]}


@pytest.fixture(scope="module")
def tenants(client: Any, billing_app: Any) -> dict[str, dict[str, str]]:
    return {"a": _register(client, "tagihan-a"), "b": _register(client, "tagihan-b")}


@pytest.fixture
def enabled(monkeypatch):
    monkeypatch.setenv("BILLING_ENABLED", "true")
    monkeypatch.setenv("MIDTRANS_ENVIRONMENT", "sandbox")
    monkeypatch.setenv("MIDTRANS_SERVER_KEY", FAKE_KEY)


def _h(t: dict[str, str]) -> dict[str, str]:
    return {"Authorization": t["Authorization"]}


def _notify(
    client: Any, order: dict[str, Any], status: str = "settlement", gross: str | None = None, **extra: Any
):
    gross = gross if gross is not None else f"{order['amount']}.00"
    payload = {
        "order_id": order["id"],
        "status_code": "200",
        "gross_amount": gross,
        "currency": "IDR",
        "transaction_status": status,
        "transaction_id": f"trx-{order['id']}",
        "fraud_status": "accept",
        **extra,
    }
    payload.setdefault("signature_key", _sig(order["id"], payload["status_code"], gross))
    return client.post(
        f"{API}/webhooks/midtrans", content=json.dumps(payload), headers={"content-type": "application/json"}
    )


def _order(client: Any, admin: dict[str, str], org: str, amount: int = 150000) -> dict[str, Any]:
    r = client.post(
        f"{API}/orders",
        json={"org_id": org, "amount": amount, "description": "Tagihan uji sandbox", "plan_code": "starter"},
        headers=admin,
    )
    assert r.status_code == 201, r.text
    return r.json()


def test_auth_first_then_503_billing_disabled_by_default(client, billing_app, tenants, admin, monkeypatch):
    """Auth-first invariant: 401 anonymous / 403 wrong caller, then 503 BILLING_DISABLED. Only the
    public webhook answers 503 to anyone (it rejects every notification while disabled)."""
    monkeypatch.delenv("BILLING_ENABLED", raising=False)
    monkeypatch.setenv("MIDTRANS_SERVER_KEY", FAKE_KEY)
    order_body = {"org_id": tenants["a"]["org"], "amount": 1, "description": "abc"}
    # anonymous: 401 on every authenticated route, no configuration revealed
    for method, path in [
        ("GET", "/status"),
        ("GET", "/orders"),
        ("GET", "/orders/ord_x"),
        ("POST", "/orders/ord_x/checkout"),
        ("POST", "/orders"),
    ]:
        r = client.request(method, f"{API}{path}", json={} if method == "POST" else None)
        assert r.status_code == 401, (method, path, r.text)
        assert "BILLING" not in r.text
    # a customer on the operator-only route: 403 before the billing switch
    r = client.post(f"{API}/orders", json=order_body, headers=_h(tenants["a"]))
    assert r.status_code == 403 and "BILLING" not in r.text
    # authorised callers: 503 BILLING_DISABLED
    calls = [
        client.get(f"{API}/status", headers=_h(tenants["a"])),
        client.get(f"{API}/orders", headers=_h(tenants["a"])),
        client.get(f"{API}/orders/ord_x", headers=_h(tenants["a"])),
        client.post(f"{API}/orders/ord_x/checkout", headers=_h(tenants["a"])),
        client.post(f"{API}/orders", json=order_body, headers=admin),
        _notify(client, {"id": "ord_x", "amount": 1}),  # public webhook: rejected while disabled
    ]
    for r in calls:
        assert r.status_code == 503, r.text
        body = r.json()["error"]
        assert body["code"] == "BILLING_DISABLED" and "belum diaktifkan" in body["message"]


def test_misconfigured_sandbox_key_is_503(client, billing_app, tenants, monkeypatch):
    monkeypatch.setenv("BILLING_ENABLED", "true")
    monkeypatch.setenv("MIDTRANS_SERVER_KEY", "Mid-server-FAKE-production-looking")
    r = client.get(f"{API}/status", headers=_h(tenants["a"]))
    assert r.status_code == 503 and r.json()["error"]["code"] == "BILLING_MISCONFIGURED"
    assert "Mid-server-FAKE" not in r.text


def test_full_sandbox_flow_with_idempotent_replay(client, billing_app, tenants, admin, enabled):
    a = tenants["a"]
    order = _order(client, admin, a["org"])
    assert order["status"] == "created" and order["environment"] == "sandbox" and order["amount"] == 150000
    # only operators create orders (no customer-chosen prices)
    r = client.post(
        f"{API}/orders", json={"org_id": a["org"], "amount": 1, "description": "murah sekali"}, headers=_h(a)
    )
    assert r.status_code in (401, 403)
    co = client.post(f"{API}/orders/{order['id']}/checkout", headers=_h(a))
    assert co.status_code == 200, co.text
    assert co.json()["status"] == "pending" and co.json()["checkout_url"].startswith(
        "https://app.sandbox.midtrans.com/"
    )
    again = client.post(f"{API}/orders/{order['id']}/checkout", headers=_h(a))
    assert again.json()["checkout_url"] == co.json()["checkout_url"]
    assert billing_app.state.billing_client.created.count(order["id"]) == 1  # idempotent checkout

    first = _notify(client, order)
    assert first.status_code == 200 and first.json() == {
        "order_id": order["id"],
        "status": "paid",
        "duplicate": False,
        "outcome": "applied",
    }
    replay = _notify(client, order)
    assert replay.status_code == 200 and replay.json()["duplicate"] is True
    events = client.app.state.container.db.query(
        "SELECT * FROM billing_events WHERE order_id = ?", (order["id"],)
    )
    assert len(events) == 1 and "signature_key" not in events[0]["payload_json"]
    # a late 'pending' after 'paid' cannot move the order backwards
    late = _notify(client, order, status="pending")
    assert late.status_code == 200 and late.json()["outcome"] == "ignored_transition"
    assert client.get(f"{API}/orders/{order['id']}", headers=_h(a)).json()["status"] == "paid"


def test_notification_validation_rejects_tampering(client, billing_app, tenants, admin, enabled):
    order = _order(client, admin, tenants["a"]["org"], amount=99000)
    client.post(f"{API}/orders/{order['id']}/checkout", headers=_h(tenants["a"]))
    bad_sig = _notify(client, order, signature_key="f" * 128)
    assert bad_sig.status_code == 401 and bad_sig.json()["error"]["code"] == "BILLING_SIGNATURE_INVALID"
    # correctly signed but for a different amount than the stored order
    wrong_amount = _notify(client, order, gross="1000.00")
    assert (
        wrong_amount.status_code == 409 and wrong_amount.json()["error"]["code"] == "BILLING_AMOUNT_MISMATCH"
    )
    wrong_currency = _notify(client, order, currency="USD")
    assert wrong_currency.status_code == 409
    unknown = _notify(client, {"id": "ord_does_not_exist", "amount": 99000})
    assert unknown.status_code == 404 and unknown.json()["error"]["code"] == "BILLING_ORDER_NOT_FOUND"
    junk = client.post(
        f"{API}/webhooks/midtrans", content=b"not json", headers={"content-type": "application/json"}
    )
    assert junk.status_code == 422 and junk.json()["error"]["code"] == "BILLING_NOTIFICATION_INVALID"
    assert client.get(f"{API}/orders/{order['id']}", headers=_h(tenants["a"])).json()["status"] == "pending"


def test_environment_mismatch_is_rejected(client, billing_app, tenants, admin, enabled):
    order = _order(client, admin, tenants["a"]["org"], amount=12000)
    db = client.app.state.container.db
    db.execute(
        "UPDATE billing_orders SET environment = 'production', status = 'pending' WHERE id = ?",
        (order["id"],),
    )
    r = _notify(client, order)
    assert r.status_code == 409 and r.json()["error"]["code"] == "BILLING_ENVIRONMENT_MISMATCH"


def test_orders_are_tenant_isolated(client, billing_app, tenants, admin, enabled):
    order = _order(client, admin, tenants["a"]["org"], amount=5000)
    b = tenants["b"]
    assert client.get(f"{API}/orders/{order['id']}", headers=_h(b)).status_code == 404
    assert client.post(f"{API}/orders/{order['id']}/checkout", headers=_h(b)).status_code == 404
    assert all(o["org_id"] == b["org"] for o in client.get(f"{API}/orders", headers=_h(b)).json()["items"])
    unknown_org = client.post(
        f"{API}/orders",
        json={"org_id": "org_missing", "amount": 5000, "description": "tidak ada"},
        headers=admin,
    )
    assert unknown_org.status_code == 404


def test_reconciliation_is_a_dry_run(client, billing_app, tenants, admin, enabled):
    from icp_api.billing.service import BillingService
    from icp_api.billing.store import BillingStore

    db = client.app.state.container.db
    behind = _order(client, admin, tenants["a"]["org"], amount=7000)
    client.post(f"{API}/orders/{behind['id']}/checkout", headers=_h(tenants["a"]))
    fake = FakeSnap()
    fake.statuses[behind["id"]] = {
        "status_code": "200",
        "transaction_status": "settlement",
        "gross_amount": "7000.00",
    }
    before = db.query("SELECT id, status, updated_at FROM billing_orders ORDER BY id")
    report = BillingService(BillingStore(db), BillingConfig.from_env(), fake).reconcile()
    after = db.query("SELECT id, status, updated_at FROM billing_orders ORDER BY id")
    assert before == after  # nothing written
    row = next(o for o in report["orders"] if o["order_id"] == behind["id"])
    assert report["dry_run"] is True and row == {
        "order_id": behind["id"],
        "stored": "pending",
        "provider": "paid",
        "verdict": "behind_provider",
    }


def test_reconcile_cli_is_dry_run_and_uses_the_injected_client(
    client, billing_app, enabled, monkeypatch, capsys
):
    from icp_api.billing import cli as billing_cli

    monkeypatch.setattr(billing_cli, "make_client", lambda config: FakeSnap())
    code = billing_cli.reconcile_main(["--limit", "5"])
    out = json.loads(capsys.readouterr().out)
    assert out["dry_run"] is True and out["checked"] <= 5
    assert code == (0 if set(out["summary"]) <= {"ok"} else 1)  # 1 = differences found (open sandbox orders)
    from icp_api.cli import main

    assert main(["billing-reconcile", "--limit", "1"]) in (0, 1)
    assert json.loads(capsys.readouterr().out)["dry_run"] is True
    monkeypatch.delenv("BILLING_ENABLED")
    monkeypatch.setenv("MIDTRANS_SERVER_KEY", "")
    assert billing_cli.reconcile_main([]) == 2
