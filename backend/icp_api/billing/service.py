"""Billing use cases: order creation (operator), checkout (tenant), notification processing, reconciliation.

Error codes (all with Indonesian messages):
  503 BILLING_DISABLED, 503 BILLING_MISCONFIGURED, 404 BILLING_ORDER_NOT_FOUND,
  401 BILLING_SIGNATURE_INVALID, 409 BILLING_AMOUNT_MISMATCH, 409 BILLING_ENVIRONMENT_MISMATCH,
  409 BILLING_ORDER_STATE, 422 BILLING_NOTIFICATION_INVALID, 502 BILLING_PROVIDER_ERROR.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from icp_api.billing import midtrans
from icp_api.billing.config import BillingConfig
from icp_api.billing.domain import IllegalTransition, next_status
from icp_api.billing.store import BillingStore, DuplicateEvent
from icp_api.errors import ApiError

logger = logging.getLogger(__name__)

MAX_AMOUNT = 1_000_000_000  # sanity bound (IDR); the actual amount is always an operator/owner decision


def require_enabled(config: BillingConfig) -> None:
    if not config.enabled:
        raise ApiError(
            503,
            "BILLING_DISABLED",
            "Pembayaran belum diaktifkan di layanan ini. Hubungi kami untuk tagihan dan perubahan paket.",
        )
    problems = config.problems()
    if problems:
        logger.error("billing misconfigured: %s", "; ".join(problems))
        raise ApiError(
            503, "BILLING_MISCONFIGURED", "Konfigurasi pembayaran tidak lengkap atau tidak konsisten."
        )


def public_order(order: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "id",
        "org_id",
        "provider",
        "environment",
        "amount",
        "currency",
        "status",
        "description",
        "plan_code",
        "checkout_url",
        "created_at",
        "updated_at",
    )
    return {k: order.get(k) for k in keys}


class BillingService:
    def __init__(self, store: BillingStore, config: BillingConfig, client: midtrans.SnapClient | None = None):
        self.store = store
        self.config = config
        self._client = client

    @property
    def client(self) -> midtrans.SnapClient:
        if self._client is None:
            self._client = midtrans.SnapClient(
                self.config.server_key, self.config.environment, timeout=self.config.timeout_seconds
            )
        return self._client

    # -- operator ---------------------------------------------------------------------------------
    def create_order(
        self, org_id: str, amount: int, currency: str, description: str, plan_code: str | None, actor: str
    ) -> dict[str, Any]:
        require_enabled(self.config)
        if currency != self.config.currency:
            raise ApiError(422, "VALIDATION_ERROR", "Mata uang tagihan harus IDR.")
        if not 0 < amount <= MAX_AMOUNT:
            raise ApiError(422, "VALIDATION_ERROR", "Jumlah tagihan tidak valid.")
        return self.store.create_order(
            org_id, self.config.environment, amount, currency, description, plan_code, actor
        )

    # -- tenant -----------------------------------------------------------------------------------
    def get_for_org(self, order_id: str, org_id: str) -> dict[str, Any]:
        order = self.store.get(order_id)
        if order is None or order["org_id"] != org_id:  # another tenant's order looks like a missing one
            raise ApiError(404, "BILLING_ORDER_NOT_FOUND", "Tagihan tidak ditemukan.")
        return order

    def checkout(self, order_id: str, org_id: str) -> dict[str, Any]:
        require_enabled(self.config)
        order = self.get_for_org(order_id, org_id)
        if order["environment"] != self.config.environment:
            raise ApiError(
                409, "BILLING_ENVIRONMENT_MISMATCH", "Tagihan ini dibuat untuk lingkungan pembayaran lain."
            )
        if order["status"] == "pending" and order.get("checkout_url"):
            return order  # idempotent: the same payment page again
        if order["status"] != "created":
            raise ApiError(
                409, "BILLING_ORDER_STATE", f"Tagihan berstatus {order['status']} tidak dapat dibayar."
            )
        try:
            tx = self.client.create_transaction(order)
        except midtrans.ProviderError as exc:
            logger.warning("snap transaction failed: %s", exc, extra={"order_id": order_id})
            raise ApiError(
                502, "BILLING_PROVIDER_ERROR", "Penyedia pembayaran tidak dapat dihubungi. Coba lagi."
            ) from exc
        next_status("created", "pending")
        if not self.store.set_status(order_id, "created", "pending", checkout_url=tx["redirect_url"]):
            current = self.store.get(order_id) or order
            if current["status"] != "pending":
                raise ApiError(409, "BILLING_ORDER_STATE", f"Tagihan berstatus {current['status']}.")
        refreshed = self.store.get(order_id)
        return refreshed or order

    # -- provider notification --------------------------------------------------------------------
    def handle_notification(self, payload: Any, request_id: str | None) -> dict[str, Any]:
        """Verify and apply one Midtrans HTTP notification. Replays are idempotent (200, no change)."""
        require_enabled(self.config)
        if not isinstance(payload, dict):
            raise ApiError(422, "BILLING_NOTIFICATION_INVALID", "Notifikasi pembayaran tidak valid.")
        if not midtrans.signature_valid(payload, self.config.server_key):
            raise ApiError(401, "BILLING_SIGNATURE_INVALID", "Tanda tangan notifikasi tidak valid.")
        order = self.store.get(str(payload.get("order_id")))
        if order is None:
            raise ApiError(404, "BILLING_ORDER_NOT_FOUND", "Tagihan tidak ditemukan.")
        if order["environment"] != self.config.environment:
            raise ApiError(
                409, "BILLING_ENVIRONMENT_MISMATCH", "Notifikasi berasal dari lingkungan pembayaran lain."
            )
        amount = midtrans.parse_amount(payload.get("gross_amount"))
        currency = str(payload.get("currency") or order["currency"])
        if amount is None or amount != Decimal(int(order["amount"])) or currency != order["currency"]:
            raise ApiError(
                409, "BILLING_AMOUNT_MISMATCH", "Jumlah atau mata uang tidak sesuai dengan tagihan."
            )
        target = midtrans.map_status(payload.get("transaction_status"), payload.get("fraud_status"))
        key = midtrans.event_key(payload)
        stored_payload = {k: v for k, v in payload.items() if k != "signature_key"}
        if self.store.event_exists(midtrans.PROVIDER, key):
            return {"order_id": order["id"], "status": order["status"], "duplicate": True}
        current = order["status"]
        outcome, new = "no_change", current
        if target is not None:
            try:
                new, changed = next_status(current, target)
            except IllegalTransition:
                outcome = "ignored_transition"
                logger.warning(
                    "ignored billing transition %s -> %s", current, target, extra={"order_id": order["id"]}
                )
            else:
                if changed:
                    ref = str(payload.get("transaction_id") or "") or None
                    if self.store.set_status(order["id"], current, new, provider_ref=ref):
                        outcome = "applied"
                    else:  # a concurrent notification moved it first; re-evaluate against the fresh state
                        fresh = self.store.get(order["id"]) or order
                        current, new = fresh["status"], fresh["status"]
        try:
            self.store.record_event(
                midtrans.PROVIDER, key, order["id"], current, new, outcome, stored_payload, request_id
            )
        except DuplicateEvent:
            return {"order_id": order["id"], "status": new, "duplicate": True}
        return {"order_id": order["id"], "status": new, "duplicate": False, "outcome": outcome}

    # -- reconciliation (dry run) -----------------------------------------------------------------
    def reconcile(self, limit: int = 200) -> dict[str, Any]:
        """Compare open orders with the provider's status. Read-only: nothing is written."""
        if self.config.problems():
            raise ApiError(
                503, "BILLING_MISCONFIGURED", "Konfigurasi pembayaran tidak lengkap atau tidak konsisten."
            )
        rows = []
        for order in self.store.list_open(limit):
            entry: dict[str, Any] = {"order_id": order["id"], "stored": order["status"]}
            try:
                data = self.client.status(order["id"])
            except midtrans.ProviderError as exc:
                entry.update(provider=None, verdict="provider_error", detail=str(exc))
                rows.append(entry)
                continue
            if str(data.get("status_code")) == "404":
                entry.update(
                    provider=None, verdict="unknown_at_provider" if order["status"] != "created" else "ok"
                )
                rows.append(entry)
                continue
            provider_status = midtrans.map_status(data.get("transaction_status"), data.get("fraud_status"))
            amount = midtrans.parse_amount(data.get("gross_amount"))
            entry["provider"] = provider_status
            if amount is not None and amount != Decimal(int(order["amount"])):
                entry["verdict"] = "amount_mismatch"
            elif provider_status is None or provider_status == order["status"]:
                entry["verdict"] = "ok"
            else:
                try:
                    next_status(order["status"], provider_status)
                    entry["verdict"] = "behind_provider"  # a notification was missed; would be applied
                except IllegalTransition:
                    entry["verdict"] = "conflict"
            rows.append(entry)
        summary: dict[str, int] = {}
        for r in rows:
            summary[r["verdict"]] = summary.get(r["verdict"], 0) + 1
        return {"dry_run": True, "checked": len(rows), "summary": summary, "orders": rows}
