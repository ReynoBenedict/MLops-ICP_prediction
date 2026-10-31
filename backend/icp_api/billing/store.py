"""billing_orders / billing_events (migration 0008) over the shared SQL executor (SQLite or D1).

Every write is a single statement; status changes are compare-and-set on the previous status, so a
concurrent update can never skip a state-machine check."""

from __future__ import annotations

import json
from typing import Any

from icp_api.storage.repository import new_id, utcnow
from icp_api.storage.sql import IntegrityError, SqlExecutor


class DuplicateEvent(Exception):
    """The provider event was already recorded (idempotent replay)."""


class BillingStore:
    def __init__(self, db: SqlExecutor):
        self.db = db

    def create_order(
        self,
        org_id: str,
        environment: str,
        amount: int,
        currency: str,
        description: str,
        plan_code: str | None,
        created_by: str,
    ) -> dict[str, Any]:
        now = utcnow()
        oid = new_id("ord")
        self.db.execute(
            "INSERT INTO billing_orders (id, org_id, provider, environment, amount, currency, status, description, "
            "plan_code, created_by, created_at, updated_at) VALUES (?, ?, 'midtrans', ?, ?, ?, 'created', ?, ?, ?, ?, ?)",
            (oid, org_id, environment, amount, currency, description, plan_code, created_by, now, now),
        )
        order = self.get(oid)
        if order is None:
            raise RuntimeError("billing order was not stored")
        return order

    def get(self, order_id: str) -> dict[str, Any] | None:
        rows = self.db.query("SELECT * FROM billing_orders WHERE id = ?", (order_id,))
        return dict(rows[0]) if rows else None

    def list_for_org(self, org_id: str, limit: int = 50) -> list[dict[str, Any]]:
        return [
            dict(r)
            for r in self.db.query(
                "SELECT * FROM billing_orders WHERE org_id = ? ORDER BY created_at DESC, id DESC LIMIT ?",
                (org_id, limit),
            )
        ]

    def list_open(self, limit: int = 200) -> list[dict[str, Any]]:
        """Orders whose provider status can still change (reconciliation scope)."""
        return [
            dict(r)
            for r in self.db.query(
                "SELECT * FROM billing_orders WHERE status IN ('created', 'pending', 'paid') "
                "ORDER BY created_at, id LIMIT ?",
                (limit,),
            )
        ]

    def set_status(
        self,
        order_id: str,
        expected: str,
        status: str,
        provider_ref: str | None = None,
        checkout_url: str | None = None,
    ) -> bool:
        """Compare-and-set: True when the order still had ``expected`` and now has ``status``."""
        return (
            self.db.execute(
                "UPDATE billing_orders SET status = ?, provider_ref = COALESCE(?, provider_ref), "
                "checkout_url = COALESCE(?, checkout_url), updated_at = ? WHERE id = ? AND status = ?",
                (status, provider_ref, checkout_url, utcnow(), order_id, expected),
            )
            == 1
        )

    def record_event(
        self,
        provider: str,
        event_key: str,
        order_id: str,
        status_from: str,
        status_to: str,
        outcome: str,
        payload: dict[str, Any],
        request_id: str | None,
    ) -> None:
        try:
            self.db.execute(
                "INSERT INTO billing_events (id, provider, event_key, order_id, status_from, status_to, outcome, "
                "payload_json, received_at, request_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    new_id("bev"),
                    provider,
                    event_key,
                    order_id,
                    status_from,
                    status_to,
                    outcome,
                    json.dumps(payload, sort_keys=True, default=str),
                    utcnow(),
                    request_id,
                ),
            )
        except IntegrityError as exc:
            if self.event_exists(provider, event_key):
                raise DuplicateEvent(event_key) from exc
            raise

    def event_exists(self, provider: str, event_key: str) -> bool:
        return bool(
            self.db.query(
                "SELECT 1 AS x FROM billing_events WHERE provider = ? AND event_key = ?",
                (provider, event_key),
            )
        )

    def events_for(self, order_id: str) -> list[dict[str, Any]]:
        return [
            dict(r)
            for r in self.db.query(
                "SELECT * FROM billing_events WHERE order_id = ? ORDER BY received_at, id", (order_id,)
            )
        ]
