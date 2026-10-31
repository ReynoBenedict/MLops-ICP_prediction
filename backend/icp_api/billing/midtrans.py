"""Midtrans Snap adapter (sandbox only). Builds requests and verifies notifications; the HTTP client is
injected, so tests never reach the network.

Official references (accessed 2026-10-20):

* HTTP notification / webhooks - https://docs.midtrans.com/docs/https-notification-webhooks
  "signature_key" = SHA512(order_id + status_code + gross_amount + ServerKey) (hex); ``gross_amount``
  is the string with two decimals exactly as sent (e.g. "10000.00"). Success: ``settlement``, or
  ``capture`` with ``fraud_status`` ``accept``; failure: ``deny``, ``cancel``, ``expire``, ``failure``.
  The page recommends verifying notification data with the GET status API.
* Snap backend API - https://docs.midtrans.com/reference/backend-integration
  ``POST https://app.sandbox.midtrans.com/snap/v1/transactions``, Basic auth (server key as user,
  empty password), body ``transaction_details.order_id`` / ``gross_amount`` (integer); 201 returns
  ``token`` and ``redirect_url``.
* Get transaction status - https://docs.midtrans.com/reference/get-transaction-status
  ``GET https://api.sandbox.midtrans.com/v2/{order_id}/status``, Basic auth.

Only sandbox base URLs are defined: production URLs were not stated on those pages and production
billing is refused by ``BillingConfig.problems``.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

import httpx

SNAP_URL = {"sandbox": "https://app.sandbox.midtrans.com/snap/v1/transactions"}
API_BASE = {"sandbox": "https://api.sandbox.midtrans.com"}
PROVIDER = "midtrans"


class ProviderError(RuntimeError):
    """The provider call failed or returned an unexpected response (no secret in the message)."""


def signature(order_id: str, status_code: str, gross_amount: str, server_key: str) -> str:
    raw = f"{order_id}{status_code}{gross_amount}{server_key}".encode()
    return hashlib.sha512(raw).hexdigest()


def signature_valid(payload: dict[str, Any], server_key: str) -> bool:
    """Constant-time comparison of the notification's ``signature_key`` with the expected value."""
    fields = [payload.get(k) for k in ("order_id", "status_code", "gross_amount", "signature_key")]
    if not server_key or not all(isinstance(v, str) and v for v in fields):
        return False
    order_id, status_code, gross_amount, presented = (str(v) for v in fields)
    expected = signature(order_id, status_code, gross_amount, server_key)
    return hmac.compare_digest(expected.encode(), presented.strip().lower().encode())


def parse_amount(gross_amount: Any) -> Decimal | None:
    try:
        value = Decimal(str(gross_amount))
    except (InvalidOperation, ValueError):
        return None
    return value if value.is_finite() else None


def map_status(transaction_status: str | None, fraud_status: str | None) -> str | None:
    """Provider status -> domain status (None: no state change, e.g. ``partial_refund``)."""
    ts = (transaction_status or "").lower()
    fraud = (fraud_status or "").lower()
    if ts == "settlement":
        return "paid"
    if ts == "capture":
        if fraud in ("", "accept"):
            return "paid"
        return "failed" if fraud == "deny" else "pending"  # challenge: still under review
    if ts in ("pending", "authorize"):
        return "pending"
    if ts in ("deny", "failure"):
        return "failed"
    if ts == "cancel":
        return "cancelled"
    if ts == "expire":
        return "expired"
    if ts == "refund":
        return "refunded"
    return None


def event_key(payload: dict[str, Any]) -> str:
    """Provider event identity: one notification per (transaction, status, status code, fraud verdict).

    Midtrans re-sends the same notification on retries; those map to the same key and are recorded
    once (UNIQUE (provider, event_key) in billing_events)."""
    parts = [
        str(payload.get("transaction_id") or payload.get("order_id") or ""),
        str(payload.get("transaction_status") or ""),
        str(payload.get("status_code") or ""),
        str(payload.get("fraud_status") or ""),
    ]
    return "|".join(parts)


class HttpLike(Protocol):
    def post(self, url: str, **kw: Any) -> httpx.Response: ...

    def get(self, url: str, **kw: Any) -> httpx.Response: ...


class SnapClient:
    """Builds and sends Snap / status requests through an injected HTTP client."""

    def __init__(
        self,
        server_key: str,
        environment: str = "sandbox",
        http: HttpLike | None = None,
        timeout: float = 15.0,
    ):
        if environment not in SNAP_URL:
            raise ProviderError(f"no Midtrans endpoint configured for environment '{environment}'")
        self.environment = environment
        self._auth = "Basic " + base64.b64encode(f"{server_key}:".encode()).decode()
        self._http: Any = http if http is not None else httpx.Client(timeout=timeout)

    def _headers(self) -> dict[str, str]:
        return {"Authorization": self._auth, "Accept": "application/json", "Content-Type": "application/json"}

    @staticmethod
    def snap_body(order: dict[str, Any]) -> dict[str, Any]:
        """Request body built only from the stored order (amount and id are never taken from input)."""
        return {
            "transaction_details": {"order_id": order["id"], "gross_amount": int(order["amount"])},
            "item_details": [
                {
                    "id": order["id"],
                    "price": int(order["amount"]),
                    "quantity": 1,
                    "name": str(order["description"])[:50],
                }
            ],
            "credit_card": {"secure": True},
        }

    def create_transaction(self, order: dict[str, Any]) -> dict[str, str]:
        try:
            resp = self._http.post(
                SNAP_URL[self.environment], json=self.snap_body(order), headers=self._headers()
            )
        except httpx.HTTPError as exc:
            raise ProviderError(f"Midtrans Snap unreachable: {type(exc).__name__}") from exc
        if resp.status_code != 201:
            raise ProviderError(f"Midtrans Snap returned HTTP {resp.status_code}")
        data = resp.json()
        if not isinstance(data, dict) or not data.get("token") or not data.get("redirect_url"):
            raise ProviderError("Midtrans Snap response lacks token/redirect_url")
        return {"token": str(data["token"]), "redirect_url": str(data["redirect_url"])}

    def status(self, order_id: str) -> dict[str, Any]:
        try:
            resp = self._http.get(
                f"{API_BASE[self.environment]}/v2/{order_id}/status", headers=self._headers()
            )
        except httpx.HTTPError as exc:
            raise ProviderError(f"Midtrans status API unreachable: {type(exc).__name__}") from exc
        if resp.status_code != 200:
            raise ProviderError(f"Midtrans status API returned HTTP {resp.status_code}")
        data = resp.json()
        if not isinstance(data, dict):
            raise ProviderError("Midtrans status API returned a non-object")
        return data
