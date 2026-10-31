"""``/api/v1/billing`` (sandbox only). Wire with ``app.include_router(icp_api.billing.router.router)``.

| Method | Path | Who | Purpose |
|---|---|---|---|
| GET  | /billing/status | signed-in member | whether billing is enabled, environment, provider |
| POST | /billing/orders | platform operator | create an order for an organisation (amount = owner decision) |
| GET  | /billing/orders | owner/admin | the organisation's orders |
| GET  | /billing/orders/{order_id} | owner/admin | one order of the organisation |
| POST | /billing/orders/{order_id}/checkout | owner/admin | create the Snap payment page (idempotent) |
| POST | /billing/webhooks/midtrans | Midtrans (no auth; signature) | payment notification |

Order of checks (auth-first invariant): on every authenticated route the caller is resolved first
(401 anonymous, 403 wrong role/kind), and only then ``BILLING_ENABLED`` is checked (503
BILLING_DISABLED / BILLING_MISCONFIGURED), so configuration is never revealed to unauthenticated
callers. The webhook is the only public route; with billing disabled it rejects every notification
with 503 BILLING_DISABLED.
"""

from __future__ import annotations

import json
from typing import Any, Literal

from fastapi import APIRouter, Depends, Request
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, ConfigDict, Field

from icp_api.billing.config import BillingConfig
from icp_api.billing.service import BillingService, public_order, require_enabled
from icp_api.billing.store import BillingStore
from icp_api.errors import ApiError
from icp_api.logging_setup import request_id_var
from icp_api.routers.common import ERRORS, audit, container
from icp_api.security import Principal, rate_limit, require_admin, require_user

router = APIRouter(prefix="/api/v1/billing", tags=["billing"], responses=ERRORS)

BILLING_ADMIN = require_user("admin", allow_operator=False, allow_api_key=False)
DEFAULT_RL = Depends(rate_limit("default", "rate_limit_default_per_minute"))


class BillingStatus(BaseModel):
    enabled: bool
    environment: str
    provider: str
    configured: bool


class OrderCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    org_id: str = Field(min_length=1, max_length=64)
    amount: int = Field(gt=0, description="Whole rupiah, decided by the owner; nothing is priced by the API")
    currency: Literal["IDR"] = "IDR"
    description: str = Field(min_length=3, max_length=200)
    plan_code: Literal["trial", "starter", "professional", "enterprise"] | None = None


class Order(BaseModel):
    id: str
    org_id: str
    provider: str
    environment: str
    amount: int
    currency: str
    status: Literal["created", "pending", "paid", "failed", "expired", "cancelled", "refunded"]
    description: str
    plan_code: str | None = None
    checkout_url: str | None = None
    created_at: str
    updated_at: str


class OrderList(BaseModel):
    items: list[Order]


class NotificationResult(BaseModel):
    order_id: str
    status: str
    duplicate: bool
    outcome: str | None = None


def billing_config() -> BillingConfig:
    return BillingConfig.from_env()


def service(request: Request, config: BillingConfig) -> BillingService:
    override = getattr(request.app.state, "billing_client", None)  # tests inject a fake Snap client
    return BillingService(BillingStore(container(request).db), config, override)


def enabled_config(config: BillingConfig = Depends(billing_config)) -> BillingConfig:
    """Declared after the principal on authenticated routes (auth first), first on the public webhook."""
    require_enabled(config)
    return config


@router.get("/status", response_model=BillingStatus, dependencies=[DEFAULT_RL])
def billing_status(
    _: Principal = Depends(require_user("viewer", allow_operator=True)),
    config: BillingConfig = Depends(enabled_config),
) -> BillingStatus:
    return BillingStatus(
        enabled=config.enabled, environment=config.environment, provider=config.provider, configured=True
    )


@router.post("/orders", response_model=Order, status_code=201, dependencies=[DEFAULT_RL])
def create_order(
    request: Request,
    body: OrderCreate,
    principal: Principal = Depends(require_admin),
    config: BillingConfig = Depends(enabled_config),
) -> Order:
    if not container(request).repo.db.query("SELECT id FROM organizations WHERE id = ?", (body.org_id,)):
        raise ApiError(404, "NOT_FOUND", "Organisasi tidak ditemukan.")
    order = service(request, config).create_order(
        body.org_id, body.amount, body.currency, body.description, body.plan_code, principal.name
    )
    audit(
        request,
        principal,
        "billing.order.create",
        "success",
        target=order["id"],
        details={"org_id": body.org_id},
    )
    return Order(**public_order(order))


@router.get("/orders", response_model=OrderList, dependencies=[DEFAULT_RL])
def list_orders(
    request: Request,
    principal: Principal = Depends(BILLING_ADMIN),
    config: BillingConfig = Depends(enabled_config),
) -> OrderList:
    store = BillingStore(container(request).db)
    return OrderList(items=[Order(**public_order(o)) for o in store.list_for_org(str(principal.org_id))])


@router.get("/orders/{order_id}", response_model=Order, dependencies=[DEFAULT_RL])
def get_order(
    request: Request,
    order_id: str,
    principal: Principal = Depends(BILLING_ADMIN),
    config: BillingConfig = Depends(enabled_config),
) -> Order:
    return Order(**public_order(service(request, config).get_for_org(order_id, str(principal.org_id))))


@router.post("/orders/{order_id}/checkout", response_model=Order, dependencies=[DEFAULT_RL])
def checkout(
    request: Request,
    order_id: str,
    principal: Principal = Depends(BILLING_ADMIN),
    config: BillingConfig = Depends(enabled_config),
) -> Order:
    order = service(request, config).checkout(order_id, str(principal.org_id))
    audit(request, principal, "billing.order.checkout", "success", target=order_id)
    return Order(**public_order(order))


@router.post("/webhooks/midtrans", response_model=NotificationResult, dependencies=[DEFAULT_RL])
async def midtrans_notification(
    request: Request, config: BillingConfig = Depends(enabled_config)
) -> NotificationResult:
    raw = await request.body()
    try:
        payload: Any = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ApiError(422, "BILLING_NOTIFICATION_INVALID", "Notifikasi pembayaran tidak valid.") from exc
    svc = service(request, config)
    result = await run_in_threadpool(svc.handle_notification, payload, request_id_var.get())
    return NotificationResult(**result)
