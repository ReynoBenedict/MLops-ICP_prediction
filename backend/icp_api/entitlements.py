"""Subscription plans and the features/limits each one unlocks.

Entitlements are enforced server-side (``require_feature`` in ``security``); the frontend only
mirrors them for presentation. Plan changes are an operator action for now (no payment provider
is integrated - see docs/COMMERCIAL_READINESS.md), so a billing integration later only needs to
call ``AccountService.set_plan``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

ROLES = ("viewer", "analyst", "admin", "owner")
ROLE_RANK = {r: i for i, r in enumerate(ROLES)}


@dataclass(frozen=True)
class Plan:
    code: str
    label_id: str
    label_en: str
    price_note_id: str
    seats: int
    forecast_horizon_max: int
    scenarios: bool
    saved_scenarios_max: int
    reports_pdf: bool
    reports_excel: bool
    alerts: bool
    alert_rules_max: int
    api_access: bool
    api_requests_per_month: int
    history_years: int  # price-explorer depth (0 = unlimited)
    insights: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


PLANS: dict[str, Plan] = {
    "trial": Plan(
        "trial",
        "Uji coba",
        "Trial",
        "Gratis, fitur terbatas",
        2,
        3,
        True,
        2,
        True,
        False,
        True,
        2,
        False,
        0,
        3,
        True,
    ),
    "starter": Plan(
        "starter",
        "Starter",
        "Starter",
        "Untuk tim kecil",
        5,
        6,
        True,
        10,
        True,
        True,
        True,
        10,
        False,
        0,
        0,
        True,
    ),
    "professional": Plan(
        "professional",
        "Profesional",
        "Professional",
        "Untuk tim analis",
        20,
        6,
        True,
        100,
        True,
        True,
        True,
        50,
        True,
        20000,
        0,
        True,
    ),
    "enterprise": Plan(
        "enterprise",
        "Enterprise",
        "Enterprise",
        "Kontrak khusus",
        1000,
        6,
        True,
        1000,
        True,
        True,
        True,
        500,
        True,
        500000,
        0,
        True,
    ),
}

# Anonymous visitors (landing page / public market snapshot) get a deliberately small slice.
ANONYMOUS_HORIZON_MAX = 3


def plan(code: str | None) -> Plan:
    return PLANS.get(code or "trial", PLANS["trial"])


def has_role(role: str, minimum: str) -> bool:
    return ROLE_RANK.get(role, -1) >= ROLE_RANK[minimum]
