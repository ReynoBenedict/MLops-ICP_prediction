"""Provider-neutral payment-intent state machine (finite; illegal transitions are rejected).

    created --> pending --> paid --> refunded
       |           |
       |           +--> failed | expired | cancelled
       +--> cancelled | expired

``failed``, ``expired``, ``cancelled`` and ``refunded`` are terminal. A repeated status (pending ->
pending, paid -> paid) is a no-op, never an error, so replayed or reordered provider notifications
cannot move an order backwards (e.g. a late ``pending`` after ``paid`` is ignored and logged).
"""

from __future__ import annotations

from typing import Literal

OrderStatus = Literal["created", "pending", "paid", "failed", "expired", "cancelled", "refunded"]
STATUSES: tuple[OrderStatus, ...] = (
    "created",
    "pending",
    "paid",
    "failed",
    "expired",
    "cancelled",
    "refunded",
)
TERMINAL: frozenset[str] = frozenset({"failed", "expired", "cancelled", "refunded"})

TRANSITIONS: dict[str, frozenset[str]] = {
    "created": frozenset({"pending", "cancelled", "expired"}),
    "pending": frozenset({"paid", "failed", "expired", "cancelled"}),
    "paid": frozenset({"refunded"}),
    "failed": frozenset(),
    "expired": frozenset(),
    "cancelled": frozenset(),
    "refunded": frozenset(),
}


class IllegalTransition(ValueError):
    def __init__(self, current: str, target: str):
        super().__init__(f"illegal billing transition {current} -> {target}")
        self.current = current
        self.target = target


def can_transition(current: str, target: str) -> bool:
    return target in TRANSITIONS.get(current, frozenset())


def next_status(current: str, target: str) -> tuple[str, bool]:
    """Return (status, changed). Same status -> unchanged; an illegal move raises ``IllegalTransition``."""
    if current not in TRANSITIONS or target not in TRANSITIONS:
        raise IllegalTransition(current, target)
    if current == target:
        return current, False
    if not can_transition(current, target):
        raise IllegalTransition(current, target)
    return target, True
