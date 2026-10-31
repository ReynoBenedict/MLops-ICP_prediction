"""Structured findings produced by rules (numbers only; text is composed later in narrative.py)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

Importance = Literal["high", "medium", "low"]
Audience = Literal["customer", "operator"]
# How a value is displayed; the narrative formats it per language.
Fmt = Literal[
    "usd", "usd_signed", "pct", "pct_signed", "idr", "rate", "int", "share", "period", "date", "text"
]


@dataclass(frozen=True)
class Value:
    key: str
    value: float | int | str | None
    fmt: Fmt
    unit: str = ""
    period: str | None = None
    label_slots: dict[str, Any] = field(default_factory=dict)  # e.g. {"b": "Brent"} for "Selisih ICP-{b}"


@dataclass(frozen=True)
class DatasetRef:
    dataset: str
    version_id: str | None
    period_end: str | None


@dataclass
class Finding:
    rule_id: str
    rule_version: int
    kind: str
    importance: Importance
    period: str | None
    template: str  # key into templates_*.TEMPLATES (title/body/next_step derived from it)
    slots: dict[str, Any]  # template slots: Value objects or plain strings/ints (already language-neutral)
    facts: list[Value]
    inputs: list[Value]
    outputs: list[Value]
    datasets: list[DatasetRef]
    method: str  # key into METHODS
    window: dict[str, Any] = field(default_factory=dict)  # subject, window_start, window_end, n
    model: dict[str, str] | None = None
    forecast_ids: list[str] = field(default_factory=list)
    audience: Audience = "customer"
    body_parts: list[tuple[str, dict[str, Any]]] = field(default_factory=list)  # extra sentences
    direction: int | None = None  # sign of the described move, for consistency checks
    key_suffix: str | None = None  # distinguishes several findings of one rule (e.g. per dataset)

    @property
    def id(self) -> str:
        base = f"{self.rule_id}_{self.period or 'all'}"
        return f"{base}_{self.key_suffix}" if self.key_suffix else base


@dataclass(frozen=True)
class Word:
    """A template word (``word.<key>``) resolved per language."""

    key: str


@dataclass(frozen=True)
class Loc:
    """Text already available in both languages (e.g. verification labels)."""

    id: str
    en: str


@dataclass(frozen=True)
class Parts:
    """Several template fragments rendered and joined (e.g. a list of recorded differences)."""

    items: tuple[tuple[str, dict[str, Any]], ...]
    sep: str = "; "
