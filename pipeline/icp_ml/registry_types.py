"""Registry data types shared by the MLflow registry and the exported-bundle registry.

This module imports no MLflow so that a serving process in bundle mode (``MODEL_SOURCE=bundle``)
never pays MLflow's import cost or needs a tracking store.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

from icp_ml.methods import MethodSpec


@dataclass
class ModelVersionInfo:
    name: str
    version: str
    run_id: str | None
    created_at_ms: int
    aliases: list[str]
    tags: dict[str, str]
    metrics: dict[str, float]
    params: dict[str, str]
    status: str

    @property
    def method_spec(self) -> MethodSpec | None:
        """The forecasting method this version implements (None for pre-redesign models)."""
        raw = self.tags.get("icp.method_spec")
        if not raw:
            return None
        d = json.loads(raw)
        return MethodSpec(
            d["name"],
            d["family"],
            dict(d.get("params") or {}),
            list(d.get("features") or []),
            d.get("description", ""),
        )


@dataclass
class ValidationCheck:
    name: str
    passed: bool
    detail: str


@dataclass
class ValidationReport:
    version: str
    passed: bool
    checks: list[ValidationCheck] = field(default_factory=list)
    candidate_score: float | None = None
    production_version: str | None = None
    production_score: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class RegistryError(RuntimeError):
    pass
