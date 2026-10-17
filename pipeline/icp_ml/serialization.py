"""skops model serialization with an explicit trusted-type allow-list.

Models are serialized with skops (no arbitrary code execution on load, unlike pickle). skops
refuses types it cannot verify; the trusted list is computed per model from what it actually
contains and must stay within ``SKOPS_ALLOWED_TYPES`` (reviewed scikit-learn internals + our own
rule-based estimators). The same allow-list guards training (MLflow) and the exported bundles
served without MLflow, so a bundle can never load a type training would have refused.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

SKOPS_ALLOWED_TYPES = frozenset(
    {
        "sklearn.tree._tree.Tree",
        "icp_ml.methods.NaiveLast",
        "icp_ml.methods.SpreadRule",
        "icp_ml.methods.BenchmarkBlend",
    }
)


class UntrustedModelType(RuntimeError):
    """The serialized model needs a type outside the allow-list."""


def check_trusted_types(types: Any) -> list[str]:
    unexpected = set(types) - SKOPS_ALLOWED_TYPES
    if unexpected:
        raise UntrustedModelType(f"Model contains types outside the skops allow-list: {sorted(unexpected)}")
    return sorted(set(types) | {"sklearn.tree._tree.Tree"})


def dump_skops(model: Any) -> tuple[bytes, list[str]]:
    """Serialize ``model`` and return (bytes, trusted types it needs), enforcing the allow-list."""
    import skops.io as sio

    data = sio.dumps(model)
    needed = sio.get_untrusted_types(data=data)
    return data, check_trusted_types(needed)


def skops_trusted_types(model: Any) -> list[str]:
    import skops.io as sio

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "model.skops")
        sio.dump(model, path)
        needed = sio.get_untrusted_types(file=path)
    return check_trusted_types(needed)


def load_skops(data: bytes, trusted: list[str] | None = None) -> Any:
    """Load a skops payload, trusting only allow-listed types (and only those actually present)."""
    import skops.io as sio

    needed = list(sio.get_untrusted_types(data=data))
    allowed = check_trusted_types(needed)
    if trusted is not None:
        # The bundle's declared list must itself be inside the allow-list (defence in depth).
        check_trusted_types(trusted)
    return sio.loads(data, trusted=allowed)
