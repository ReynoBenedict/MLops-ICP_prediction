"""Vercel entry point of the ICP API (Python runtime, FastAPI preset).

`scripts/vercel-backend.mjs` copies this file to the root of the deploy folder
(var/deploy/vercel-backend/), next to `backend/`, `pipeline/` and `infra/cloudflare/d1/migrations/`
in their repository layout. Vercel imports this module and serves its top-level `app` (ASGI).
See docs/DEPLOYMENT.md and backend/icp_api/serverless.py.
"""

import sys
from pathlib import Path

# The bundle is read-only: never try to write __pycache__ next to the sources (Vercel ships
# precompiled bytecode with the function).
sys.dont_write_bytecode = True

_ROOT = Path(__file__).resolve().parent
for _package_dir in ("pipeline", "backend"):  # backend ends up first on sys.path
    _entry = str(_ROOT / _package_dir)
    if _entry not in sys.path:
        sys.path.insert(0, _entry)

from icp_api.serverless import app  # noqa: E402  (needs the path setup above)

__all__ = ["app"]
