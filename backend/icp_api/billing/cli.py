"""``icp-api billing-reconcile``: dry-run comparison of open orders with the provider's status.

Read-only towards the database (nothing is applied); only GET status calls go to the provider, and
only when billing is configured (sandbox). Prints a JSON report; exit 0 when every order is ``ok``,
1 when differences were found, 2 when billing is not configured."""

from __future__ import annotations

import argparse
import json
from typing import Any

from icp_api.billing import midtrans
from icp_api.billing.config import BillingConfig


def make_client(config: BillingConfig) -> Any:
    return midtrans.SnapClient(config.server_key, config.environment, timeout=config.timeout_seconds)


def reconcile_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="icp-api billing-reconcile")
    parser.add_argument("--limit", type=int, default=200)
    args = parser.parse_args(argv)
    from icp_api.billing.service import BillingService
    from icp_api.billing.store import BillingStore
    from icp_api.container import build_container
    from icp_api.settings import load_settings

    config = BillingConfig.from_env()
    problems = config.problems()
    if problems:
        print(json.dumps({"dry_run": True, "configured": False, "problems": problems}, indent=2))
        return 2
    container = build_container(load_settings())
    report = BillingService(BillingStore(container.db), config, make_client(config)).reconcile(args.limit)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if set(report["summary"]) <= {"ok"} else 1


def register_cli(sub: Any) -> None:
    """Add the subcommand to ``icp-api`` (called from icp_api.cli)."""
    p = sub.add_parser(
        "billing-reconcile", help="dry run: compare open billing orders with the provider status"
    )
    p.add_argument("--limit", type=int, default=200)
    p.set_defaults(func=lambda a: reconcile_main(["--limit", str(a.limit)]))
