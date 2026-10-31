"""Provenance diagnostics and dissent evidence (operator CLI; Agent D, mission v6).

``icp-api verify-provenance [--state-dir DIR]`` (read-only)
    Re-derives every ICP month's status, basis, document count and label from the stored evidence and
    compares it with what every surface shows (/data/verification, the provenance report, the insight
    breakdown). It also checks that the value in the active dataset is backed by at least one stored
    reading, and that it matches the stored ESDM reconciliation state when that state is present.
    Prints one JSON line per month that has issues, then a summary line. Exit code 1 if any month
    disagrees.

``icp-api backfill-dissent [--dry-run] [--state-dir DIR]`` (additive, idempotent)
    Records the source readings that the reconciliation outvoted or that differ from the active value as
    ``verification_status = "dissent"`` evidence. It never changes dataset values or existing rows.
    Readings for months outside the active dataset are not dissent; they are only counted.

``icp-api repair-decree-references [--apply] [--ocr] [--state-dir DIR] [--backup-dir DIR] [--backup-taken]``
    (dry run by default)
    Re-reads every stored decree copy (checksum-verified) with the current parser and corrects the decree
    number recorded with its evidence: in ``source_evidence.reference`` and in the reconciliation state.
    Before 2026-10-25 the parser took the first "Nomor ...K/..." in the text, which for decrees whose own
    number is not in the text layer was a regulation cited in the considerations (e.g. the formula decree
    276.K/MG.03/MEM.M/2026 for both August and September 2026). Values never change; idempotent.
    With ``--apply`` and at least one correction, the command first takes a backup itself and verifies it
    (local store: ``icp-api backup`` into ``--backup-dir`` plus copies of the two state files, then
    ``restore-check`` against the live database); on a deployed store it refuses unless the operator
    confirms a platform backup with ``--backup-taken``. Nothing is written when the backup fails. The
    audit row is written whatever happens, with the counts actually applied (outcome ``failure`` after a
    partial run).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from icp_api.services.verification import (
    CONFIRMED,
    LEGEND,
    NOT_LIVE,
    OFFICIAL_KINDS,
    TOLERANCE,
    classify_icp_month,
)

if TYPE_CHECKING:
    from icp_api.container import Container
    from icp_api.settings import Settings
    from icp_ml.sources.esdm_icp import ReconciledMonth

DISSENT = "dissent"


def _load_reconciliation(state_dir: Path | None) -> list[ReconciledMonth] | None:
    from icp_ml.sources.esdm_icp import ReconciledMonth

    path = (state_dir or Path()) / "reconciliation.json"
    if state_dir is None or not path.is_file():
        return None
    return [ReconciledMonth(**m) for m in json.loads(path.read_text(encoding="utf-8"))]


def dissent_rows(
    months: list[ReconciledMonth], series: pd.Series, version_id: str | None
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Readings of months in the active series that differ from the active value (> TOLERANCE)."""
    rows: list[dict[str, Any]] = []
    outside = 0
    for m in months:
        p = pd.Period(m.period, freq="M")
        readings = [o for o in m.sources if o.get("value") is not None]
        if p not in series.index:
            outside += sum(1 for _ in readings)
            continue
        active = float(series[p])
        for o in readings:
            if abs(float(o["value"]) - active) <= TOLERANCE:
                continue
            rows.append(
                {
                    "period": m.period,
                    "value": float(o["value"]),
                    "kind": o["kind"],
                    "verification_status": DISSENT,
                    "source_url": o.get("url") or "esdm reconciliation state",
                    "document_sha256": o.get("sha256"),
                    "reference": o.get("reference"),
                    "retrieved_at": o.get("retrieved_at") or "",
                    "note": m.note or f"{o['kind']} differs ({o['value']}) from the value in use ({active})",
                    "dataset_version_id": version_id,
                }
            )
    return rows, {"dissent": len(rows), "outside_active_range": outside}


def backfill_dissent(c: Container, state_dir: Path | None, dry_run: bool) -> dict[str, Any]:
    months = _load_reconciliation(state_dir)
    if months is None:
        return {"error": f"no reconciliation state at {state_dir}"}
    active = c.datasets.optional("icp")
    if active is None:
        return {"error": "no active ICP dataset"}
    rows, stats = dissent_rows(months, active.series, active.version["id"])
    have = {c.repo.evidence_key(r) for r in c.repo.all_evidence("icp")}
    new = [r for r in rows if c.repo.evidence_key({**r, "dataset": "icp"}) not in have]
    out: dict[str, Any] = {"candidates": len(rows), **stats}
    if dry_run:
        return {**out, "would_insert": len(new), "periods": sorted({r["period"] for r in new})}
    inserted = c.repo.insert_missing_evidence("icp", rows)
    if inserted:
        c.repo.audit("system:cli", "dataset.icp.dissent_backfill", "success", target=active.version["id"])
    return {**out, "inserted": inserted}


class BackupRequired(RuntimeError):
    """``repair-decree-references --apply`` could not take or verify a backup; nothing was written."""


STATE_FILES = ("decrees.json", "reconciliation.json")


def take_verified_backup(
    settings: Settings, state_dir: Path, out: str | Path | None, backup_taken: bool
) -> str:
    """Back up everything the decree repair writes, and verify the copy, before it writes anything.

    Local store: ``icp-api backup`` into ``out`` (default var/backups) plus copies of the two ESDM state
    files under ``esdm-state/``, then ``restore-check`` against the live database (integrity, schema,
    objects; the evidence row count must equal the live one) and a checksum comparison of the state
    copies. Deployed store (D1, Postgres, R2): the operator takes the platform backup (docs/OPERATIONS.md)
    and confirms it with ``backup_taken``. Returns what the audit row records; raises ``BackupRequired``."""
    from icp_api.cli import is_local_store, write_local_backup
    from icp_api.restore_check import restore_check
    from icp_ml.sources.base import sha256_bytes

    if not is_local_store(settings):
        if backup_taken:
            return "confirmed by operator (--backup-taken)"
        raise BackupRequired(
            "this store is backed up with platform tooling (docs/OPERATIONS.md, Backups): take that backup, "
            "then re-run with --apply --backup-taken"
        )
    dest = write_local_backup(settings, out)
    copies = dest / "esdm-state"
    copies.mkdir(exist_ok=True)
    for name in STATE_FILES:
        src = state_dir / name
        if src.is_file():
            data = src.read_bytes()
            (copies / name).write_bytes(data)
            if sha256_bytes((copies / name).read_bytes()) != sha256_bytes(data):
                raise BackupRequired(f"state file copy {copies / name} does not match {src}")
    report = restore_check(dest, live_db=Path(settings.sqlite_path))
    evidence_diff = (report.get("comparison") or {}).get("row_count_diff", {}).get("source_evidence")
    if not report["ok"] or evidence_diff:
        problems = list(report["problems"]) + ([f"source_evidence {evidence_diff}"] if evidence_diff else [])
        raise BackupRequired(f"backup {dest} did not verify: {'; '.join(problems)}")
    return str(dest)


def repair_decree_references(
    c: Container,
    state_dir: Path,
    apply: bool,
    ocr: bool = False,
    before_apply: Callable[[], str | None] | None = None,
) -> dict[str, Any]:
    """Correct stored decree numbers from the decree documents themselves (see module docstring).

    ``before_apply`` runs only when there is something to write, before anything is written (the CLI
    takes and verifies the backup there); when it raises, nothing is written. Its result is recorded in
    the audit row as ``backup``."""
    from icp_ml.sources import esdm_backfill, esdm_icp

    if not (state_dir / "decrees.json").is_file():
        return {"error": f"no decree state at {state_dir}"}
    changes = esdm_backfill.recheck_decree_numbers(state_dir, ocr=ocr)
    rows = c.repo.all_evidence("icp")
    planned = []
    for ch in changes:
        stored = [
            r
            for r in rows
            if r["period"] == ch["period"]
            and r.get("document_sha256") == ch["sha256"]
            and r["kind"] in esdm_icp.DECREE_KINDS
        ]
        planned.append(
            {
                **{k: ch[k] for k in ("period", "method", "old", "new")},
                "evidence_rows": sum(1 for r in stored if r.get("reference") != ch["new"]),
            }
        )
    out: dict[str, Any] = {
        "documents": len(changes),
        "evidence_rows": sum(p["evidence_rows"] for p in planned),
        "changes": planned,
        "ocr_rechecked": ocr,
    }
    if not apply or not changes:
        return {**out, "applied": False}
    backup = before_apply() if before_apply is not None else None
    active = c.datasets.optional("icp")
    updated = 0
    state: dict[str, int] | None = None
    try:
        for ch in changes:
            updated += c.repo.correct_decree_reference("icp", ch["period"], ch["sha256"], ch["new"])
        state = esdm_backfill.apply_decree_numbers(state_dir, changes)
    finally:
        # Written whatever happened, with what was actually applied: after a failure part-way (for
        # example the state files could not be written) the trail still shows the corrected rows.
        c.repo.audit(
            "system:cli",
            "dataset.icp.decree_reference_repair",
            "success" if state is not None else "failure",
            target=active.version["id"] if active is not None else None,
            details={
                "documents": len(changes),
                "evidence_rows": updated,
                **(state or {}),
                "state_files_written": state is not None,
                "backup": backup,
            },
        )
    return {
        **out,
        "applied": True,
        "evidence_rows_updated": updated,
        "state_updated": state,
        "backup": backup,
    }


def month_checks(
    period: str,
    value: float | None,
    rows: list[dict[str, Any]],
    shown: dict[str, Any],
    recon: ReconciledMonth | None,
) -> list[str]:
    """Disagreements between what a surface shows for one month and what stored evidence supports."""
    issues: list[str] = []
    derived = classify_icp_month(value, rows)
    for key in ("basis", "status", "documents"):
        if key in shown and shown[key] != derived[key]:
            issues.append(f"{key}: shown {shown[key]!r}, evidence supports {derived[key]!r}")
    if "label" in shown and shown["label"] != LEGEND[derived["basis"]]["label"]:
        issues.append("label does not match the derived basis")
    live = [r for r in rows if r.get("verification_status") not in NOT_LIVE]
    if derived["basis"] not in ("no_evidence", "conflict") and value is not None:
        if not any(r.get("value") is not None and abs(float(r["value"]) - value) <= TOLERANCE for r in live):
            issues.append("value in use is not backed by any stored reading")
    if derived["basis"] in CONFIRMED and derived["basis"] == "two_documents":
        docs = {
            str(r.get("document_sha256") or r.get("source_url"))
            for r in live
            if r["kind"] in OFFICIAL_KINDS
            and r.get("value") is not None
            and value is not None
            and abs(float(r["value"]) - value) <= TOLERANCE
        }
        if len(docs) < 2:
            issues.append("two_documents without two distinct agreeing documents")
    if (
        recon is not None
        and recon.value is not None
        and value is not None
        and abs(recon.value - value) > TOLERANCE
    ):
        issues.append(f"active value {value} differs from the reconciled value {recon.value}")
    return issues


def _report_counts(c: Container) -> dict[str, int]:
    content: dict[str, Any] = {"tables": {}, "sections": []}
    c.reports._provenance(content, None)
    by_label = {v["label"]["id"]: k for k, v in LEGEND.items()}
    rows = content["tables"].get("Verifikasi ICP per bulan") or []
    counts = Counter(by_label.get(str(r["Dasar nilai"]), "?") for r in rows)
    return {k: n for k, n in counts.items() if k not in CONFIRMED}


def _insight_counts(c: Container) -> dict[str, int]:
    from icp_api.insights import rules

    ctx = c.insights.context()
    f = rules.data_verification(ctx) if ctx is not None else None
    if f is None:
        return {}
    by_label = {v["label"]["id"]: k for k, v in LEGEND.items()}
    return {
        by_label.get(v.label_slots["label"].id, "?"): int(v.value or 0)
        for v in f.outputs
        if v.key == "basis_count"
    }


def verify_provenance(c: Container, state_dir: Path | None = None) -> dict[str, Any]:
    """Month-by-month agreement between stored evidence and every surface (read-only)."""
    months = c.verification.months("icp", include_urls=True)
    by_period: dict[str, list[dict[str, Any]]] = {}
    for r in c.repo.all_evidence("icp"):
        by_period.setdefault(str(r["period"]), []).append(r)
    recon = {m.period: m for m in (_load_reconciliation(state_dir) or [])}
    disagreements: list[dict[str, Any]] = []
    gaps: list[dict[str, Any]] = []
    for m in months:
        rows = by_period.get(m["period"], [])
        issues = month_checks(m["period"], m["value"], rows, m, recon.get(m["period"]))
        if issues:
            disagreements.append({"period": m["period"], "basis": m["basis"], "issues": issues})
        rm = recon.get(m["period"])
        if rm is not None and m["value"] is not None:
            stored = {
                (str(r["kind"]), round(float(r["value"]), 2)) for r in rows if r.get("value") is not None
            }
            missing = [
                f"{o['kind']} {o['value']}"
                for o in rm.sources
                if o.get("value") is not None and (str(o["kind"]), round(float(o["value"]), 2)) not in stored
            ]
            if missing:
                gaps.append({"period": m["period"], "readings_not_recorded": missing})
    api_counts = {
        k: n
        for k, n in c.verification.summary("icp", None, None)["counts"].items()
        if n and k not in CONFIRMED
    }
    surfaces = {
        "verification_api": api_counts,
        "provenance_report": _report_counts(c),
        "insight_counts": _insight_counts(c),
    }
    if not (surfaces["verification_api"] == surfaces["provenance_report"] == surfaces["insight_counts"]):
        disagreements.append(
            {"period": None, "basis": None, "issues": [f"surface counts differ: {surfaces}"]}
        )
    return {
        "months": len(months),
        "bases": dict(Counter(m["basis"] for m in months)),
        "dissent_months": sum(1 for m in months if m["dissent"]),
        "disagreements": disagreements,
        "gaps": gaps,
        "surfaces": surfaces,
        "reconciliation_state": bool(recon),
    }


# -- CLI ---------------------------------------------------------------------------------------------
def _container() -> Container:
    from icp_api.container import build_container
    from icp_api.settings import load_settings

    return build_container(load_settings())


def _default_state_dir(c: Container) -> Path:
    return Path(c.settings.source_state_dir) / "esdm"


def _verify(args: argparse.Namespace) -> int:
    c = _container()
    state = Path(args.state_dir) if args.state_dir else _default_state_dir(c)
    report = verify_provenance(c, state)
    for d in report["disagreements"]:
        print(json.dumps({"disagreement": d}, ensure_ascii=False))
    for g in report["gaps"]:
        print(json.dumps({"gap": g}, ensure_ascii=False))
    print(
        json.dumps(
            {
                "months": report["months"],
                "bases": report["bases"],
                "dissent_months": report["dissent_months"],
                "disagreements": len(report["disagreements"]),
                "gaps": len(report["gaps"]),
                "surfaces": report["surfaces"],
                "reconciliation_state": report["reconciliation_state"],
            },
            ensure_ascii=False,
        )
    )
    return 1 if report["disagreements"] else 0


def _backfill(args: argparse.Namespace) -> int:
    c = _container()
    state = Path(args.state_dir) if args.state_dir else _default_state_dir(c)
    out = backfill_dissent(c, state, args.dry_run)
    print(json.dumps(out, ensure_ascii=False))
    return 1 if "error" in out else 0


def _repair(args: argparse.Namespace) -> int:
    c = _container()
    state = Path(args.state_dir) if args.state_dir else _default_state_dir(c)
    try:
        out = repair_decree_references(
            c,
            state,
            apply=args.apply,
            ocr=args.ocr,
            before_apply=lambda: take_verified_backup(c.settings, state, args.backup_dir, args.backup_taken),
        )
    except BackupRequired as exc:
        print(json.dumps({"error": f"nothing written: {exc}", "applied": False}, ensure_ascii=False))
        return 2
    print(json.dumps(out, ensure_ascii=False))
    return 1 if "error" in out else 0


def register_cli(sub: Any) -> None:
    """Hook for ``icp_api.cli`` (three subcommands)."""
    p = sub.add_parser(
        "repair-decree-references",
        help="re-read stored decrees and correct the decree numbers recorded with ICP evidence (dry run "
        "unless --apply; values never change)",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="write the corrections after taking and verifying a backup (default: report only)",
    )
    p.add_argument("--ocr", action="store_true", help="also re-read scanned decrees with OCR (slow)")
    p.add_argument("--state-dir", default=None, help="ESDM state (default: SOURCE_STATE_DIR/esdm)")
    p.add_argument(
        "--backup-dir",
        default=None,
        help="local store: where --apply writes its backup (default var/backups)",
    )
    p.add_argument(
        "--backup-taken",
        action="store_true",
        help="deployed store only: confirm that a platform backup was taken (required with --apply there)",
    )
    p.set_defaults(func=_repair)
    p = sub.add_parser(
        "verify-provenance",
        help="read-only: check every ICP month's label against stored evidence and all surfaces",
    )
    p.add_argument(
        "--state-dir", default=None, help="ESDM reconciliation state (default: SOURCE_STATE_DIR/esdm)"
    )
    p.set_defaults(func=_verify)
    p = sub.add_parser(
        "backfill-dissent",
        help="record outvoted/differing ICP source readings as dissent evidence (additive, idempotent)",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--state-dir", default=None)
    p.set_defaults(func=_backfill)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="icp-provenance", description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    register_cli(sub)
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
