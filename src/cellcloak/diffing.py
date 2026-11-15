"""Run-to-run comparison used by the CLI and the local web UI."""

from __future__ import annotations

from typing import Any

from .errors import LedgerError
from .ledger import Ledger


def _basic(detail: dict[str, Any]) -> dict[str, Any]:
    run = detail["run"]
    summary = run.get("summary", {})
    return {
        "id": run["id"],
        "dataset_name": run["dataset_name"],
        "status": run["status"],
        "started_at": run["started_at"],
        "contract_hash": run["contract_hash"],
        "suppression_ratio": summary.get("suppression_ratio", 0.0),
        "primary_count": summary.get("primary_count", 0),
        "secondary_count": summary.get("secondary_count", 0),
        "table_count": summary.get("table_count", 0),
        "blocked_reasons": summary.get("block_reasons", []),
    }


def _public_hashes(detail: dict[str, Any]) -> dict[str, str]:
    return {
        artifact["kind"].split(":", 1)[1]: artifact["sha256"]
        for artifact in detail["artifacts"]
        if artifact["kind"].startswith("public_table:")
    }


def compare_runs(ledger: Ledger, run_a: str, run_b: str) -> dict[str, Any]:
    detail_a = ledger.get_run_detail(run_a)
    detail_b = ledger.get_run_detail(run_b)
    if detail_a is None or detail_b is None:
        missing = run_a if detail_a is None else run_b
        raise LedgerError("E_RUN_NOT_FOUND", "run is not present in the local ledger", table_id=missing)
    for detail in (detail_a, detail_b):
        tombstone = detail.get("tombstone")
        if tombstone is not None:
            code = "E_RUN_PURGED" if tombstone["state"] == "purged" else "E_RUN_PURGING"
            raise LedgerError(
                code,
                "run raw records were purged; only the audit tombstone remains, nothing to compare",
                table_id=detail["run"]["id"],
            )
    basic_a = _basic(detail_a)
    basic_b = _basic(detail_b)
    hashes_a = _public_hashes(detail_a)
    hashes_b = _public_hashes(detail_b)
    public_hash_changed = hashes_a != hashes_b

    table_a = {table["table_id"]: table for table in detail_a["tables"]}
    table_b = {table["table_id"]: table for table in detail_b["tables"]}
    table_rows: list[dict[str, Any]] = []
    for table_id in sorted(set(table_a) | set(table_b)):
        left = table_a.get(table_id)
        right = table_b.get(table_id)
        ratio_a = left["suppression_ratio"] if left else 0.0
        ratio_b = right["suppression_ratio"] if right else 0.0
        primary_a = left["primary_count"] if left else 0
        primary_b = right["primary_count"] if right else 0
        secondary_a = left["secondary_count"] if left else 0
        secondary_b = right["secondary_count"] if right else 0
        hash_a = hashes_a.get(table_id)
        hash_b = hashes_b.get(table_id)
        changed = (
            left is None
            or right is None
            or ratio_a != ratio_b
            or primary_a != primary_b
            or secondary_a != secondary_b
            or (hash_a != hash_b)
            or (left["table_status"] != right["table_status"])
        )
        table_rows.append(
            {
                "table_id": table_id,
                "only_in_a": right is None,
                "only_in_b": left is None,
                "status_a": left["table_status"] if left else "absent",
                "status_b": right["table_status"] if right else "absent",
                "ratio_a": ratio_a,
                "ratio_b": ratio_b,
                "ratio_delta": ratio_b - ratio_a,
                "primary_a": primary_a,
                "primary_b": primary_b,
                "primary_delta": primary_b - primary_a,
                "secondary_a": secondary_a,
                "secondary_b": secondary_b,
                "secondary_delta": secondary_b - secondary_a,
                "public_sha256_a": hash_a,
                "public_sha256_b": hash_b,
                "public_hash_changed": hash_a != hash_b,
                "changed": changed,
            }
        )

    return {
        "run_a": basic_a,
        "run_b": basic_b,
        "summary": {
            "status_changed": basic_a["status"] != basic_b["status"],
            "blocked_changed": bool(basic_a["blocked_reasons"]) != bool(basic_b["blocked_reasons"]),
            "suppression_ratio_delta": basic_b["suppression_ratio"] - basic_a["suppression_ratio"],
            "primary_delta": basic_b["primary_count"] - basic_a["primary_count"],
            "secondary_delta": basic_b["secondary_count"] - basic_a["secondary_count"],
            "public_hash_changed": public_hash_changed,
            "changed": public_hash_changed
            or basic_a["status"] != basic_b["status"]
            or basic_a["suppression_ratio"] != basic_b["suppression_ratio"]
            or basic_a["primary_count"] != basic_b["primary_count"]
            or basic_a["secondary_count"] != basic_b["secondary_count"],
        },
        "tables": table_rows,
    }
