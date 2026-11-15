"""End-to-end run orchestration and ledger-backed re-verification."""

from __future__ import annotations

import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .contract import CellRef, Contract, SharedGroup, load_contract
from .ingest import load_table
from .joint import STRATEGY, joint_suppress
from .ledger import Ledger
from .model import (
    RISK_LABELS,
    SUPPRESSION_NONE,
    SuppressionReason,
    TableModel,
    cell_fingerprint,
)
from .report import Artifact, EvaluatedTable, build_report, emit_outputs
from .risk import RiskAssessment, classify
from .suppress import SuppressionResult
from .verify import (
    JointVerifyResult,
    VerifyResult,
    verify_joint,
    verify_table,
)


@dataclass
class RunOutcome:
    run_id: str
    status: str
    output_dir: Path
    report: dict[str, Any]
    artifacts: list[Artifact]


@dataclass
class ReverifyOutcome:
    run_id: str
    passed: bool
    status: str
    tables: list[dict[str, Any]]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def new_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"run-{stamp}-{secrets.token_hex(3)}"


def _table_status(evaluation: EvaluatedTable) -> str:
    if evaluation.suppression.blocked:
        return "blocked"
    if not evaluation.verification.passed:
        return "verify_failed"
    return "ok"


def _joint_checks_payload(
    joint: JointVerifyResult | None, *, table_id: str | None = None, include_per_table: bool = False
) -> list[dict[str, Any]]:
    if joint is None:
        return []
    checks: list[dict[str, Any]] = []
    if include_per_table:
        for owner, result in joint.per_table.items():
            if table_id is not None and owner != table_id:
                continue
            for check in result.checks:
                checks.append(
                    {
                        "table_id": owner,
                        "name": check.name,
                        "result": "pass" if check.passed else "fail",
                        "detail": check.detail,
                    }
                )
    for check in joint.checks:
        checks.append(
            {
                "table_id": table_id,
                "name": check.name,
                "result": "pass" if check.passed else "fail",
                "detail": check.detail,
            }
        )
    return checks


def _group_review_payload(contract: Contract, joint) -> tuple[dict[str, Any], ...]:
    return tuple(group.as_record() for group in joint.groups)


def _merged_verification(
    table_id: str,
    per_table: VerifyResult,
    joint: JointVerifyResult,
    grouped_tables: set[str],
) -> VerifyResult:
    """Fold whole-release checks into each relation member's verdict.

    A table that belongs to a shared relation cannot report a table-local
    ``ok`` while the joint release check failed; the report, the ledger and
    the CLI must all reach the same conclusion for the same package.
    """

    if table_id not in grouped_tables:
        return per_table
    checks = tuple(per_table.checks) + tuple(joint.checks)
    return VerifyResult(
        passed=per_table.passed and joint.passed,
        checks=checks,
        evidence=per_table.evidence,
    )


def _table_group_names(contract: Contract) -> dict[str, tuple[str, ...]]:
    names: dict[str, list[str]] = {spec.table_id: [] for spec in contract.tables}
    for group in contract.shared_cells:
        for table_id in group.table_ids:
            names.setdefault(table_id, []).append(group.name)
    return {table_id: tuple(values) for table_id, values in names.items()}


def _table_meta(evaluation: EvaluatedTable, contract: Contract) -> dict[str, Any]:
    model = evaluation.model
    shared = contract.shared_groups_for(model.table_id)
    return {
        "row_dim": model.row_dim,
        "col_dim": model.col_dim,
        "value_dim": model.value_dim,
        "row_keys": list(model.row_keys),
        "col_keys": list(model.col_keys),
        "max_suppression_ratio": contract.privacy.max_suppression_ratio,
        "source_name": model.source_name,
        "source_sha256": model.source_sha256,
        "risk_level": evaluation.risk.risk_level(evaluation.suppression.blocked),
        "risk_class_counts": {label: int(evaluation.risk.counts.get(label, 0)) for label in RISK_LABELS},
        "public_zero_cells": [
            {"row": row, "col": col}
            for row, col in sorted(evaluation.risk.public_zero_coords)
        ],
        "info_loss": {
            "suppressed_count_sum_ratio": evaluation.suppression.suppressed_count_sum_ratio,
            "max_row_suppression_ratio": evaluation.suppression.max_row_suppression_ratio,
            "max_col_suppression_ratio": evaluation.suppression.max_col_suppression_ratio,
        },
        "shared_relations": [
            {
                "name": group.name,
                "cells": [
                    {"table": cell.table_id, "row": cell.row, "col": cell.col}
                    for cell in group.cells
                ],
            }
            for group in shared
        ],
        "strategy": STRATEGY,
    }


def _ledger_payload(
    contract: Contract,
    outcome_run_id: str,
    status: str,
    started_at: str,
    finished_at: str,
    out_dir: Path,
    report: dict[str, Any],
    evaluations: list[EvaluatedTable],
    artifacts: list[Artifact],
    joint=None,
    joint_verification: JointVerifyResult | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    table_records: list[dict[str, Any]] = []
    cell_records: list[dict[str, Any]] = []
    check_records: list[dict[str, Any]] = []
    group_names = _table_group_names(contract)
    for evaluation in evaluations:
        model = evaluation.model
        table_status = _table_status(evaluation)
        table_records.append(
            {
                "table_id": model.table_id,
                "row_count": model.row_count(),
                "col_count": model.col_count(),
                "cell_count": model.cell_count(),
                "primary_count": evaluation.suppression.primary_count,
                "secondary_count": evaluation.suppression.secondary_count,
                "suppression_ratio": evaluation.suppression.suppression_ratio,
                "table_status": table_status,
                "block_reasons": [
                    {"code": reason.code, "detail": reason.detail}
                    for reason in evaluation.suppression.block_reasons
                ],
                "verify_passed": evaluation.verification.passed,
                "meta": _table_meta(evaluation, contract),
            }
        )
        risk_by_coordinate = {finding.coordinate: finding.category for finding in evaluation.risk.findings}
        for cell in model.cells():
            reason = evaluation.suppression.suppressed.get(cell.coordinate)
            cell_records.append(
                {
                    "table_id": model.table_id,
                    "row_key": cell.row,
                    "col_key": cell.col,
                    "raw_count": cell.count,
                    "suppression_type": reason.kind if reason else SUPPRESSION_NONE,
                    "risk_category": risk_by_coordinate[(cell.row, cell.col)],
                    "fingerprint": cell_fingerprint(model.table_id, cell.row, cell.col),
                }
            )
        for check in evaluation.verification.checks:
            check_records.append(
                {
                    "table_id": model.table_id,
                    "name": check.name,
                    "result": "pass" if check.passed else "fail",
                    "evidence": {"detail": check.detail},
                    "source": "run",
                }
            )
        for group_name in group_names.get(model.table_id, ()):
            check_records.append(
                {
                    "table_id": model.table_id,
                    "name": "shared_relation_member",
                    "result": "pass" if table_status != "blocked" else "fail",
                    "evidence": {"detail": f"member of shared relation {group_name!r}"},
                    "source": "run",
                }
            )
    if joint is not None or joint_verification is not None:
        for check in _joint_checks_payload(joint_verification):
            check_records.append(
                {
                    "table_id": None,
                    "name": check["name"],
                    "result": check["result"],
                    "evidence": {"detail": check["detail"], "scope": "joint"},
                    "source": "run",
                }
            )
        if joint is not None:
            for group in joint.groups:
                for reason in group.block_reasons:
                    check_records.append(
                        {
                            "table_id": reason["table_id"],
                            "name": "shared_relation_block",
                            "result": "fail",
                            "evidence": {
                                "detail": reason["detail"],
                                "code": reason["code"],
                                "group": group.name,
                                "scope": "joint",
                            },
                            "source": "run",
                        }
                    )
    run_record = {
        "id": outcome_run_id,
        "dataset_name": contract.dataset_name,
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "contract_hash": contract.contract_hash,
        "contract_path": str(contract.path),
        "output_dir": str(out_dir),
        "summary": report["summary"],
    }
    artifact_records = [artifact.as_record() for artifact in artifacts]
    return run_record, table_records, cell_records, check_records, artifact_records


def execute_run(
    contract_path: str | Path,
    output_dir: str | Path,
    ledger: Ledger,
    *,
    run_id: str | None = None,
) -> RunOutcome:
    started_at = _now()
    contract = load_contract(contract_path)
    run_id = run_id or new_run_id()

    models: dict[str, TableModel] = {}
    assessments: dict[str, RiskAssessment] = {}
    for spec in contract.tables:
        model = load_table(spec)
        models[spec.table_id] = model
        assessments[spec.table_id] = classify(model, spec, contract.privacy)

    # The cross-table solver first reproduces each table's local result, then
    # closes declared shared relations.  A contract without shared relations
    # therefore takes exactly the single-table path.
    joint = joint_suppress(contract, models, assessments)
    ratios = {
        spec.table_id: contract.privacy.max_suppression_ratio for spec in contract.tables
    }
    joint_verification = verify_joint(
        contract.shared_cells,
        models,
        {table_id: result.suppressed for table_id, result in joint.suppressions.items()},
        primary_coords={
            table_id: assessment.primary_coords
            for table_id, assessment in assessments.items()
        },
        max_suppression_ratios=ratios,
        public_zero_coords={
            table_id: assessment.public_zero_coords
            for table_id, assessment in assessments.items()
        },
    )

    evaluations: list[EvaluatedTable] = []
    grouped_tables = {
        cell.table_id for group in contract.shared_cells for cell in group.cells
    }
    for spec in contract.tables:
        table_id = spec.table_id
        suppression: SuppressionResult = joint.suppressions[table_id]
        verification = _merged_verification(
            table_id,
            joint_verification.per_table[table_id],
            joint_verification,
            grouped_tables,
        )
        evaluations.append(
            EvaluatedTable(
                models[table_id], assessments[table_id], suppression, verification
            )
        )

    blocked = any(item.suppression.blocked for item in evaluations)
    verify_failed = not joint_verification.passed
    status = "blocked" if blocked else "verify_failed" if verify_failed else "ok"
    finished_at = _now()
    group_reviews = _group_review_payload(contract, joint)
    joint_events = tuple(event.as_record() for event in joint.events)
    joint_checks = tuple(
        {
            "name": check.name,
            "result": "pass" if check.passed else "fail",
            "detail": check.detail,
        }
        for check in joint_verification.checks
    )
    report = build_report(
        contract,
        run_id,
        status,
        started_at,
        finished_at,
        evaluations,
        group_reviews=group_reviews,
        joint_events=joint_events,
        joint_checks=joint_checks,
        strategy=STRATEGY,
    )
    out_dir = Path(output_dir).expanduser().resolve()
    artifacts = emit_outputs(out_dir, report, evaluations, publish=status == "ok")
    payload = _ledger_payload(
        contract,
        run_id,
        status,
        started_at,
        finished_at,
        out_dir,
        report,
        evaluations,
        artifacts,
        joint,
        joint_verification,
    )
    ledger.record_run(*payload)
    return RunOutcome(
        run_id=run_id,
        status=status,
        output_dir=out_dir,
        report=report,
        artifacts=artifacts,
    )


def _model_from_ledger(table: dict[str, Any]) -> TableModel:
    meta = table["meta"]
    counts = {
        (cell["row_key"], cell["col_key"]): int(cell["raw_count"])
        for cell in table["cells"]
    }
    row_keys = tuple(meta.get("row_keys", []))
    col_keys = tuple(meta.get("col_keys", []))
    missing = [(row, col) for row in row_keys for col in col_keys if (row, col) not in counts]
    if not row_keys or not col_keys or missing:
        raise ValueError(f"ledger table {table['table_id']} has an incomplete cell matrix")
    return TableModel(
        table_id=table["table_id"],
        row_dim=meta.get("row_dim", ""),
        col_dim=meta.get("col_dim", ""),
        value_dim=meta.get("value_dim", ""),
        row_keys=row_keys,
        col_keys=col_keys,
        counts=counts,
        source_name=meta.get("source_name", ""),
        source_sha256=meta.get("source_sha256", ""),
    )


def _groups_from_ledger(tables: list[dict[str, Any]]) -> tuple[SharedGroup, ...]:
    """Reconstruct declared shared relations from the table metadata."""

    groups: list[SharedGroup] = []
    seen: set[str] = set()
    for table in tables:
        meta = table.get("meta", {})
        for raw_group in meta.get("shared_relations", []) or []:
            name = raw_group.get("name")
            if not isinstance(name, str) or name in seen:
                continue
            seen.add(name)
            cells = tuple(
                CellRef(cell["table"], cell["row"], cell["col"])
                for cell in raw_group.get("cells", [])
            )
            groups.append(SharedGroup(name=name, cells=cells))
    return tuple(groups)


def reverify_run(ledger: Ledger, run_id: str) -> ReverifyOutcome:
    state = ledger.load_for_verification(run_id)
    tables: list[dict[str, Any]] = []
    checks: list[dict[str, Any]] = []
    passed = True
    models: dict[str, TableModel] = {}
    suppressions: dict[str, dict[tuple[str, str], SuppressionReason]] = {}
    primaries: dict[str, set[tuple[str, str]]] = {}
    public_zeros: dict[str, set[tuple[str, str]]] = {}
    limits: dict[str, float] = {}
    for table in state["tables"]:
        try:
            model = _model_from_ledger(table)
        except ValueError as exc:
            passed = False
            checks.append(
                {
                    "table_id": table["table_id"],
                    "name": "ledger_matrix_complete",
                    "result": "fail",
                    "evidence": {"detail": str(exc)},
                }
            )
            continue
        suppressed: dict[tuple[str, str], SuppressionReason] = {}
        for cell in table["cells"]:
            if cell["suppression_type"] == SUPPRESSION_NONE:
                continue
            suppressed[(cell["row_key"], cell["col_key"])] = SuppressionReason(
                kind=cell["suppression_type"],
                risk_category=cell["risk_category"],
                detail="restored from local ledger for independent re-verification",
                round_no=0,
            )
        primary = {
            (cell["row_key"], cell["col_key"])
            for cell in table["cells"]
            if cell["suppression_type"] == "primary"
        }
        models[model.table_id] = model
        suppressions[model.table_id] = suppressed
        primaries[model.table_id] = primary
        public_zeros[model.table_id] = {
            (cell["row"], cell["col"])
            for cell in table["meta"].get("public_zero_cells", []) or []
        }
        limit = table["meta"].get("max_suppression_ratio")
        if limit is not None:
            limits[model.table_id] = float(limit)
        # Keep the single-table result available for per-table summaries.
        result = verify_table(
            model,
            suppressed,
            primary_coords=primary,
            max_suppression_ratio=limit,
        )
        passed = passed and result.passed
        tables.append({"table_id": model.table_id, "passed": result.passed})
        for check in result.checks:
            checks.append(
                {
                    "table_id": model.table_id,
                    "name": check.name,
                    "result": "pass" if check.passed else "fail",
                    "evidence": {"detail": check.detail, "source": "reverify"},
                }
            )

    groups = _groups_from_ledger(state["tables"])
    if groups:
        joint = verify_joint(
            groups,
            models,
            suppressions,
            primary_coords=primaries,
            max_suppression_ratios=limits,
            public_zero_coords=public_zeros,
        )
        passed = passed and joint.passed
        for check in joint.checks:
            checks.append(
                {
                    "table_id": None,
                    "name": check.name,
                    "result": "pass" if check.passed else "fail",
                    "evidence": {
                        "detail": check.detail,
                        "source": "reverify",
                        "scope": "joint",
                    },
                }
            )
        for table in tables:
            joint_result = joint.per_table.get(table["table_id"])
            if joint_result is not None:
                table["passed"] = table["passed"] and joint_result.passed
        if not joint.passed:
            # Whole-release checks are not attributable to a single table:
            # every member of a declared relation shares the verdict so the
            # re-verification output cannot show "all tables pass".
            grouped = {cell.table_id for group in groups for cell in group.cells}
            for table in tables:
                if table["table_id"] in grouped:
                    table["passed"] = False
    ledger.clear_reverify_checks(run_id)
    if checks:
        ledger.add_checks(run_id, checks, source="reverify")
    ledger.update_table_verification(
        run_id, {table["table_id"]: table["passed"] for table in tables}
    )
    current_status = state["run"]["status"]
    if not passed:
        new_status = "blocked" if current_status == "blocked" else "verify_failed"
    else:
        new_status = current_status if current_status in {"ok", "blocked"} else "ok"
    if new_status != current_status:
        ledger.update_status(run_id, new_status)
    return ReverifyOutcome(run_id=run_id, passed=passed, status=new_status, tables=tables)
