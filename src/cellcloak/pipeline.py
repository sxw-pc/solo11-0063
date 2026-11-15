"""End-to-end run orchestration and ledger-backed re-verification."""

from __future__ import annotations

import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .contract import Contract, load_contract
from .errors import LedgerError
from .ingest import load_table
from .ledger import Ledger
from .model import (
    RISK_LABELS,
    SUPPRESSION_NONE,
    SuppressionReason,
    TableModel,
    cell_fingerprint,
)
from .report import Artifact, EvaluatedTable, build_report, emit_outputs
from .risk import classify
from .suppress import suppress_table
from .verify import verify_table


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


def _table_meta(evaluation: EvaluatedTable, contract: Contract) -> dict[str, Any]:
    model = evaluation.model
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
        "info_loss": {
            "suppressed_count_sum_ratio": evaluation.suppression.suppressed_count_sum_ratio,
            "max_row_suppression_ratio": evaluation.suppression.max_row_suppression_ratio,
            "max_col_suppression_ratio": evaluation.suppression.max_col_suppression_ratio,
        },
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
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    table_records: list[dict[str, Any]] = []
    cell_records: list[dict[str, Any]] = []
    check_records: list[dict[str, Any]] = []
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
    evaluations: list[EvaluatedTable] = []
    for spec in contract.tables:
        model = load_table(spec)
        assessment = classify(model, spec, contract.privacy)
        suppression = suppress_table(model, spec, assessment, contract.privacy)
        verification = verify_table(
            model,
            suppression.suppressed,
            primary_coords=assessment.primary_coords,
            max_suppression_ratio=contract.privacy.max_suppression_ratio,
        )
        evaluations.append(EvaluatedTable(model, assessment, suppression, verification))

    blocked = any(item.suppression.blocked for item in evaluations)
    verify_failed = any(not item.verification.passed for item in evaluations)
    status = "blocked" if blocked else "verify_failed" if verify_failed else "ok"
    finished_at = _now()
    report = build_report(contract, run_id, status, started_at, finished_at, evaluations)
    out_dir = Path(output_dir).expanduser().resolve()
    artifacts = emit_outputs(out_dir, report, evaluations, publish=status == "ok")
    payload = _ledger_payload(
        contract, run_id, status, started_at, finished_at, out_dir, report, evaluations, artifacts
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


def reverify_run(ledger: Ledger, run_id: str) -> ReverifyOutcome:
    try:
        state = ledger.load_for_verification(run_id)
    except LedgerError as exc:
        # A run whose raw records are gone (or going) must never look simply
        # "missing" or vacuously verified; report the purge state explicitly.
        if exc.code in ("E_RUN_PURGED", "E_RUN_PURGING"):
            status = "purged" if exc.code == "E_RUN_PURGED" else "purging"
            return ReverifyOutcome(run_id=run_id, passed=False, status=status, tables=[])
        raise
    tables: list[dict[str, Any]] = []
    checks: list[dict[str, Any]] = []
    passed = True
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
        result = verify_table(
            model,
            suppressed,
            primary_coords=primary,
            max_suppression_ratio=table["meta"].get("max_suppression_ratio"),
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
    ledger.clear_reverify_checks(run_id)
    if checks:
        ledger.add_checks(run_id, checks, source="reverify")
    current_status = state["run"]["status"]
    if not passed:
        new_status = "blocked" if current_status == "blocked" else "verify_failed"
    else:
        new_status = current_status if current_status in {"ok", "blocked"} else "ok"
    if new_status != current_status:
        ledger.update_status(run_id, new_status)
    return ReverifyOutcome(run_id=run_id, passed=passed, status=new_status, tables=tables)
