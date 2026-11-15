"""End-to-end run orchestration and ledger-backed re-verification."""

from __future__ import annotations

import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .contract import Contract, load_contract
from .errors import LedgerError
from .ingest import load_table
from .ledger import CellMaterial, Ledger, cell_stream_expectation
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


def _run_status(evaluations: list[EvaluatedTable]) -> str:
    if any(item.suppression.blocked for item in evaluations):
        return "blocked"
    if any(not item.verification.passed for item in evaluations):
        return "verify_failed"
    return "ok"


def _evaluate_contract(contract: Contract) -> list[EvaluatedTable]:
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
    return evaluations


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


def _table_records(
    contract: Contract, evaluations: list[EvaluatedTable]
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for evaluation in evaluations:
        model = evaluation.model
        records.append(
            {
                "table_id": model.table_id,
                "row_count": model.row_count(),
                "col_count": model.col_count(),
                "cell_count": model.cell_count(),
                "primary_count": evaluation.suppression.primary_count,
                "secondary_count": evaluation.suppression.secondary_count,
                "suppression_ratio": evaluation.suppression.suppression_ratio,
                "table_status": _table_status(evaluation),
                "block_reasons": [
                    {"code": reason.code, "detail": reason.detail}
                    for reason in evaluation.suppression.block_reasons
                ],
                "verify_passed": evaluation.verification.passed,
                "meta": _table_meta(evaluation, contract),
            }
        )
    return records


def _check_records(evaluations: list[EvaluatedTable]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for evaluation in evaluations:
        for check in evaluation.verification.checks:
            records.append(
                {
                    "table_id": evaluation.model.table_id,
                    "name": check.name,
                    "result": "pass" if check.passed else "fail",
                    "evidence": {"detail": check.detail},
                    "source": "run",
                }
            )
    return records


def _run_record(
    contract: Contract,
    run_id: str,
    status: str,
    started_at: str,
    finished_at: str,
    out_dir: Path,
    summary: dict[str, Any],
) -> dict[str, Any]:
    return {
        "id": run_id,
        "dataset_name": contract.dataset_name,
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "contract_hash": contract.contract_hash,
        "contract_path": str(contract.path),
        "output_dir": str(out_dir),
        "summary": summary,
    }


def _iter_cell_material(evaluations: list[EvaluatedTable]) -> Iterator[CellMaterial]:
    """Canonical cell stream: table by table, row by row, col by col.

    Shared by the seal-expectation pass and the chunked write pass so both
    fold the exact same coordinate sequence. Runs in O(1) extra memory.
    """

    for evaluation in evaluations:
        model = evaluation.model
        risk_by_coordinate = {finding.coordinate: finding.category for finding in evaluation.risk.findings}
        suppressed = evaluation.suppression.suppressed
        for cell in model.cells():
            reason = suppressed.get(cell.coordinate)
            yield (
                model.table_id,
                cell.row,
                cell.col,
                cell.count,
                reason.kind if reason else SUPPRESSION_NONE,
                risk_by_coordinate[(cell.row, cell.col)],
            )


def _iter_cell_records(evaluations: list[EvaluatedTable]) -> Iterator[dict[str, Any]]:
    for table_id, row, col, count, suppression, risk in _iter_cell_material(evaluations):
        yield {
            "table_id": table_id,
            "row_key": row,
            "col_key": col,
            "raw_count": count,
            "suppression_type": suppression,
            "risk_category": risk,
            "fingerprint": cell_fingerprint(table_id, row, col),
        }


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
    evaluations = _evaluate_contract(contract)

    status = _run_status(evaluations)
    finished_at = _now()
    report = build_report(contract, run_id, status, started_at, finished_at, evaluations)
    out_dir = Path(output_dir).expanduser().resolve()
    artifacts = emit_outputs(out_dir, report, evaluations, publish=status == "ok")

    run_record = _run_record(contract, run_id, status, started_at, finished_at, out_dir, report["summary"])
    table_records = _table_records(contract, evaluations)
    check_records = _check_records(evaluations)
    artifact_records = [artifact.as_record() for artifact in artifacts]
    # First streaming pass: fold the canonical cell stream into the seal
    # expectation. O(chunk) memory, no cell records are materialised.
    cell_expect = cell_stream_expectation(run_id, _iter_cell_material(evaluations), ledger.chunk_size)
    ledger.begin_run(run_record, table_records, check_records, artifact_records, cell_expect=cell_expect)
    # Second streaming pass: bounded chunks, one short transaction each. If
    # anything fails here the unsealed recording stays in the ledger and can
    # be resumed (`ledger resume`) or discarded (`ledger abort`).
    ledger.stream_cells(run_id, _iter_cell_records(evaluations))
    ledger.seal_run(run_id)
    return RunOutcome(
        run_id=run_id,
        status=status,
        output_dir=out_dir,
        report=report,
        artifacts=artifacts,
    )


def _verify_recording_metadata(
    state: dict[str, Any],
    ledger: Ledger,
    contract: Contract,
    evaluations: list[EvaluatedTable],
) -> None:
    """Ensure a re-executed pipeline reproduces the interrupted recording."""

    if contract.contract_hash != state["contract_hash"]:
        raise LedgerError(
            "E_LEDGER_RESUME",
            "contract changed since the recording started; abort the recording and re-run",
            table_id=state["id"],
        )
    status = _run_status(evaluations)
    if status != state["status"]:
        raise LedgerError(
            "E_LEDGER_RESUME",
            f"re-executed pipeline status {status!r} does not match the recording {state['status']!r}",
            table_id=state["id"],
        )
    regenerated = {
        item["table_id"]: item for item in _table_records(contract, evaluations)
    }
    detail = ledger.load_recording_tables(state["id"])
    stored = {item["table_id"]: item for item in detail}
    if set(stored) != set(regenerated):
        raise LedgerError(
            "E_LEDGER_RESUME",
            "re-executed pipeline tables do not match the recording",
            table_id=state["id"],
        )
    for table_id, current in regenerated.items():
        previous = stored[table_id]
        for field in ("row_count", "col_count", "cell_count", "primary_count", "secondary_count", "table_status"):
            if previous[field] != current[field]:
                raise LedgerError(
                    "E_LEDGER_RESUME",
                    f"table {table_id} field {field} changed since the recording started",
                    table_id=state["id"],
                )
    expect = state["expect"]
    chunk_size = int(expect.get("chunk_size") or ledger.chunk_size)
    fresh = cell_stream_expectation(state["id"], _iter_cell_material(evaluations), chunk_size)
    if fresh["cell_count"] != expect.get("cell_count") or fresh["digest"] != expect.get("digest"):
        raise LedgerError(
            "E_LEDGER_RESUME",
            "re-executed cell stream does not match the seal expectation; "
            "abort the recording and re-run",
            table_id=state["id"],
        )


def resume_run(ledger: Ledger, run_id: str) -> RunOutcome:
    """Finish an interrupted recording after a restart.

    The pipeline is re-executed deterministically from the recorded contract;
    already committed chunks are verified and skipped, the remaining chunks
    are appended, and the run is sealed. Recorded outputs are not rewritten.
    """

    state = ledger.recording_state(run_id)
    if state is None:
        raise LedgerError("E_RUN_NOT_FOUND", "run is not present in the local ledger", table_id=run_id)
    if state["sealed"]:
        raise LedgerError("E_RUN_SEALED", "run is already sealed; nothing to resume", table_id=run_id)
    contract = load_contract(state["contract_path"])
    evaluations = _evaluate_contract(contract)
    _verify_recording_metadata(state, ledger, contract, evaluations)
    ledger.stream_cells(run_id, _iter_cell_records(evaluations))
    ledger.seal_run(run_id)
    detail = ledger.get_run_detail(run_id)
    artifacts = [
        Artifact(Path(item["path"]), item["kind"], item["sha256"])
        for item in (detail["artifacts"] if detail else [])
    ]
    return RunOutcome(
        run_id=run_id,
        status=state["status"],
        output_dir=Path(state["output_dir"]),
        report={"summary": state["summary"]},
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
    state = ledger.load_for_verification(run_id)
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
