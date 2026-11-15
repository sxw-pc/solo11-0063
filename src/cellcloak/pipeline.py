"""End-to-end run orchestration and ledger-backed re-verification."""

from __future__ import annotations

import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .contract import Contract, TableSpec, load_contract
from .ingest import load_table, load_table_from_bytes
from .ledger import Ledger
from .manifest import (
    DEFAULT_INPUT_MODE,
    CapturedInputSet,
    InputManifest,
    capture_input_set,
    load_manifest,
)
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
        "source_size": model.source_size,
        "risk_level": evaluation.risk.risk_level(evaluation.suppression.blocked),
        "risk_class_counts": {label: int(evaluation.risk.counts.get(label, 0)) for label in RISK_LABELS},
        "info_loss": {
            "suppressed_count_sum_ratio": evaluation.suppression.suppressed_count_sum_ratio,
            "max_row_suppression_ratio": evaluation.suppression.max_row_suppression_ratio,
            "max_col_suppression_ratio": evaluation.suppression.max_col_suppression_ratio,
        },
    }


def _input_set_record(
    contract: Contract,
    manifest: InputManifest | None,
    captured: CapturedInputSet | None,
    evaluations: list[EvaluatedTable],
) -> dict[str, Any]:
    """One input-set identity shared by report and ledger.

    With a manifest the members come from the verified manifest itself, not
    from the live paths, so the record proves what was captured and parsed.
    Without a manifest the run is explicitly marked as an unpinned input set
    while preserving the historical per-table fields.
    """

    if manifest is not None and captured is not None:
        record = captured.input_set_record()
        used = {
            evaluation.model.table_id: (
                evaluation.model.source_sha256,
                evaluation.model.source_size,
            )
            for evaluation in evaluations
        }
        members: list[dict[str, Any]] = []
        for member in manifest.members:
            observed_sha, observed_size = used.get(member.table_id, (None, None))
            members.append(
                {
                    **member.as_record(),
                    "parsed_sha256": observed_sha,
                    "parsed_size": observed_size,
                    "matches": observed_sha == member.sha256 and observed_size == member.size,
                }
            )
        record["members"] = members
        return record
    return {
        "pinned": False,
        "mode": "legacy",
        "manifest_hash": None,
        "manifest_path": None,
        "contract_hash": contract.normalized_hash,
        "dataset_name": contract.dataset_name,
        "members": [
            {
                "table_id": evaluation.model.table_id,
                "source": evaluation.model.source_name,
                "sha256": evaluation.model.source_sha256,
                "size": evaluation.model.source_size,
                "parsed_sha256": evaluation.model.source_sha256,
                "parsed_size": evaluation.model.source_size,
                "matches": True,
            }
            for evaluation in evaluations
        ],
        "note": "unpinned input set: sources were read per table without a manifest",
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
    input_set: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    table_records: list[dict[str, Any]] = []
    cell_records: list[dict[str, Any]] = []
    check_records: list[dict[str, Any]] = []
    input_members = {
        str(member.get("table_id")): member
        for member in input_set.get("members", [])
        if isinstance(member, dict)
    }
    for evaluation in evaluations:
        model = evaluation.model
        table_status = _table_status(evaluation)
        member = input_members.get(model.table_id, {})
        meta = _table_meta(evaluation, contract)
        meta["input_set_pinned"] = bool(input_set.get("pinned"))
        meta["manifest_sha256"] = member.get("sha256")
        meta["input_set_manifest_hash"] = input_set.get("manifest_hash")
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
                "meta": meta,
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
        "input_set": input_set,
    }
    artifact_records = [artifact.as_record() for artifact in artifacts]
    return run_record, table_records, cell_records, check_records, artifact_records


def _evaluate(model: TableModel, spec: TableSpec, contract: Contract) -> EvaluatedTable:
    assessment = classify(model, spec, contract.privacy)
    suppression = suppress_table(model, spec, assessment, contract.privacy)
    verification = verify_table(
        model,
        suppression.suppressed,
        primary_coords=assessment.primary_coords,
        max_suppression_ratio=contract.privacy.max_suppression_ratio,
    )
    return EvaluatedTable(model, assessment, suppression, verification)


def execute_run(
    contract_path: str | Path,
    output_dir: str | Path,
    ledger: Ledger,
    *,
    run_id: str | None = None,
    manifest_path: str | Path | None = None,
    input_mode: str = DEFAULT_INPUT_MODE,
    snapshot_dir: str | Path | None = None,
) -> RunOutcome:
    """Run the pipeline, optionally pinned to a verified input-set manifest.

    Without ``manifest_path`` the legacy behaviour is preserved: each table is
    read from its live source in contract order and the run is recorded as an
    unpinned input set. With a manifest, the manifest signature and its
    contract/table binding are validated and every model is built from the
    verified capture. Any mismatch fails before public output or ledger writes.
    """

    started_at = _now()
    contract = load_contract(contract_path)
    run_id = run_id or new_run_id()

    manifest = load_manifest(manifest_path) if manifest_path is not None else None
    evaluations: list[EvaluatedTable] = []
    captured: CapturedInputSet | None = None
    try:
        if manifest is not None:
            captured = capture_input_set(
                contract, manifest, mode=input_mode, snapshot_dir=snapshot_dir
            )
        for spec in contract.tables:
            if captured is not None:
                member = captured.member(spec.table_id)
                model = load_table_from_bytes(
                    spec, captured.read_bytes(spec.table_id), expected_sha256=member.sha256
                )
            else:
                model = load_table(spec)
            evaluations.append(_evaluate(model, spec, contract))
        input_set = _input_set_record(contract, manifest, captured, evaluations)
    finally:
        if captured is not None:
            captured.close()

    blocked = any(item.suppression.blocked for item in evaluations)
    verify_failed = any(not item.verification.passed for item in evaluations)
    status = "blocked" if blocked else "verify_failed" if verify_failed else "ok"
    finished_at = _now()
    report = build_report(
        contract, run_id, status, started_at, finished_at, evaluations, input_set=input_set
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
        input_set,
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
        source_size=meta.get("source_size"),
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
