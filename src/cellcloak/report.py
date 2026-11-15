"""Public table publication and redacted audit-report assembly."""

from __future__ import annotations

import csv
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .contract import Contract
from .errors import PipelineError
from .model import RISK_BLOCKED, RISK_LABELS, TableModel, cell_fingerprint
from .risk import RiskAssessment
from .suppress import SuppressionResult
from .verify import VerifyResult

MARKER_NAME = ".cellcloak-output.json"


@dataclass(frozen=True)
class EvaluatedTable:
    model: TableModel
    risk: RiskAssessment
    suppression: SuppressionResult
    verification: VerifyResult


@dataclass(frozen=True)
class Artifact:
    path: Path
    kind: str
    sha256: str

    def as_record(self) -> dict[str, str]:
        return {"path": str(self.path), "kind": self.kind, "sha256": self.sha256}


def _table_status(evaluation: EvaluatedTable) -> str:
    if evaluation.suppression.blocked:
        return "blocked"
    if not evaluation.verification.passed:
        return "verify_failed"
    return "ok"


def _verification_payload(result: VerifyResult) -> dict[str, Any]:
    return {
        "passed": result.passed,
        "checks": [
            {"name": check.name, "result": "pass" if check.passed else "fail", "detail": check.detail}
            for check in result.checks
        ],
        "evidence": [
            {
                "row": item.row,
                "col": item.col,
                "fingerprint": item.fingerprint,
                "suppression_type": item.suppression_type,
                "risk_category": item.risk_category,
                "row_suppressed_count": item.row_suppressed_count,
                "col_suppressed_count": item.col_suppressed_count,
                "directly_inferable": item.directly_inferable,
                "deduced_via": list(item.deduced_via),
            }
            for item in result.evidence
        ],
    }


def build_report(
    contract: Contract,
    run_id: str,
    status: str,
    started_at: str,
    finished_at: str,
    evaluations: list[EvaluatedTable],
    *,
    input_set: dict[str, Any] | None = None,
) -> dict[str, Any]:
    tables: list[dict[str, Any]] = []
    risk_totals = {label: 0 for label in RISK_LABELS}
    total_cells = 0
    total_suppressed = 0
    total_primary = 0
    total_secondary = 0
    blocked_tables: list[dict[str, str]] = []
    verify_passed = True
    input_members = {
        str(member.get("table_id")): member
        for member in (input_set or {}).get("members", [])
        if isinstance(member, dict)
    }

    for evaluation in evaluations:
        model = evaluation.model
        suppression = evaluation.suppression
        table_status = _table_status(evaluation)
        blocked = table_status == "blocked"
        verify_passed = verify_passed and evaluation.verification.passed
        if blocked:
            blocked_tables.extend(
                {"table_id": model.table_id, "code": reason.code, "detail": reason.detail}
                for reason in suppression.block_reasons
            )
        risk_counts = {
            label: int(evaluation.risk.counts.get(label, 0)) for label in RISK_LABELS
        }
        for label in RISK_LABELS[:-1]:
            risk_totals[label] += risk_counts[label]
        risk_totals[RISK_BLOCKED] += int(blocked)
        suppressed_cells = [
            {
                "row": row,
                "col": col,
                "fingerprint": cell_fingerprint(model.table_id, row, col),
                "suppression_type": reason.kind,
                "risk_category": reason.risk_category,
                "round": reason.round_no,
            }
            for (row, col), reason in sorted(suppression.suppressed.items())
        ]
        tables.append(
            {
                "table_id": model.table_id,
                "source": model.source_name,
                "source_sha256": model.source_sha256,
                "source_size": model.source_size,
                "input_member": input_members.get(model.table_id) if (input_set or {}).get("pinned") else None,
                "row_dim": model.row_dim,
                "col_dim": model.col_dim,
                "row_count": model.row_count(),
                "col_count": model.col_count(),
                "cell_count": model.cell_count(),
                "primary_count": suppression.primary_count,
                "secondary_count": suppression.secondary_count,
                "suppressed_cell_count": len(suppression.suppressed),
                "suppression_ratio": suppression.suppression_ratio,
                "table_status": table_status,
                "risk_level": evaluation.risk.risk_level(blocked),
                "risk_class_counts": risk_counts,
                "block_reasons": [
                    {"code": reason.code, "detail": reason.detail}
                    for reason in suppression.block_reasons
                ],
                "info_loss": {
                    "suppressed_cell_count": len(suppression.suppressed),
                    "cell_suppression_ratio": suppression.suppression_ratio,
                    "suppressed_count_sum_ratio": suppression.suppressed_count_sum_ratio,
                    "max_row_suppression_ratio": suppression.max_row_suppression_ratio,
                    "max_col_suppression_ratio": suppression.max_col_suppression_ratio,
                },
                "suppressed_cells": suppressed_cells,
                "verification": _verification_payload(evaluation.verification),
            }
        )
        total_cells += model.cell_count()
        total_suppressed += len(suppression.suppressed)
        total_primary += suppression.primary_count
        total_secondary += suppression.secondary_count

    summary = {
        "table_count": len(evaluations),
        "cell_count": total_cells,
        "primary_count": total_primary,
        "secondary_count": total_secondary,
        "suppressed_cell_count": total_suppressed,
        "suppression_ratio": (total_suppressed / total_cells) if total_cells else 0.0,
        "blocked_table_count": sum(table["table_status"] == "blocked" for table in tables),
        "block_reasons": blocked_tables,
        "verify_passed": verify_passed,
        "risk_class_counts": risk_totals,
        "risk_class_note": "blocked counts tables; all other labels count source cells",
    }
    return {
        "report_version": 1,
        "title": "Cellcloak audit report",
        "dataset_name": contract.dataset_name,
        "contract_version": contract.version,
        "contract_hash": contract.contract_hash,
        "normalized_contract_hash": contract.normalized_hash,
        "run_id": run_id,
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "input_set": input_set
        or {
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
        },
        "inputs": [
            {
                "table_id": evaluation.model.table_id,
                "source": evaluation.model.source_name,
                "sha256": evaluation.model.source_sha256,
                "size": evaluation.model.source_size,
                "manifest_sha256": input_members.get(evaluation.model.table_id, {}).get("sha256"),
                "input_set_pinned": bool((input_set or {}).get("pinned")),
            }
            for evaluation in evaluations
        ],
        "summary": summary,
        "tables": tables,
    }


def _public_csv(path: Path, model: TableModel, suppression: SuppressionResult) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow([model.row_dim, *model.col_keys, "Total"])
        for row in model.row_keys:
            values = [
                "NA" if (row, col) in suppression.suppressed else str(model.count(row, col))
                for col in model.col_keys
            ]
            writer.writerow([row, *values, str(model.row_total(row))])
        writer.writerow(
            ["Total", *[str(model.col_total(col)) for col in model.col_keys], str(model.total())]
        )


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(content, encoding="utf-8", newline="\n")
    os.replace(temporary, path)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _clean_previous_artifacts(root: Path, marker: Path) -> None:
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
        relative_paths = payload.get("artifacts", [])
        if not isinstance(relative_paths, list):
            raise ValueError("invalid artifact list")
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise PipelineError("E_OUTPUT_MARKER", "output directory marker is invalid", path=root) from exc
    for relative in relative_paths:
        if not isinstance(relative, str):
            continue
        candidate = (root / relative).resolve()
        try:
            candidate.relative_to(root)
        except ValueError:
            continue
        if candidate.is_file() and candidate.name != MARKER_NAME:
            candidate.unlink()


def _prepare_output(root: Path) -> None:
    if root.exists() and not root.is_dir():
        raise PipelineError("E_OUTPUT_NOT_DIRECTORY", "output path is not a directory", path=root)
    root.mkdir(parents=True, exist_ok=True)
    marker = root / MARKER_NAME
    if marker.exists():
        _clean_previous_artifacts(root, marker)
    else:
        for name in ("public", "audit"):
            candidate = root / name
            if candidate.exists() and any(candidate.iterdir()):
                raise PipelineError(
                    "E_OUTPUT_NOT_EMPTY",
                    "refusing to overwrite public/audit content without a Cellcloak marker",
                    path=root,
                )
    (root / "public").mkdir(exist_ok=True)
    (root / "audit").mkdir(exist_ok=True)


def emit_outputs(
    output_dir: Path,
    report: dict[str, Any],
    evaluations: list[EvaluatedTable],
    *,
    publish: bool,
) -> list[Artifact]:
    root = output_dir.expanduser().resolve()
    _prepare_output(root)
    artifacts: list[Artifact] = []
    if publish:
        for evaluation in evaluations:
            path = root / "public" / f"{evaluation.model.table_id}.csv"
            _public_csv(path, evaluation.model, evaluation.suppression)
            artifacts.append(Artifact(path, f"public_table:{evaluation.model.table_id}", _sha256(path)))

    audit = root / "audit"
    json_path = audit / "report.json"
    _write_text(json_path, json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    artifacts.append(Artifact(json_path, "audit_json", _sha256(json_path)))

    from .report_markdown import render_markdown

    md_path = audit / "report.md"
    _write_text(md_path, render_markdown(report))
    artifacts.append(Artifact(md_path, "audit_md", _sha256(md_path)))

    environment = Environment(
        loader=FileSystemLoader(str(Path(__file__).parent / "templates")),
        autoescape=select_autoescape(("html",)),
    )
    html_path = audit / "report.html"
    html = environment.get_template("report.html").render(report=report)
    _write_text(html_path, html)
    artifacts.append(Artifact(html_path, "audit_html", _sha256(html_path)))

    marker_payload = {
        "version": 1,
        "run_id": report["run_id"],
        "artifacts": [str(artifact.path.relative_to(root)).replace("\\", "/") for artifact in artifacts],
    }
    _write_text(root / MARKER_NAME, json.dumps(marker_payload, indent=2, sort_keys=True) + "\n")
    return artifacts
