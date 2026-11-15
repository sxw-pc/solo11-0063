"""Public table publication and redacted audit-report assembly.

Release layout and commit protocol
----------------------------------
Every run publishes one immutable *generation* directory::

    <out>/generations/<run_id>/
        public/<table_id>.csv        # only populated for publishable runs
        audit/report.json|md|html
        .cellcloak-output.json       # generation marker (version 2)

A generation is written under ``generations/.staging-<run_id>/`` and sealed
with a single ``os.rename``. It becomes the *current release* only when the
``current`` symlink is atomically replaced (``os.replace`` of a symlink) —
that single pointer flip is the commit point for the whole generation, so
readers never observe a half-written mix of two runs. The browsing paths
``<out>/public``, ``<out>/audit`` and ``<out>/.cellcloak-output.json`` are
stable symlinks that resolve through ``current``.

Because SQLite and the filesystem cannot commit atomically together, the
ordering is fixed (see ``pipeline.execute_run``): seal generation → commit
ledger record → flip ``current``. Therefore ``current`` always points at a
generation whose run is recorded in the ledger, and ledger artifact paths
always reference files that still exist with matching SHA-256, because
sealed generations are never modified or deleted by the tool. Leftover
``.staging-*`` directories come from interrupted runs, were never sealed,
recorded or pointed to, and are removed at the start of the next run.
Sealed generations that never became current (ledger commit failed) are
kept for forensics; operators may prune any generation that is neither
referenced by the ledger nor the current pointer. Incomplete runs are not
resumed — a retry always creates a fresh run ID and generation.

Output directories written by older versions (real ``public/``/``audit/``
directories plus a version-1 marker file) are still readable, both by
:func:`read_current_release` and by the publisher, which archives the
legacy content into ``generations/legacy-<run_id>/`` before publishing.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import shutil
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
GENERATIONS_DIR = "generations"
CURRENT_LINK = "current"
_STAGING_PREFIX = ".staging-"
_RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")


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


@dataclass(frozen=True)
class Release:
    """A sealed generation plus its ledger-ready artifact records.

    The generation is complete on disk but is not the current release until
    :meth:`publish` flips the ``current`` pointer. The pipeline commits the
    ledger record between sealing and publishing.
    """

    root: Path
    run_id: str
    generation_dir: Path
    artifacts: tuple[Artifact, ...]

    def publish(self) -> None:
        """Atomically make this generation the current release."""
        _publish_current(self.root, self.run_id)


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
) -> dict[str, Any]:
    tables: list[dict[str, Any]] = []
    risk_totals = {label: 0 for label in RISK_LABELS}
    total_cells = 0
    total_suppressed = 0
    total_primary = 0
    total_secondary = 0
    blocked_tables: list[dict[str, str]] = []
    verify_passed = True

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
        "run_id": run_id,
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "inputs": [
            {"table_id": evaluation.model.table_id, "source": evaluation.model.source_name,
             "sha256": evaluation.model.source_sha256}
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


def _load_marker_payload(marker: Path) -> dict[str, Any]:
    """Strictly parse a legacy marker; any defect refuses the output directory."""

    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PipelineError("E_OUTPUT_MARKER", "output directory marker is invalid", path=marker) from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("artifacts", []), list):
        raise PipelineError("E_OUTPUT_MARKER", "output directory marker is invalid", path=marker)
    return payload


def _read_marker_tolerant(marker: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict) or not isinstance(payload.get("artifacts", []), list):
        return None
    return payload


def _has_release_layout(root: Path) -> bool:
    return (
        (root / CURRENT_LINK).is_symlink()
        or (root / MARKER_NAME).is_symlink()
        or (root / GENERATIONS_DIR).is_dir()
    )


def _adopt_legacy_layout(root: Path, marker: Path) -> None:
    """Archive a version-1 output directory into ``generations/legacy-<run_id>/``.

    The legacy marker is validated first (invalid markers refuse the run, as
    before). Content is moved, not deleted, so the previous run's files stay
    on disk for forensics even though the ledger still references the old
    in-place paths. The marker is moved last so a crash mid-adoption is
    re-adopted on the next run.
    """

    payload = _load_marker_payload(marker)
    run_id = payload.get("run_id")
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", str(run_id)).strip(".") if run_id else ""
    safe = safe or "unknown"
    generations = root / GENERATIONS_DIR
    generations.mkdir(exist_ok=True)
    archive = generations / f"legacy-{safe}"
    suffix = 2
    while archive.exists():
        archive = generations / f"legacy-{safe}-{suffix}"
        suffix += 1
    archive.mkdir()
    for name in ("public", "audit", MARKER_NAME):
        candidate = root / name
        if candidate.is_symlink() or not candidate.exists():
            continue
        os.replace(candidate, archive / name)


def _prepare_output(root: Path) -> None:
    if root.exists() and not root.is_dir():
        raise PipelineError("E_OUTPUT_NOT_DIRECTORY", "output path is not a directory", path=root)
    root.mkdir(parents=True, exist_ok=True)
    marker = root / MARKER_NAME
    generations = root / GENERATIONS_DIR
    if generations.exists() and not generations.is_dir():
        raise PipelineError("E_OUTPUT_NOT_DIRECTORY", "generations path is not a directory", path=generations)
    if marker.is_file() and not marker.is_symlink():
        # Version-1 directory: real marker file. Adopt it into the generation
        # layout even if a crashed adoption left generations/ behind.
        _adopt_legacy_layout(root, marker)
    elif not _has_release_layout(root):
        offenders = [
            root / name
            for name in ("public", "audit")
            if (root / name).is_dir()
            and not (root / name).is_symlink()
            and any((root / name).iterdir())
        ]
        if offenders:
            raise PipelineError(
                "E_OUTPUT_NOT_EMPTY",
                "refusing to overwrite public/audit content without a Cellcloak marker",
                path=root,
            )
        for name in ("public", "audit"):
            candidate = root / name
            if candidate.is_dir() and not candidate.is_symlink():
                candidate.rmdir()  # empty real dirs make way for release links
    generations.mkdir(exist_ok=True)
    # Staging directories were never sealed, recorded in the ledger, or
    # pointed to by `current`; removing them cannot invalidate anything.
    for staging in generations.glob(f"{_STAGING_PREFIX}*"):
        if staging.is_dir() and not staging.is_symlink():
            shutil.rmtree(staging, ignore_errors=True)


def _validate_run_id(run_id: str) -> None:
    if not _RUN_ID_PATTERN.match(run_id):
        raise PipelineError("E_RUN_ID_INVALID", "run ID is not safe to use as a generation directory name")


def emit_outputs(
    output_dir: Path,
    report: dict[str, Any],
    evaluations: list[EvaluatedTable],
    *,
    publish: bool,
) -> Release:
    """Stage and seal one immutable generation; the caller publishes it later.

    The whole release (public tables, three audit reports, marker) is written
    under a staging directory and sealed with one ``os.rename``. Nothing in
    the output root changes visibility here: the ``current`` pointer only
    moves when the returned :class:`Release` is published after the ledger
    commit. Artifact paths and digests are computed from the sealed files so
    the ledger always references their final, immutable location.
    """

    root = output_dir.expanduser().resolve()
    run_id = str(report["run_id"])
    _validate_run_id(run_id)
    _prepare_output(root)
    generations = root / GENERATIONS_DIR
    final = generations / run_id
    if final.exists() or final.is_symlink():
        raise PipelineError("E_RUN_DUPLICATE", "a sealed generation already exists for this run ID", path=final)
    staging = generations / f"{_STAGING_PREFIX}{run_id}"
    staging.mkdir()
    relative_artifacts: list[tuple[str, str]] = []
    try:
        (staging / "public").mkdir()
        (staging / "audit").mkdir()
        if publish:
            for evaluation in evaluations:
                relative = f"public/{evaluation.model.table_id}.csv"
                _public_csv(staging / relative, evaluation.model, evaluation.suppression)
                relative_artifacts.append((relative, f"public_table:{evaluation.model.table_id}"))

        json_relative = "audit/report.json"
        _write_text(
            staging / json_relative,
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        )
        relative_artifacts.append((json_relative, "audit_json"))

        from .report_markdown import render_markdown

        md_relative = "audit/report.md"
        _write_text(staging / md_relative, render_markdown(report))
        relative_artifacts.append((md_relative, "audit_md"))

        environment = Environment(
            loader=FileSystemLoader(str(Path(__file__).parent / "templates")),
            autoescape=select_autoescape(("html",)),
        )
        html_relative = "audit/report.html"
        html = environment.get_template("report.html").render(report=report)
        _write_text(staging / html_relative, html)
        relative_artifacts.append((html_relative, "audit_html"))

        marker_payload = {
            "version": 2,
            "run_id": run_id,
            "status": report.get("status"),
            "artifacts": [relative for relative, _ in relative_artifacts],
        }
        _write_text(
            staging / MARKER_NAME,
            json.dumps(marker_payload, indent=2, sort_keys=True) + "\n",
        )
        os.rename(staging, final)
    except BaseException:
        # The staging directory was never sealed, recorded, or pointed to.
        shutil.rmtree(staging, ignore_errors=True)
        raise
    artifacts = tuple(
        Artifact(final / relative, kind, _sha256(final / relative))
        for relative, kind in relative_artifacts
    )
    return Release(root=root, run_id=run_id, generation_dir=final, artifacts=artifacts)


def _publish_current(root: Path, run_id: str) -> None:
    """Flip the ``current`` pointer to a sealed generation in one atomic step.

    ``public``, ``audit`` and the root marker are stable symlinks that resolve
    through ``current``, so the single symlink replace below is the only
    operation readers can observe; the convenience links never need to change
    once created.
    """

    generation = root / GENERATIONS_DIR / run_id
    if not generation.is_dir():
        raise PipelineError("E_OUTPUT_PUBLISH", "sealed generation is missing", path=generation)
    current = root / CURRENT_LINK
    temporary = root / f".{CURRENT_LINK}.tmp"
    try:
        if temporary.is_symlink() or temporary.exists():
            temporary.unlink()
        temporary.symlink_to(f"{GENERATIONS_DIR}/{run_id}")
        os.replace(temporary, current)
        for name in ("public", "audit", MARKER_NAME):
            link = root / name
            desired = f"{CURRENT_LINK}/{name}"
            if link.is_symlink():
                if os.readlink(link) == desired:
                    continue
                link.unlink()
            elif link.exists():
                raise PipelineError(
                    "E_OUTPUT_PUBLISH",
                    "refusing to replace a real path with a release link",
                    path=link,
                )
            link.symlink_to(desired)
    except OSError as exc:
        raise PipelineError("E_OUTPUT_PUBLISH", "cannot switch the current release pointer", path=root) from exc


def read_current_release(output_dir: str | Path) -> dict[str, Any] | None:
    """Read the release an output directory currently presents, if any.

    Understands both the generation layout (via the ``current`` pointer) and
    legacy version-1 marker directories. Never raises for missing or corrupt
    content — a directory that cannot be interpreted simply has no readable
    current release. Artifact paths are resolved against the directory that
    holds the marker (the generation directory, or the root for legacy
    layouts).
    """

    root = Path(output_dir).expanduser().resolve()
    if not root.is_dir():
        return None
    current = root / CURRENT_LINK
    if current.is_symlink():
        try:
            generation = current.resolve(strict=True)
        except OSError:
            return None
        payload = _read_marker_tolerant(generation / MARKER_NAME)
        if payload is None:
            return None
        return _release_view("generations", root, generation, payload)
    marker = root / MARKER_NAME
    if marker.is_file() and not marker.is_symlink():
        payload = _read_marker_tolerant(marker)
        if payload is None:
            return None
        return _release_view("legacy", root, root, payload)
    return None


def _release_view(
    layout: str, root: Path, generation: Path, payload: dict[str, Any]
) -> dict[str, Any]:
    artifacts = payload.get("artifacts", [])
    return {
        "layout": layout,
        "run_id": payload.get("run_id"),
        "status": payload.get("status"),
        "root": root,
        "generation_dir": generation,
        "public_dir": generation / "public",
        "audit_dir": generation / "audit",
        "artifacts": [
            generation / item for item in artifacts if isinstance(item, str)
        ],
    }
