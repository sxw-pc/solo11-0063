"""Recoverable purge of retired runs: raw cell records and internal reports.

Lifecycle (route A): a durable ``purging`` tombstone is committed first, then
each ledger-registered internal-report file is re-validated against the live
filesystem and deleted only when every safety check passes, then the run's
database rows are cascade-deleted and the tombstone is marked ``purged`` in
the same transaction. Every step is idempotent, so re-running ``forget``
after a crash resumes safely; ``verify`` reports ``purging``/``purged``
instead of silently treating the run as missing.

File deletion never trusts the ledger path alone. A candidate is deleted only
when it is registered in the ledger for this run *and* confirmed by the
output directory's Cellcloak marker, stays inside the run's output directory
after symlink resolution, is not referenced by any other retained run, and
its current on-disk SHA-256 still matches the hash recorded for this run.
Anything else is kept and reported with a specific outcome.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .errors import CellcloakError, LedgerError
from .ledger import Ledger
from .report import MARKER_NAME

# File outcomes. ``would_delete`` is the only outcome that leads to unlink.
WOULD_DELETE = "would_delete"
DELETED = "deleted"
ALREADY_ABSENT = "already_absent"
KEPT_HASH_MISMATCH = "kept_hash_mismatch"
KEPT_SHARED_REFERENCE = "kept_shared_reference"
KEPT_NOT_IN_MANIFEST = "kept_not_in_manifest"
KEPT_MARKER_INVALID = "kept_marker_invalid"
KEPT_PATH_ESCAPE = "kept_path_escape"
KEPT_SYMLINK = "kept_symlink"
KEPT_NOT_A_FILE = "kept_not_a_file"
KEPT_ERROR = "kept_error"

_REASON_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def validate_reason_code(reason: str | None) -> str:
    normalized = (reason or "").strip()
    if not _REASON_PATTERN.match(normalized):
        raise CellcloakError(
            "E_FORGET_REASON",
            "reason code must be 1-64 characters from [A-Za-z0-9._:-] starting with a letter or digit",
        )
    return normalized


@dataclass
class FilePlan:
    path: str
    kind: str
    sha256: str
    outcome: str

    def as_dict(self) -> dict[str, str]:
        return {"path": self.path, "kind": self.kind, "sha256": self.sha256, "outcome": self.outcome}


@dataclass
class ForgetOutcome:
    run_id: str
    reason_code: str
    state: str  # "dry_run" | "purged" | "already_purged"
    resumed: bool
    files: list[FilePlan] = field(default_factory=list)
    db_rows: dict[str, int] = field(default_factory=dict)
    result: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "reason_code": self.reason_code,
            "state": self.state,
            "resumed": self.resumed,
            "files": [plan.as_dict() for plan in self.files],
            "db_rows": dict(self.db_rows),
            "result": self.result,
        }


@dataclass(frozen=True)
class _Marker:
    ok: bool
    entries: frozenset[str]


def _read_marker(root: Path) -> _Marker:
    """Confirm the output manifest belongs to this tool before trusting it."""

    try:
        payload = json.loads((root / MARKER_NAME).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return _Marker(False, frozenset())
    if not isinstance(payload, dict) or payload.get("version") != 1:
        return _Marker(False, frozenset())
    artifacts = payload.get("artifacts")
    if not isinstance(payload.get("run_id"), str) or not isinstance(artifacts, list):
        return _Marker(False, frozenset())
    if not all(isinstance(entry, str) for entry in artifacts):
        return _Marker(False, frozenset())
    return _Marker(True, frozenset(artifacts))


def _evaluate_file(
    item: Mapping[str, str],
    resolved_root: Path,
    marker: _Marker,
    shared: set[str],
) -> str:
    candidate = Path(item["path"])
    try:
        if candidate.is_symlink():
            return KEPT_SYMLINK
    except OSError:
        return KEPT_ERROR
    resolved = candidate.resolve()
    try:
        relative = resolved.relative_to(resolved_root)
    except ValueError:
        return KEPT_PATH_ESCAPE
    try:
        if not candidate.exists():
            return ALREADY_ABSENT
        if not candidate.is_file():
            return KEPT_NOT_A_FILE
    except OSError:
        return KEPT_ERROR
    if not marker.ok:
        return KEPT_MARKER_INVALID
    if relative.as_posix() not in marker.entries:
        return KEPT_NOT_IN_MANIFEST
    if str(candidate) in shared or str(resolved) in shared:
        return KEPT_SHARED_REFERENCE
    try:
        digest = hashlib.sha256(candidate.read_bytes()).hexdigest()
    except OSError:
        return KEPT_ERROR
    if digest != item["sha256"]:
        return KEPT_HASH_MISMATCH
    return WOULD_DELETE


def _delete_file(path: str) -> str:
    try:
        Path(path).unlink()
        return DELETED
    except FileNotFoundError:
        return ALREADY_ABSENT
    except OSError:
        return KEPT_ERROR


def _evaluate_all(
    ledger: Ledger, run_id: str, manifest: Mapping[str, Any]
) -> list[FilePlan]:
    root = Path(manifest["output_dir"]).expanduser()
    resolved_root = root.resolve()
    marker = _read_marker(resolved_root)
    files = manifest.get("files", [])
    shared = ledger.shared_reference_paths(run_id, [item["path"] for item in files])
    plans: list[FilePlan] = []
    for item in files:
        plans.append(
            FilePlan(
                path=item["path"],
                kind=item["kind"],
                sha256=item["sha256"],
                outcome=_evaluate_file(item, resolved_root, marker, shared),
            )
        )
    return plans


def _summarize_files(plans: Sequence[FilePlan]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    kinds: dict[str, str] = {}
    for plan in plans:
        counts[plan.outcome] = counts.get(plan.outcome, 0) + 1
        kinds[plan.kind] = plan.outcome
    kept = sum(count for outcome, count in counts.items() if outcome.startswith("kept_"))
    return {
        "outcome": "purged" if kept == 0 else "purged_with_kept_files",
        "files": counts,
        "kinds": kinds,
    }


def _manifest_for(ledger: Ledger, run_id: str, output_dir: str) -> dict[str, Any]:
    return {"output_dir": output_dir, "files": ledger.purge_candidates(run_id)}


def _valid_manifest(manifest: Any) -> bool:
    if not isinstance(manifest, dict) or not isinstance(manifest.get("output_dir"), str):
        return False
    files = manifest.get("files")
    if not isinstance(files, list):
        return False
    return all(
        isinstance(item, dict)
        and all(isinstance(item.get(key), str) for key in ("path", "kind", "sha256"))
        for item in files
    )


def forget_run(
    ledger: Ledger,
    run_id: str,
    reason_code: str,
    *,
    dry_run: bool = False,
) -> ForgetOutcome:
    """Purge a run's raw records and internal reports, or plan it (dry-run).

    Raises LedgerError(E_RUN_NOT_FOUND) when neither a run nor a tombstone
    exists. A finished purge is idempotent: repeating it reports
    ``already_purged`` without touching anything.
    """

    reason = validate_reason_code(reason_code)
    tombstone = ledger.get_tombstone(run_id)
    if tombstone is not None and tombstone["state"] == "purged":
        return ForgetOutcome(
            run_id=run_id,
            reason_code=reason,
            state="already_purged",
            resumed=False,
            result=tombstone["result"],
        )
    detail = ledger.get_run_detail(run_id)
    if detail is None:
        raise LedgerError("E_RUN_NOT_FOUND", "run is not present in the local ledger", table_id=run_id)

    resumed = tombstone is not None  # state == "purging": resume the interrupted purge
    if resumed:
        # The committed tombstone is authoritative; a fresh --reason cannot rewrite it.
        reason = tombstone["reason_code"]
    manifest = tombstone["manifest"] if resumed else None
    if not _valid_manifest(manifest):
        # Fresh purge, or a damaged working manifest: rebuild from the ledger.
        manifest = _manifest_for(ledger, run_id, detail["run"]["output_dir"])

    if dry_run:
        plans = _evaluate_all(ledger, run_id, manifest)
        return ForgetOutcome(
            run_id=run_id,
            reason_code=reason,
            state="dry_run",
            resumed=resumed,
            files=plans,
            db_rows=ledger.purge_row_counts(run_id),
        )

    if not resumed:
        # Route A, step 1: commit the durable purging tombstone before any mutation.
        ledger.start_purge(run_id, reason, _now(), manifest)

    # Route A, step 2: re-validate and delete internal-report files one by one.
    plans = _evaluate_all(ledger, run_id, manifest)
    for plan in plans:
        if plan.outcome == WOULD_DELETE:
            plan.outcome = _delete_file(plan.path)

    # Route A, steps 3+4: cascade-delete raw-count rows and mark the tombstone
    # purged in one transaction, so a crash can leave neither half-applied.
    result = ledger.finalize_purge(run_id, _now(), _summarize_files(plans))
    return ForgetOutcome(
        run_id=run_id,
        reason_code=reason,
        state="purged",
        resumed=resumed,
        files=plans,
        db_rows=result["db_rows"],
        result=result,
    )
