"""Purge lifecycle tests: file safety, crash recovery, tombstone behavior."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from cellcloak.diffing import compare_runs
from cellcloak.errors import CellcloakError, LedgerError
from cellcloak.forget import forget_run
from cellcloak.ledger import Ledger
from cellcloak.pipeline import execute_run, reverify_run
from cellcloak.report import MARKER_NAME

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "fixtures" / "demo" / "contract.yml"
REASON = "retention-2026"


def _run(ledger: Ledger, out: Path):
    return execute_run(DEMO, out, ledger)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _audit_paths(out: Path) -> dict[str, Path]:
    return {
        "audit_json": out / "audit" / "report.json",
        "audit_md": out / "audit" / "report.md",
        "audit_html": out / "audit" / "report.html",
    }


def _db_counts(db: Path, run_id: str) -> dict[str, int]:
    with sqlite3.connect(db) as conn:
        counts = {
            table: conn.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = ?", (run_id,)).fetchone()[0]
            for table in ("tables", "cells", "checks", "artifacts")
        }
        counts["runs"] = conn.execute("SELECT COUNT(*) FROM runs WHERE id = ?", (run_id,)).fetchone()[0]
        return counts


def _tombstone_row(db: Path, run_id: str):
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute("SELECT * FROM purge_tombstones WHERE run_id = ?", (run_id,)).fetchone()


def test_purge_deletes_raw_records_and_internal_reports(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    out = tmp_path / "out"
    outcome = _run(ledger, out)
    run_id = outcome.run_id
    audit = _audit_paths(out)

    result = forget_run(ledger, run_id, REASON)

    assert result.state == "purged"
    assert result.result["outcome"] == "purged"
    assert {plan.outcome for plan in result.files} == {"deleted"}
    for path in audit.values():
        assert not path.exists()
    # Public tables carry no raw counts and are not part of the purge.
    assert (out / "public" / "region_by_age.csv").is_file()
    assert (out / MARKER_NAME).is_file()

    counts = _db_counts(db, run_id)
    assert counts == {"runs": 0, "tables": 0, "cells": 0, "checks": 0, "artifacts": 0}
    assert result.db_rows["cells"] > 0

    tombstone = ledger.get_tombstone(run_id)
    assert tombstone["state"] == "purged"
    assert tombstone["reason_code"] == REASON
    assert tombstone["finished_at"]
    # Minimal tombstone: no manifest, no paths, no coordinates, no raw counts.
    assert tombstone["manifest"] is None
    row = _tombstone_row(db, run_id)
    assert row["manifest_json"] is None
    persisted = row["result_json"]
    assert str(tmp_path) not in persisted
    assert "report.json" not in persisted
    assert "raw_count" not in persisted
    assert "R01" not in persisted


def test_dry_run_changes_nothing(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    out = tmp_path / "out"
    outcome = _run(ledger, out)
    run_id = outcome.run_id
    audit = _audit_paths(out)
    hashes_before = {kind: _sha(path) for kind, path in audit.items()}
    counts_before = _db_counts(db, run_id)

    plan = forget_run(ledger, run_id, REASON, dry_run=True)

    assert plan.state == "dry_run"
    assert {plan_item.outcome for plan_item in plan.files} == {"would_delete"}
    assert len(plan.files) == 3
    assert plan.db_rows["cells"] == counts_before["cells"]
    assert ledger.get_tombstone(run_id) is None
    assert _db_counts(db, run_id) == counts_before
    assert hashes_before == {kind: _sha(path) for kind, path in audit.items()}


def test_shared_output_dir_keeps_files_owned_by_later_run(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    out = tmp_path / "shared"
    first = _run(ledger, out)
    second = _run(ledger, out)  # same dir: rewrites public+audit and the marker
    assert first.run_id != second.run_id
    audit = _audit_paths(out)

    result = forget_run(ledger, first.run_id, REASON)

    assert result.state == "purged"
    assert result.result["outcome"] == "purged_with_kept_files"
    # The later retained run still references these exact paths.
    assert {plan.outcome for plan in result.files} == {"kept_shared_reference"}
    for path in audit.values():
        assert path.is_file()
    # The surviving run is untouched and still verifiable.
    assert _db_counts(db, second.run_id)["cells"] > 0
    assert reverify_run(ledger, second.run_id).passed

    # Once the older run's artifact rows are gone, the newer run purges cleanly.
    second_result = forget_run(ledger, second.run_id, REASON)
    assert second_result.result["outcome"] == "purged"
    assert all(not path.exists() for path in audit.values())


def test_purging_newer_run_first_keeps_shared_files(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    out = tmp_path / "shared"
    first = _run(ledger, out)
    second = _run(ledger, out)
    audit = _audit_paths(out)

    # The newer run owns the on-disk content, but the retained older run still
    # references the same paths, so nothing may be unlinked yet.
    result = forget_run(ledger, second.run_id, REASON)
    assert {plan.outcome for plan in result.files} == {"kept_shared_reference"}
    assert all(path.is_file() for path in audit.values())

    # Purging the older run afterwards hits the hash mismatch (content is newer).
    older = forget_run(ledger, first.run_id, REASON)
    assert {plan.outcome for plan in older.files} == {"kept_hash_mismatch"}
    # The newer run's tombstone is already final and path-free by design, so a
    # repeat is an idempotent no-op that keeps reporting the kept files; the
    # conservative keeps are surfaced for manual follow-up, never retried.
    repeat = forget_run(ledger, second.run_id, REASON)
    assert repeat.state == "already_purged"
    assert repeat.result["outcome"] == "purged_with_kept_files"
    assert repeat.result["kinds"] == {
        "audit_html": "kept_shared_reference",
        "audit_json": "kept_shared_reference",
        "audit_md": "kept_shared_reference",
    }
    assert all(path.is_file() for path in audit.values())


def test_dry_run_on_purging_run_changes_nothing(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    out = tmp_path / "out"
    outcome = _run(ledger, out)
    manifest = {"output_dir": str(out), "files": ledger.purge_candidates(outcome.run_id)}
    ledger.start_purge(outcome.run_id, REASON, "2026-11-15T00:00:00Z", manifest)

    plan = forget_run(ledger, outcome.run_id, REASON, dry_run=True)

    assert plan.state == "dry_run"
    assert plan.resumed is True
    assert {item.outcome for item in plan.files} == {"would_delete"}
    # Still purging afterwards; nothing deleted, rows intact.
    assert ledger.get_tombstone(outcome.run_id)["state"] == "purging"
    assert all(path.is_file() for path in _audit_paths(out).values())
    assert _db_counts(db, outcome.run_id)["cells"] > 0


def test_external_rewrite_keeps_file_on_hash_mismatch(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    out = tmp_path / "out"
    outcome = _run(ledger, out)
    audit = _audit_paths(out)
    audit["audit_json"].write_text('{"tampered": true}\n', encoding="utf-8")

    result = forget_run(ledger, outcome.run_id, REASON)

    outcomes = {plan.kind: plan.outcome for plan in result.files}
    assert outcomes["audit_json"] == "kept_hash_mismatch"
    assert outcomes["audit_md"] == "deleted"
    assert outcomes["audit_html"] == "deleted"
    assert audit["audit_json"].read_text(encoding="utf-8") == '{"tampered": true}\n'
    assert result.result["outcome"] == "purged_with_kept_files"
    assert _db_counts(db, outcome.run_id)["cells"] == 0


def test_corrupt_or_missing_marker_keeps_files(tmp_path):
    for vandal in ("corrupt", "missing", "foreign"):
        db = tmp_path / f"ledger-{vandal}.sqlite3"
        ledger = Ledger(db)
        out = tmp_path / f"out-{vandal}"
        outcome = _run(ledger, out)
        marker = out / MARKER_NAME
        if vandal == "corrupt":
            marker.write_text("{ not json", encoding="utf-8")
        elif vandal == "foreign":
            # Valid JSON, but not a Cellcloak output manifest.
            marker.write_text('{"tool": "other", "files": []}\n', encoding="utf-8")
        else:
            marker.unlink()
        audit = _audit_paths(out)

        result = forget_run(ledger, outcome.run_id, REASON)

        assert {plan.outcome for plan in result.files} == {"kept_marker_invalid"}
        assert all(path.is_file() for path in audit.values())
        # Raw counts are still purged; only file deletion is refused.
        assert _db_counts(db, outcome.run_id)["cells"] == 0
        assert ledger.get_tombstone(outcome.run_id)["state"] == "purged"


def test_file_dropped_from_manifest_is_kept(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    out = tmp_path / "out"
    outcome = _run(ledger, out)
    marker = out / MARKER_NAME
    payload = json.loads(marker.read_text(encoding="utf-8"))
    payload["artifacts"] = [entry for entry in payload["artifacts"] if entry != "audit/report.md"]
    marker.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    result = forget_run(ledger, outcome.run_id, REASON)

    outcomes = {plan.kind: plan.outcome for plan in result.files}
    assert outcomes["audit_md"] == "kept_not_in_manifest"
    assert outcomes["audit_json"] == "deleted"
    assert (out / "audit" / "report.md").is_file()


def test_ledger_path_escape_is_refused(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    out = tmp_path / "out"
    outcome = _run(ledger, out)
    outside = tmp_path / "outside" / "report.json"
    outside.parent.mkdir()
    outside.write_text("secret\n", encoding="utf-8")
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO artifacts (run_id, path, kind, sha256) VALUES (?, ?, 'audit_json', ?)",
            (outcome.run_id, str(outside), _sha(outside)),
        )

    result = forget_run(ledger, outcome.run_id, REASON)

    escaped = next(plan for plan in result.files if plan.path == str(outside))
    assert escaped.outcome == "kept_path_escape"
    assert outside.read_text(encoding="utf-8") == "secret\n"


def test_symlink_candidate_is_never_followed(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    out = tmp_path / "out"
    outcome = _run(ledger, out)
    target = tmp_path / "target.txt"
    target.write_text("do not delete\n", encoding="utf-8")
    link = out / "audit" / "report.json"
    link.unlink()
    link.symlink_to(target)

    result = forget_run(ledger, outcome.run_id, REASON)

    outcomes = {plan.kind: plan.outcome for plan in result.files}
    assert outcomes["audit_json"] == "kept_symlink"
    assert link.is_symlink()
    assert target.read_text(encoding="utf-8") == "do not delete\n"


def test_already_absent_files_do_not_block_purge(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    out = tmp_path / "out"
    outcome = _run(ledger, out)
    (out / "audit" / "report.md").unlink()

    result = forget_run(ledger, outcome.run_id, REASON)

    outcomes = {plan.kind: plan.outcome for plan in result.files}
    assert outcomes["audit_md"] == "already_absent"
    assert outcomes["audit_json"] == "deleted"
    assert result.result["outcome"] == "purged"


def test_resume_after_crash_before_file_deletion(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    out = tmp_path / "out"
    outcome = _run(ledger, out)
    manifest = {"output_dir": str(out), "files": ledger.purge_candidates(outcome.run_id)}
    ledger.start_purge(outcome.run_id, REASON, "2026-11-15T00:00:00Z", manifest)

    # While purging, verify refuses to continue from incomplete state.
    verification = reverify_run(ledger, outcome.run_id)
    assert not verification.passed
    assert verification.status == "purging"
    with pytest.raises(LedgerError) as info:
        ledger.load_for_verification(outcome.run_id)
    assert info.value.code == "E_RUN_PURGING"

    resumed = forget_run(ledger, outcome.run_id, REASON)
    assert resumed.state == "purged"
    assert resumed.resumed is True
    assert {plan.outcome for plan in resumed.files} == {"deleted"}
    assert ledger.get_tombstone(outcome.run_id)["state"] == "purged"
    assert reverify_run(ledger, outcome.run_id).status == "purged"


def test_resume_after_crash_mid_file_loop(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    out = tmp_path / "out"
    outcome = _run(ledger, out)
    manifest = {"output_dir": str(out), "files": ledger.purge_candidates(outcome.run_id)}
    ledger.start_purge(outcome.run_id, REASON, "2026-11-15T00:00:00Z", manifest)
    # Crash after exactly one file was deleted.
    (out / "audit" / "report.html").unlink()

    resumed = forget_run(ledger, outcome.run_id, REASON)

    outcomes = {plan.kind: plan.outcome for plan in resumed.files}
    assert outcomes["audit_html"] == "already_absent"
    assert outcomes["audit_json"] == "deleted"
    assert outcomes["audit_md"] == "deleted"
    assert _db_counts(db, outcome.run_id)["cells"] == 0
    assert ledger.get_tombstone(outcome.run_id)["state"] == "purged"


def test_resume_after_crash_between_files_and_db_delete(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    out = tmp_path / "out"
    outcome = _run(ledger, out)
    manifest = {"output_dir": str(out), "files": ledger.purge_candidates(outcome.run_id)}
    ledger.start_purge(outcome.run_id, REASON, "2026-11-15T00:00:00Z", manifest)
    for path in _audit_paths(out).values():
        path.unlink()
    # Crash before the cascade delete: raw rows are still there.
    assert _db_counts(db, outcome.run_id)["cells"] > 0

    resumed = forget_run(ledger, outcome.run_id, REASON)

    assert resumed.state == "purged"
    assert {plan.outcome for plan in resumed.files} == {"already_absent"}
    assert _db_counts(db, outcome.run_id) == {"runs": 0, "tables": 0, "cells": 0, "checks": 0, "artifacts": 0}
    assert ledger.get_tombstone(outcome.run_id)["state"] == "purged"


def test_resume_after_crash_inside_finalize(tmp_path, monkeypatch):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    out = tmp_path / "out"
    outcome = _run(ledger, out)

    real_finalize = Ledger.finalize_purge

    def exploding_finalize(self, run_id, finished_at, files_result):
        raise RuntimeError("simulated crash")

    monkeypatch.setattr(Ledger, "finalize_purge", exploding_finalize)
    with pytest.raises(RuntimeError):
        forget_run(ledger, outcome.run_id, REASON)
    monkeypatch.setattr(Ledger, "finalize_purge", real_finalize)

    # Files are gone, rows remain, tombstone says purging; verify is explicit.
    assert all(not path.exists() for path in _audit_paths(out).values())
    assert _db_counts(db, outcome.run_id)["cells"] > 0
    assert ledger.get_tombstone(outcome.run_id)["state"] == "purging"
    assert reverify_run(ledger, outcome.run_id).status == "purging"

    resumed = forget_run(ledger, outcome.run_id, REASON)
    assert resumed.state == "purged"
    assert _db_counts(db, outcome.run_id)["cells"] == 0
    assert ledger.get_tombstone(outcome.run_id)["state"] == "purged"


def test_repeated_forget_is_idempotent(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    out = tmp_path / "out"
    outcome = _run(ledger, out)
    first = forget_run(ledger, outcome.run_id, REASON)
    finished_at = ledger.get_tombstone(outcome.run_id)["finished_at"]

    second = forget_run(ledger, outcome.run_id, REASON)

    assert second.state == "already_purged"
    assert second.result == first.result
    assert ledger.get_tombstone(outcome.run_id)["finished_at"] == finished_at


def test_forget_missing_run_raises_not_found(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    with pytest.raises(LedgerError) as info:
        forget_run(ledger, "run-missing", REASON)
    assert info.value.code == "E_RUN_NOT_FOUND"


def test_reason_code_validation(tmp_path):
    from cellcloak.forget import validate_reason_code

    ledger = Ledger(tmp_path / "ledger.sqlite3")
    _run(ledger, tmp_path / "out")
    for bad in ("", "  ", "has space", "中文", "-leading-dash", "x" * 65, "semi;colon"):
        with pytest.raises(CellcloakError) as info:
            forget_run(ledger, "run-whatever", bad)
        assert info.value.code == "E_FORGET_REASON"
    for good in ("retention-2026", "GDPR.Art.17", "policy:v2", "Q4_cleanup"):
        assert validate_reason_code(good) == good


def test_old_database_is_migrated_on_open(tmp_path):
    # Simulate a pre-tombstone ledger: only the original table shape.
    db = tmp_path / "old.sqlite3"
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE runs (id TEXT PRIMARY KEY, dataset_name TEXT NOT NULL,
                status TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT NOT NULL,
                contract_hash TEXT NOT NULL, contract_path TEXT NOT NULL,
                output_dir TEXT NOT NULL, summary_json TEXT NOT NULL);
            CREATE TABLE artifacts (id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                path TEXT NOT NULL, kind TEXT NOT NULL, sha256 TEXT NOT NULL);
            """
        )
    ledger = Ledger(db)
    assert ledger.get_tombstone("run-anything") is None
    with sqlite3.connect(db) as conn:
        names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert "purge_tombstones" in names


def test_runs_detail_and_verify_reflect_purged_state(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    out = tmp_path / "out"
    keep = _run(ledger, tmp_path / "keep")
    gone = _run(ledger, out)
    forget_run(ledger, gone.run_id, REASON)

    runs = ledger.list_runs()
    assert len(runs) == 2
    purged = next(run for run in runs if run["id"] == gone.run_id)
    assert purged["status"] == "purged"
    assert purged["tombstone"]["reason_code"] == REASON
    retained = next(run for run in runs if run["id"] == keep.run_id)
    assert retained["status"] == "ok"
    assert retained["tombstone"] is None

    detail = ledger.get_run_detail(gone.run_id)
    assert detail["run"]["status"] == "purged"
    assert detail["tables"] == [] and detail["checks"] == [] and detail["artifacts"] == []
    assert detail["tombstone"]["state"] == "purged"

    verification = reverify_run(ledger, gone.run_id)
    assert not verification.passed
    assert verification.status == "purged"
    with pytest.raises(LedgerError) as info:
        ledger.load_for_verification(gone.run_id)
    assert info.value.code == "E_RUN_PURGED"


def test_diff_refuses_purged_run(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    keep = _run(ledger, tmp_path / "keep")
    gone = _run(ledger, tmp_path / "gone")
    forget_run(ledger, gone.run_id, REASON)

    with pytest.raises(LedgerError) as info:
        compare_runs(ledger, keep.run_id, gone.run_id)
    assert info.value.code == "E_RUN_PURGED"


def test_purging_run_appears_in_listing(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    out = tmp_path / "out"
    outcome = _run(ledger, out)
    manifest = {"output_dir": str(out), "files": ledger.purge_candidates(outcome.run_id)}
    ledger.start_purge(outcome.run_id, REASON, "2026-11-15T00:00:00Z", manifest)

    runs = ledger.list_runs()
    assert runs[0]["id"] == outcome.run_id
    assert runs[0]["status"] == "purging"
    assert runs[0]["tombstone"]["state"] == "purging"
    detail = ledger.get_run_detail(outcome.run_id)
    assert detail["run"]["status"] == "purging"
    assert detail["tombstone"]["manifest"]["files"]
