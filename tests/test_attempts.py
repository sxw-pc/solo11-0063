"""Immutable verify-attempt audit: lifecycle, snapshot binding, fault injection.

These tests deterministically reproduce the two partial-state break points that
existed before attempts were introduced:

* ``Ledger.update_status`` raising after the new failure checks were committed
* ``Ledger.add_checks`` raising after the previous evidence had been deleted

With attempts, an interrupted sealing transaction must either roll back as a
whole (leaving the last completed attempt and ``runs.status`` consistent) or
write a complete new attempt — never a torn mixture.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from cellcloak import ledger as ledger_module
from cellcloak import pipeline as pipeline_module
from cellcloak.diffing import compare_runs
from cellcloak.errors import VerifySnapshotError
from cellcloak.ledger import Ledger
from cellcloak.pipeline import execute_run, reverify_run

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "fixtures/demo/contract.yml"
BLOCKED = ROOT / "fixtures/blocked/contract.yml"
TAMPERED_CELL = (
    "UPDATE cells SET suppression_type = 'none' "
    "WHERE run_id = ? AND table_id = 'region_by_age' "
    "AND row_key = 'R01' AND col_key = '65+'"
)
REPAIR_CELL = (
    "UPDATE cells SET suppression_type = 'primary' "
    "WHERE run_id = ? AND table_id = 'region_by_age' "
    "AND row_key = 'R01' AND col_key = '65+'"
)


def _demo(ledger: Ledger, out: Path):
    return execute_run(DEMO, out, ledger)


def _attempts(ledger: Ledger, run_id: str) -> list[dict]:
    return ledger.get_run_detail(run_id)["attempts"]


def _status(db: Path, run_id: str) -> str:
    with sqlite3.connect(db) as conn:
        return conn.execute("SELECT status FROM runs WHERE id = ?", (run_id,)).fetchone()[0]


def _reverify_check_count(db: Path, run_id: str) -> int:
    with sqlite3.connect(db) as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM checks WHERE run_id = ? AND source = 'reverify'", (run_id,)
        ).fetchone()[0]


# --------------------------------------------------------------------- happy path


def test_each_verify_creates_a_new_attempt_with_snapshot_and_checks(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    outcome = _demo(ledger, tmp_path / "out")

    initial = _attempts(ledger, outcome.run_id)
    assert len(initial) == 1
    assert initial[0]["source"] == "run"
    assert initial[0]["status"] == "completed"
    assert initial[0]["result"] == "pass"
    assert len(initial[0]["snapshot_sha"]) == 64
    assert initial[0]["started_at"] and initial[0]["finished_at"]
    assert all(check["attempt_pk"] == initial[0]["id"] for check in initial[0]["checks"])

    result = reverify_run(ledger, outcome.run_id)
    assert result.attempt_status == "completed"
    assert result.attempt_id
    assert len(result.snapshot_sha) == 64

    attempts = _attempts(ledger, outcome.run_id)
    assert [a["sequence_no"] for a in attempts] == [1, 2]
    assert attempts[1]["source"] == "reverify"
    assert attempts[1]["status"] == "completed"
    assert attempts[1]["result"] == "pass"
    assert attempts[1]["snapshot_sha"] == attempts[0]["snapshot_sha"]
    # Checks belong to their own attempt; the original evidence is untouched.
    assert {c["id"] for c in attempts[0]["checks"]}.isdisjoint(
        {c["id"] for c in attempts[1]["checks"]}
    )
    assert len(attempts[1]["checks"]) == len(attempts[0]["checks"])


def test_fail_then_repair_then_pass_keeps_full_timed_history(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    outcome = _demo(ledger, tmp_path / "out")
    run_id = outcome.run_id

    with sqlite3.connect(db) as conn:
        conn.execute(TAMPERED_CELL, (run_id,))
    failed = reverify_run(ledger, run_id)
    assert not failed.passed
    assert failed.status == "verify_failed"
    assert _status(db, run_id) == "verify_failed"

    with sqlite3.connect(db) as conn:
        conn.execute(REPAIR_CELL, (run_id,))
    repaired = reverify_run(ledger, run_id)
    assert repaired.passed
    assert repaired.status == "ok"
    assert _status(db, run_id) == "ok"

    attempts = _attempts(ledger, run_id)
    assert [(a["source"], a["status"], a["result"]) for a in attempts] == [
        ("run", "completed", "pass"),
        ("reverify", "completed", "fail"),
        ("reverify", "completed", "pass"),
    ]
    # The failed attempt's evidence remains auditable and time-stamped.
    failed_attempt = attempts[1]
    assert failed_attempt["finished_at"] >= failed_attempt["started_at"]
    assert any(check["result"] == "fail" for check in failed_attempt["checks"])
    assert attempts[2]["attempt_id"] != failed_attempt["attempt_id"]


def test_blocked_run_stays_blocked_on_failing_attempts(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    blocked = execute_run(BLOCKED, tmp_path / "blocked", ledger)

    result = reverify_run(ledger, blocked.run_id)
    assert not result.passed
    assert result.status == "blocked"
    assert _status(db, blocked.run_id) == "blocked"
    latest = _attempts(ledger, blocked.run_id)[-1]
    assert latest["status"] == "completed" and latest["result"] == "fail"

    # Repeated verification keeps the blocked projection and appends history.
    again = reverify_run(ledger, blocked.run_id)
    assert again.status == "blocked"
    assert ledger.get_run_detail(blocked.run_id)["run"]["status"] == "blocked"
    assert [a["result"] for a in _attempts(ledger, blocked.run_id)] == ["fail", "fail", "fail"]


# ------------------------------------------------------------- fault injection


def test_update_status_break_rolls_back_checks_and_keeps_old_status(tmp_path, monkeypatch):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    outcome = _demo(ledger, tmp_path / "out")
    run_id = outcome.run_id
    # A previous, healthy reverify is the last completed evidence.
    reverify_run(ledger, run_id)
    with sqlite3.connect(db) as conn:
        conn.execute(TAMPERED_CELL, (run_id,))

    def boom(self, run_id, status, *, conn=None):
        raise RuntimeError("injected interruption")

    monkeypatch.setattr(ledger_module.Ledger, "update_status", boom)
    with pytest.raises(RuntimeError, match="injected interruption"):
        reverify_run(ledger, run_id)

    assert _status(db, run_id) == "ok"
    assert _reverify_check_count(db, run_id) == 14  # only the pre-break attempt's checks
    attempts = _attempts(ledger, run_id)
    assert attempts[-1]["status"] == "interrupted"
    assert attempts[-1]["result"] is None
    assert attempts[-1]["finished_at"] is None
    assert attempts[-1]["checks"] == []
    # The previous completed attempt and the run attempt stay intact.
    assert [a["status"] for a in attempts[:-1]] == ["completed", "completed"]


def test_add_checks_break_removes_no_old_evidence_and_marks_interrupted(tmp_path, monkeypatch):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    outcome = _demo(ledger, tmp_path / "out")
    run_id = outcome.run_id
    reverify_run(ledger, run_id)
    with sqlite3.connect(db) as conn:
        conn.execute(TAMPERED_CELL, (run_id,))

    def boom(self, run_id, checks, source="reverify", *, conn=None, attempt_pk=None):
        raise RuntimeError("injected interruption")

    monkeypatch.setattr(ledger_module.Ledger, "add_checks", boom)
    with pytest.raises(RuntimeError, match="injected interruption"):
        reverify_run(ledger, run_id)

    # Old evidence survives and no half-written new checks exist.
    assert _reverify_check_count(db, run_id) == 14
    assert _status(db, run_id) == "ok"
    attempts = _attempts(ledger, run_id)
    assert attempts[-1]["status"] == "interrupted"
    assert attempts[-2]["status"] == "completed"


def test_interrupted_attempt_does_not_block_a_fresh_successful_attempt(tmp_path, monkeypatch):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    outcome = _demo(ledger, tmp_path / "out")
    run_id = outcome.run_id

    def boom(self, run_id, status, *, conn=None):
        raise RuntimeError("injected interruption")

    monkeypatch.setattr(ledger_module.Ledger, "update_status", boom)
    with pytest.raises(RuntimeError):
        reverify_run(ledger, run_id)
    monkeypatch.undo()

    retry = reverify_run(ledger, run_id)
    assert retry.passed and retry.status == "ok"
    statuses = [a["status"] for a in _attempts(ledger, run_id)]
    assert statuses == ["completed", "interrupted", "completed"]


# ----------------------------------------------------------------- snapshot sha


def test_snapshot_conflict_seals_stale_without_status_change(tmp_path, monkeypatch):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    outcome = _demo(ledger, tmp_path / "out")
    run_id = outcome.run_id
    original_verify = pipeline_module.verify_table

    def mutate_during_computation(model, suppressed, **kwargs):
        with sqlite3.connect(db) as conn:
            conn.execute(TAMPERED_CELL, (run_id,))
        return original_verify(model, suppressed, **kwargs)

    monkeypatch.setattr(pipeline_module, "verify_table", mutate_during_computation)
    with pytest.raises(VerifySnapshotError) as info:
        reverify_run(ledger, run_id)
    assert info.value.code == "E_VERIFY_SNAPSHOT_CONFLICT"
    assert isinstance(info.value.attempt_id, int)

    assert _status(db, run_id) == "ok"
    stale = _attempts(ledger, run_id)[-1]
    assert stale["status"] == "stale"
    assert stale["result"] is None
    assert stale["checks"] == []
    assert stale["detail"]["actual_snapshot_sha"] != stale["snapshot_sha"]

    # Retrying on the now-modified matrix produces a fresh, sealed attempt.
    monkeypatch.undo()
    retry = reverify_run(ledger, run_id)
    assert retry.attempt_status == "completed"
    assert retry.status == "verify_failed"
    history = [a["status"] for a in _attempts(ledger, run_id)]
    assert history == ["completed", "stale", "completed"]


def test_sealing_an_attempt_with_zero_checks_still_commits_atomically(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    outcome = _demo(ledger, tmp_path / "out")
    run_id = outcome.run_id
    state = ledger.begin_verification(run_id)
    sealed = ledger.complete_verification(
        run_id,
        state["attempt_pk"],
        state["attempt_id"],
        state["snapshot_sha"],
        [],
        True,
        "ok",
        [],
    )
    assert sealed["stale"] is False
    detail = ledger.get_run_detail(run_id)
    last = detail["attempts"][-1]
    assert last["status"] == "completed"
    assert last["checks"] == []
    assert detail["run"]["status"] == "ok"


def test_snapshot_is_deterministic_and_excludes_run_status(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    outcome = _demo(ledger, tmp_path / "out")
    run_id = outcome.run_id
    before = ledger.load_for_verification(run_id)["snapshot_sha"]
    ledger.update_status(run_id, "verify_failed")
    after = ledger.load_for_verification(run_id)["snapshot_sha"]
    assert before == after

    with sqlite3.connect(db) as conn:
        conn.execute(TAMPERED_CELL, (run_id,))
    changed = ledger.load_for_verification(run_id)["snapshot_sha"]
    assert changed != before


# ------------------------------------------------------------- legacy migration


def _build_pre_attempt_database(db: Path) -> tuple[str, list[tuple]]:
    """Create a database in the historical, pre-attempt schema shape."""

    ledger = Ledger(db)
    outcome = execute_run(DEMO, db.parent / "out", ledger)
    reverify_run(ledger, outcome.run_id)
    run_id = outcome.run_id
    del ledger
    with sqlite3.connect(db) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        rows = conn.execute(
            "SELECT run_id, table_id, name, result, evidence_json, source FROM checks"
        ).fetchall()
        conn.execute("DROP TABLE checks")
        conn.execute("DROP TABLE verify_attempts")
        conn.execute(
            """CREATE TABLE checks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                table_id TEXT,
                name TEXT NOT NULL,
                result TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                source TEXT NOT NULL
            )"""
        )
        conn.executemany(
            "INSERT INTO checks (run_id, table_id, name, result, evidence_json, source) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.execute("PRAGMA user_version = 0")
    return run_id, rows


def test_legacy_database_migrates_old_checks_into_completed_attempts(tmp_path):
    db = tmp_path / "legacy.sqlite3"
    run_id, legacy_rows = _build_pre_attempt_database(db)

    ledger = Ledger(db)  # triggers migration on open
    detail = ledger.get_run_detail(run_id)
    attempts = detail["attempts"]
    assert [(a["source"], a["status"], a["result"]) for a in attempts] == [
        ("run", "completed", "pass"),
        ("legacy", "completed", "pass"),
    ]
    assert all(len(a["snapshot_sha"]) == 64 for a in attempts)
    assert sum(len(a["checks"]) for a in attempts) == len(legacy_rows)
    # Every check is now owned by an attempt and none were dropped.
    assert all(check["attempt_pk"] is not None for check in detail["checks"])
    assert len(detail["checks"]) == len(legacy_rows)
    assert attempts[1]["detail"]["migrated_from_source"] == "reverify"

    # Migration is idempotent and the migrated ledger supports new attempts.
    Ledger(db)
    again = reverify_run(Ledger(db), run_id)
    assert again.attempt_status == "completed"
    sources = [a["source"] for a in _attempts(ledger, run_id)]
    assert sources == ["run", "legacy", "reverify"]


def test_legacy_migration_derives_fail_result_from_failing_checks(tmp_path):
    db = tmp_path / "legacy-blocked.sqlite3"
    ledger = Ledger(db)
    blocked = execute_run(BLOCKED, db.parent / "blocked", ledger)
    run_id = blocked.run_id
    reverify_run(ledger, run_id)
    del ledger
    with sqlite3.connect(db) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        rows = conn.execute(
            "SELECT run_id, table_id, name, result, evidence_json, source FROM checks"
        ).fetchall()
        assert any(row[3] == "fail" for row in rows)
        conn.execute("DROP TABLE checks")
        conn.execute("DROP TABLE verify_attempts")
        conn.execute(
            """CREATE TABLE checks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL, table_id TEXT, name TEXT NOT NULL,
                result TEXT NOT NULL, evidence_json TEXT NOT NULL, source TEXT NOT NULL
            )"""
        )
        conn.executemany(
            "INSERT INTO checks (run_id, table_id, name, result, evidence_json, source) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.execute("PRAGMA user_version = 0")

    migrated = Ledger(db)
    attempts = migrated.get_run_detail(run_id)["attempts"]
    assert all(a["result"] == "fail" for a in attempts)
    assert all(a["status"] == "completed" for a in attempts)


def test_legacy_run_only_checks_migrate_without_a_reverify_group(tmp_path):
    db = tmp_path / "legacy-run-only.sqlite3"
    ledger = Ledger(db)
    outcome = execute_run(DEMO, db.parent / "out", ledger)
    run_id = outcome.run_id
    del ledger
    with sqlite3.connect(db) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        rows = conn.execute(
            "SELECT run_id, table_id, name, result, evidence_json, source FROM checks"
        ).fetchall()
        conn.execute("DROP TABLE checks")
        conn.execute("DROP TABLE verify_attempts")
        conn.execute(
            """CREATE TABLE checks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL, table_id TEXT, name TEXT NOT NULL,
                result TEXT NOT NULL, evidence_json TEXT NOT NULL, source TEXT NOT NULL
            )"""
        )
        conn.executemany(
            "INSERT INTO checks (run_id, table_id, name, result, evidence_json, source) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.execute("PRAGMA user_version = 0")

    migrated = Ledger(db)
    attempts = migrated.get_run_detail(run_id)["attempts"]
    assert len(attempts) == 1
    assert attempts[0]["source"] == "run"
    assert len(attempts[0]["checks"]) == len(rows)


# ------------------------------------------------------------- diffing contract


def test_diff_ignores_attempt_count_and_only_compares_current_facts(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    run_a = execute_run(DEMO, tmp_path / "a", ledger)
    run_b = execute_run(DEMO, tmp_path / "b", ledger)
    baseline = compare_runs(ledger, run_a.run_id, run_b.run_id)

    reverify_run(ledger, run_a.run_id)
    reverify_run(ledger, run_a.run_id)
    # A failing attempt changes run A's status, which IS a current fact.
    with sqlite3.connect(db) as conn:
        conn.execute(TAMPERED_CELL, (run_a.run_id,))
    reverify_run(ledger, run_a.run_id)

    after = compare_runs(ledger, run_a.run_id, run_b.run_id)
    assert after["summary"]["status_changed"] is True
    # Table-level data and public artifacts remain identical across the runs;
    # the extra attempts themselves contribute no table differences.
    assert all(not table["changed"] for table in after["tables"])
    # Re-running reverify without further data changes is a stable comparison.
    with sqlite3.connect(db) as conn:
        conn.execute(REPAIR_CELL, (run_a.run_id,))
    reverify_run(ledger, run_a.run_id)
    repaired = compare_runs(ledger, run_a.run_id, run_b.run_id)
    assert repaired == baseline
