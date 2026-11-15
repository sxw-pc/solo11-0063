"""Chunked recording, sealing and crash-recovery tests for the ledger."""

import json
import sqlite3
import threading
import tracemalloc
from pathlib import Path

import pytest

from cellcloak.cli import main as cli_main
from cellcloak.errors import LedgerError
from cellcloak.ledger import (
    Ledger,
    cell_stream_expectation,
    genesis_digest,
)
from cellcloak.model import cell_fingerprint
from cellcloak.pipeline import execute_run, reverify_run, resume_run

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "fixtures" / "demo" / "contract.yml"

# Pre-chunking (schema v0) layout, used to prove old databases migrate.
OLD_SCHEMA = """
CREATE TABLE runs (
    id TEXT PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    contract_hash TEXT NOT NULL,
    contract_path TEXT NOT NULL,
    output_dir TEXT NOT NULL,
    summary_json TEXT NOT NULL
);
CREATE TABLE tables (
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    table_id TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    col_count INTEGER NOT NULL,
    cell_count INTEGER NOT NULL,
    primary_count INTEGER NOT NULL,
    secondary_count INTEGER NOT NULL,
    suppression_ratio REAL NOT NULL,
    table_status TEXT NOT NULL,
    block_reasons_json TEXT NOT NULL,
    verify_passed INTEGER NOT NULL,
    meta_json TEXT NOT NULL,
    PRIMARY KEY (run_id, table_id)
);
CREATE TABLE cells (
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    table_id TEXT NOT NULL,
    row_key TEXT NOT NULL,
    col_key TEXT NOT NULL,
    raw_count INTEGER NOT NULL,
    suppression_type TEXT NOT NULL,
    risk_category TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    PRIMARY KEY (run_id, table_id, row_key, col_key)
);
CREATE TABLE checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    table_id TEXT,
    name TEXT NOT NULL,
    result TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    source TEXT NOT NULL
);
CREATE TABLE artifacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    kind TEXT NOT NULL,
    sha256 TEXT NOT NULL
);
"""


def _tuples(table_id, rows, cols):
    for r in range(rows):
        for c in range(cols):
            yield (table_id, f"r{r:05d}", f"c{c:04d}", (r * 31 + c * 7) % 41, "none", "safe")


def _records(tuples):
    for table_id, row, col, count, supp, risk in tuples:
        yield {
            "table_id": table_id,
            "row_key": row,
            "col_key": col,
            "raw_count": count,
            "suppression_type": supp,
            "risk_category": risk,
            "fingerprint": cell_fingerprint(table_id, row, col),
        }


def _begin(ledger, run_id, table_id="t1", rows=4, cols=5, checks=1, artifacts=1):
    run = {
        "id": run_id,
        "dataset_name": "synthetic",
        "status": "ok",
        "started_at": "2026-01-01T00:00:00Z",
        "finished_at": "2026-01-01T00:00:01Z",
        "contract_hash": "ab" * 32,
        "contract_path": "/tmp/contract.yml",
        "output_dir": "/tmp/out",
        "summary": {"cell_count": rows * cols},
    }
    tables = [
        {
            "table_id": table_id,
            "row_count": rows,
            "col_count": cols,
            "cell_count": rows * cols,
            "primary_count": 0,
            "secondary_count": 0,
            "suppression_ratio": 0.0,
            "table_status": "ok",
            "block_reasons": [],
            "verify_passed": True,
            "meta": {},
        }
    ]
    check_rows = [
        {"table_id": table_id, "name": f"check{i}", "result": "pass", "evidence": {}, "source": "run"}
        for i in range(checks)
    ]
    artifact_rows = [
        {"path": f"/tmp/out/a{i}.json", "kind": "audit", "sha256": "cd" * 32}
        for i in range(artifacts)
    ]
    expect = cell_stream_expectation(run_id, _tuples(table_id, rows, cols), ledger.chunk_size)
    ledger.begin_run(run, tables, check_rows, artifact_rows, cell_expect=expect)
    return expect


def _counts(db_path, run_id):
    with sqlite3.connect(db_path) as conn:
        cells = conn.execute("SELECT COUNT(*) FROM cells WHERE run_id = ?", (run_id,)).fetchone()[0]
        chunks = conn.execute("SELECT COUNT(*) FROM run_chunks WHERE run_id = ?", (run_id,)).fetchone()[0]
        runs = conn.execute("SELECT COUNT(*) FROM runs WHERE id = ?", (run_id,)).fetchone()[0]
    return runs, chunks, cells


# ----------------------------------------------------------------------
# Chunk protocol
# ----------------------------------------------------------------------


def test_chunked_record_and_seal_roundtrip(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3", chunk_size=64)
    _begin(ledger, "run-a", rows=10, cols=10)  # 100 cells -> 2 chunks
    result = ledger.stream_cells("run-a", _records(_tuples("t1", 10, 10)))
    assert result == {"chunks_written": 2, "chunks_skipped": 0}
    # Unsealed: invisible to every public read.
    assert ledger.list_runs() == []
    assert ledger.get_run_detail("run-a") is None
    with pytest.raises(LedgerError) as info:
        ledger.load_for_verification("run-a")
    assert info.value.code == "E_RUN_NOT_FOUND"
    ledger.seal_run("run-a")
    assert [run["id"] for run in ledger.list_runs()] == ["run-a"]
    detail = ledger.load_for_verification("run-a")
    assert len(detail["tables"][0]["cells"]) == 100


def test_duplicate_chunk_is_idempotent_and_conflict_is_rejected(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3", chunk_size=64)
    _begin(ledger, "run-a", rows=10, cols=10)
    chunk0 = list(_records(_tuples("t1", 10, 10)))[:64]
    assert ledger.write_cell_chunk("run-a", 0, chunk0) == "written"
    # Identical retry: dropped, no double insert.
    assert ledger.write_cell_chunk("run-a", 0, chunk0) == "duplicate"
    _, chunks, cells = _counts(ledger.db_path, "run-a")
    assert (chunks, cells) == (1, 64)
    # Same sequence, different content: explicit rejection.
    tampered = [dict(cell, raw_count=cell["raw_count"] + 1) for cell in chunk0]
    with pytest.raises(LedgerError) as info:
        ledger.write_cell_chunk("run-a", 0, tampered)
    assert info.value.code == "E_LEDGER_CHUNK_CONFLICT"


def test_out_of_order_chunk_is_rejected(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3", chunk_size=64)
    _begin(ledger, "run-a", rows=10, cols=10)
    records = list(_records(_tuples("t1", 10, 10)))
    with pytest.raises(LedgerError) as info:
        ledger.write_cell_chunk("run-a", 1, records[64:])
    assert info.value.code == "E_LEDGER_CHUNK_SEQ"
    assert _counts(ledger.db_path, "run-a")[1:] == (0, 0)


def test_write_to_sealed_or_unknown_run_is_rejected(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3", chunk_size=64)
    _begin(ledger, "run-a", rows=4, cols=4)
    ledger.stream_cells("run-a", _records(_tuples("t1", 4, 4)))
    ledger.seal_run("run-a")
    with pytest.raises(LedgerError) as info:
        ledger.write_cell_chunk("run-a", 1, list(_records(_tuples("t1", 4, 4)))[:1])
    assert info.value.code == "E_RUN_SEALED"
    with pytest.raises(LedgerError) as missing:
        ledger.write_cell_chunk("run-missing", 0, list(_records(_tuples("t1", 4, 4)))[:1])
    assert missing.value.code == "E_RUN_NOT_FOUND"


def test_zero_cell_run_seals(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3", chunk_size=64)
    expect = cell_stream_expectation("run-empty", [], 64)
    assert expect == {"cell_count": 0, "digest": genesis_digest("run-empty")}
    _begin(ledger, "run-empty", rows=0, cols=0)
    ledger.seal_run("run-empty")
    assert [run["id"] for run in ledger.list_runs()] == ["run-empty"]


# ----------------------------------------------------------------------
# Crash mid-chunk / between chunks, and rollback atomicity
# ----------------------------------------------------------------------


def test_failed_chunk_rolls_back_cells_and_chunk_row(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db, chunk_size=64)
    _begin(ledger, "run-a", rows=10, cols=10)
    # Duplicate coordinate inside one chunk -> integrity error mid-executemany.
    broken = list(_records(_tuples("t1", 10, 10)))[:64]
    broken[10] = dict(broken[10], row_key=broken[0]["row_key"], col_key=broken[0]["col_key"])
    with pytest.raises(LedgerError) as info:
        ledger.write_cell_chunk("run-a", 0, broken)
    assert info.value.code == "E_LEDGER_CHUNK_CONFLICT"
    # Nothing from the failed chunk survived: no cells, no chunk row.
    assert _counts(db, "run-a")[1:] == (0, 0)
    # The run can still be completed correctly afterwards.
    ledger.stream_cells("run-a", _records(_tuples("t1", 10, 10)))
    ledger.seal_run("run-a")
    assert [run["id"] for run in ledger.list_runs()] == ["run-a"]


def test_generator_failure_mid_stream_leaves_resumable_state(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db, chunk_size=50)
    _begin(ledger, "run-a", rows=10, cols=10)  # 100 cells, chunk 50 -> 2 chunks

    def flaky():
        for index, record in enumerate(_records(_tuples("t1", 10, 10))):
            if index == 75:  # crash inside the second chunk
                raise RuntimeError("simulated crash")
            yield record

    with pytest.raises(RuntimeError):
        ledger.stream_cells("run-a", flaky())
    runs, chunks, cells = _counts(db, "run-a")
    assert (runs, chunks, cells) == (1, 1, 50)  # first chunk committed, second rolled back
    # Restart: a fresh Ledger on the same file sees the pending recording.
    restarted = Ledger(db, chunk_size=50)
    pending = restarted.pending_runs()
    assert len(pending) == 1
    assert pending[0]["run_id"] == "run-a"
    assert pending[0]["cells_committed"] == 50
    assert pending[0]["expected_cells"] == 100
    # Still invisible; resuming the deterministic stream finishes and seals.
    assert restarted.list_runs() == []
    result = restarted.stream_cells("run-a", _records(_tuples("t1", 10, 10)))
    assert result == {"chunks_written": 1, "chunks_skipped": 1}
    restarted.seal_run("run-a")
    assert [run["id"] for run in restarted.list_runs()] == ["run-a"]


def test_abort_cascades_all_staged_rows(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db, chunk_size=16)
    _begin(ledger, "run-a", rows=8, cols=8, checks=2, artifacts=2)
    ledger.stream_cells("run-a", _records(_tuples("t1", 8, 8)))
    ledger.abort_run("run-a")
    assert _counts(db, "run-a") == (0, 0, 0)
    with sqlite3.connect(db) as conn:
        for table in ("tables", "checks", "artifacts"):
            assert conn.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = 'run-a'").fetchone()[0] == 0
    assert ledger.pending_runs() == []
    with pytest.raises(LedgerError) as info:
        ledger.abort_run("run-a")
    assert info.value.code == "E_RUN_NOT_FOUND"


def test_abort_of_sealed_run_is_refused(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3", chunk_size=64)
    _begin(ledger, "run-a", rows=4, cols=4)
    ledger.stream_cells("run-a", _records(_tuples("t1", 4, 4)))
    ledger.seal_run("run-a")
    with pytest.raises(LedgerError) as info:
        ledger.abort_run("run-a")
    assert info.value.code == "E_RUN_SEALED"
    assert [run["id"] for run in ledger.list_runs()] == ["run-a"]


def test_duplicate_begin_run_rolls_back_completely(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db, chunk_size=64)
    _begin(ledger, "run-a", rows=4, cols=4)
    with pytest.raises(LedgerError) as info:
        _begin(ledger, "run-a", rows=4, cols=4)
    assert info.value.code == "E_RUN_DUPLICATE"
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM tables WHERE run_id = 'run-a'").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM checks WHERE run_id = 'run-a'").fetchone()[0] == 1


# ----------------------------------------------------------------------
# Seal verification: digest mismatch, gaps, foreign chunks
# ----------------------------------------------------------------------


def test_seal_rejects_digest_mismatch(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3", chunk_size=64)
    _begin(ledger, "run-a", rows=4, cols=4)
    # Stream a different cell population than the expectation was built from.
    foreign = _records(_tuples("t1", 4, 4))
    foreign = (dict(cell, raw_count=cell["raw_count"] + 1) for cell in foreign)
    ledger.stream_cells("run-a", foreign)
    with pytest.raises(LedgerError) as info:
        ledger.seal_run("run-a")
    assert info.value.code == "E_LEDGER_SEAL"
    assert ledger.list_runs() == []  # stays unsealed


def test_seal_rejects_missing_chunk_and_missing_cells(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db, chunk_size=50)
    _begin(ledger, "run-a", rows=10, cols=10)
    ledger.stream_cells("run-a", _records(_tuples("t1", 10, 10)))
    with sqlite3.connect(db) as conn:
        conn.execute("DELETE FROM run_chunks WHERE run_id = 'run-a' AND chunk_seq = 1")
    with pytest.raises(LedgerError) as info:
        ledger.seal_run("run-a")
    assert info.value.code == "E_LEDGER_SEAL"
    with sqlite3.connect(db) as conn:
        conn.execute("DELETE FROM cells WHERE run_id = 'run-a' AND row_key = 'r00000'")
    with pytest.raises(LedgerError) as again:
        ledger.seal_run("run-a")
    assert again.value.code == "E_LEDGER_SEAL"


def test_chunk_from_another_run_is_detected(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db, chunk_size=64)
    _begin(ledger, "run-a", table_id="t1", rows=4, cols=4)
    _begin(ledger, "run-b", table_id="t2", rows=4, cols=4)
    ledger.stream_cells("run-b", _records(_tuples("t2", 4, 4)))
    # Feeding run B's population into run A writes chunks whose chained
    # digest cannot match A's expectation: the seal refuses.
    ledger.stream_cells("run-a", _records(_tuples("t2", 4, 4)))
    with pytest.raises(LedgerError) as info:
        ledger.seal_run("run-a")
    assert info.value.code == "E_LEDGER_SEAL"
    ledger.abort_run("run-a")
    # A chunk row physically moved under another run breaks the run-bound
    # chain even when the cell population is identical.
    _begin(ledger, "run-c", table_id="t2", rows=4, cols=4)
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE run_chunks SET run_id = 'run-c' WHERE run_id = 'run-b' AND chunk_seq = 0")
    with pytest.raises(LedgerError) as moved:
        ledger.seal_run("run-c")
    assert moved.value.code == "E_LEDGER_SEAL"


def test_seal_rejects_broken_child_associations(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db, chunk_size=64)
    _begin(ledger, "run-a", rows=4, cols=4, checks=1, artifacts=1)
    ledger.stream_cells("run-a", _records(_tuples("t1", 4, 4)))
    with sqlite3.connect(db) as conn:
        conn.execute("DELETE FROM checks WHERE run_id = 'run-a'")
    with pytest.raises(LedgerError) as info:
        ledger.seal_run("run-a")
    assert info.value.code == "E_LEDGER_SEAL"


# ----------------------------------------------------------------------
# Concurrency: readers never see a recording run
# ----------------------------------------------------------------------


def test_concurrent_readers_never_see_unsealed_run(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    writer = Ledger(db, chunk_size=256)
    _begin(writer, "run-live", rows=100, cols=100)  # 10k cells -> 40 chunks
    _begin(writer, "run-sealed", rows=4, cols=4)
    writer.stream_cells("run-sealed", _records(_tuples("t1", 4, 4)))
    writer.seal_run("run-sealed")

    stop = threading.Event()
    failures: list[str] = []

    def reader() -> None:
        ledger = Ledger(db)
        while not stop.is_set():
            try:
                listed = "run-live" in [run["id"] for run in ledger.list_runs(limit=50)]
                detail = ledger.get_run_detail("run-live")
                if listed != (detail is not None):
                    # The two reads may legitimately straddle the atomic seal;
                    # only a persistent mismatch is a leak.
                    listed = "run-live" in [run["id"] for run in ledger.list_runs(limit=50)]
                    detail = ledger.get_run_detail("run-live")
                    if listed != (detail is not None):
                        failures.append("run-live visibility is inconsistent")
                        return
                try:
                    internal = ledger.load_for_verification("run-live")
                except LedgerError as exc:
                    if exc.code != "E_RUN_NOT_FOUND":
                        failures.append(f"unexpected error code {exc.code}")
                        return
                    if listed or detail is not None:
                        failures.append("run-live listed but not loadable")
                        return
                else:
                    # Visible is only acceptable fully sealed and complete:
                    # never a partial chunk, never a zero-cell matrix.
                    cells = internal["tables"][0]["cells"]
                    if len(cells) != 10_000:
                        failures.append(f"partial matrix visible: {len(cells)} cells")
                        return
                # A sealed neighbour stays fully readable throughout.
                assert ledger.get_run_detail("run-sealed") is not None
            except AssertionError as exc:  # pragma: no cover - failure path
                failures.append(str(exc) or "reader assertion failed")
                return

    threads = [threading.Thread(target=reader) for _ in range(3)]
    for thread in threads:
        thread.start()
    records = list(_records(_tuples("t1", 100, 100)))
    for seq in range(40):
        writer.write_cell_chunk("run-live", seq, records[seq * 256 : (seq + 1) * 256])
    writer.seal_run("run-live")
    stop.set()
    for thread in threads:
        thread.join()
    assert failures == []
    assert {run["id"] for run in writer.list_runs(limit=50)} == {"run-sealed", "run-live"}
    detail = writer.load_for_verification("run-live")
    assert len(detail["tables"][0]["cells"]) == 10_000


# ----------------------------------------------------------------------
# Migration from pre-chunking databases
# ----------------------------------------------------------------------


def test_legacy_database_migrates_and_stays_readable(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    with sqlite3.connect(db) as conn:
        conn.executescript(OLD_SCHEMA)
        conn.execute(
            "INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("run-legacy", "old", "ok", "2025-01-01T00:00:00Z", "2025-01-01T00:00:01Z",
             "ef" * 32, "/tmp/c.yml", "/tmp/out", json.dumps({"cell_count": 4})),
        )
        conn.execute(
            "INSERT INTO tables VALUES (?, ?, 2, 2, 4, 0, 0, 0.0, 'ok', '[]', 1, '{}')",
            ("run-legacy", "t1"),
        )
        conn.executemany(
            "INSERT INTO cells VALUES (?, ?, ?, ?, 7, 'none', 'safe', ?)",
            [
                ("run-legacy", "t1", row, col, cell_fingerprint("t1", row, col))
                for row in ("R1", "R2")
                for col in ("C1", "C2")
            ],
        )
        conn.execute("INSERT INTO checks (run_id, table_id, name, result, evidence_json, source) "
                     "VALUES ('run-legacy', 't1', 'c', 'pass', '{}', 'run')")
        conn.execute("INSERT INTO artifacts (run_id, path, kind, sha256) "
                     "VALUES ('run-legacy', '/tmp/out/a.json', 'audit', ?)", ("ab" * 32,))

    ledger = Ledger(db)  # migrates in place
    with sqlite3.connect(db) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 1
        assert conn.execute("SELECT sealed FROM runs WHERE id = 'run-legacy'").fetchone()[0] == 1
    assert [run["id"] for run in ledger.list_runs()] == ["run-legacy"]
    detail = ledger.get_run_detail("run-legacy")
    assert detail is not None and len(detail["tables"]) == 1
    internal = ledger.load_for_verification("run-legacy")
    assert len(internal["tables"][0]["cells"]) == 4
    # Re-opening is a no-op and new runs still record normally.
    Ledger(db)
    outcome = execute_run(DEMO, tmp_path / "out", ledger)
    assert outcome.run_id in [run["id"] for run in ledger.list_runs()]


# ----------------------------------------------------------------------
# Bounded memory
# ----------------------------------------------------------------------


def test_streaming_peak_memory_is_bounded(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3", chunk_size=2048)
    rows, cols = 300, 200  # 60k cells; a full dict list would exceed 30 MB
    _begin(ledger, "run-big", rows=rows, cols=cols)
    tracemalloc.start()
    try:
        ledger.stream_cells("run-big", _records(_tuples("t1", rows, cols)))
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    ledger.seal_run("run-big")
    assert peak < 25 * 1024 * 1024
    assert len(ledger.load_for_verification("run-big")["tables"][0]["cells"]) == rows * cols


# ----------------------------------------------------------------------
# Public API hygiene
# ----------------------------------------------------------------------


def test_public_reads_hide_recording_internals_and_raw_counts(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3", chunk_size=16)
    _begin(ledger, "run-a", rows=4, cols=4)
    ledger.stream_cells("run-a", _records(_tuples("t1", 4, 4)))
    ledger.seal_run("run-a")
    listed = ledger.list_runs()
    detail = ledger.get_run_detail("run-a")
    blob = json.dumps({"listed": listed, "detail": detail})
    for forbidden in ("sealed", "seal_expect", "run_chunks", "raw_count", "chunk_digest", "chain_digest"):
        assert forbidden not in blob
    assert set(listed[0]) == {
        "id", "dataset_name", "status", "started_at", "finished_at", "contract_hash",
        "contract_path", "output_dir", "summary", "suppression_ratio", "primary_count",
        "secondary_count", "table_count", "cell_count", "blocked_reasons",
    }


# ----------------------------------------------------------------------
# Pipeline-level crash, resume and CLI recovery
# ----------------------------------------------------------------------


def _crash_on_third_chunk(ledger: Ledger, monkeypatch) -> None:
    original = Ledger._write_chunk
    calls = {"n": 0}

    def flaky(self, conn, run_id, seq, cells):
        calls["n"] += 1
        if calls["n"] == 3:
            raise RuntimeError("simulated crash after two chunks")
        return original(self, conn, run_id, seq, cells)

    monkeypatch.setattr(Ledger, "_write_chunk", flaky)


def test_pipeline_crash_then_resume_and_reverify(tmp_path, monkeypatch):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db, chunk_size=8)  # demo fixture -> several chunks
    _crash_on_third_chunk(ledger, monkeypatch)
    with pytest.raises(RuntimeError):
        execute_run(DEMO, tmp_path / "out", ledger)

    monkeypatch.undo()
    restarted = Ledger(db, chunk_size=8)
    assert restarted.list_runs() == []
    pending = restarted.pending_runs()
    assert len(pending) == 1
    run_id = pending[0]["run_id"]
    with pytest.raises(LedgerError):
        reverify_run(restarted, run_id)

    outcome = resume_run(restarted, run_id)
    assert outcome.status == "ok"
    assert [run["id"] for run in restarted.list_runs()] == [run_id]
    verification = reverify_run(restarted, run_id)
    assert verification.passed
    # A sealed run cannot be resumed or aborted again.
    with pytest.raises(LedgerError) as info:
        resume_run(restarted, run_id)
    assert info.value.code == "E_RUN_SEALED"


def test_resume_refuses_changed_contract(tmp_path, monkeypatch, contract_factory):
    contract = contract_factory(rows=[f"R{i}" for i in range(6)], cols=["C1", "C2"])
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db, chunk_size=4)
    _crash_on_third_chunk(ledger, monkeypatch)
    with pytest.raises(RuntimeError):
        execute_run(contract, tmp_path / "out", ledger)
    monkeypatch.undo()
    run_id = ledger.pending_runs()[0]["run_id"]

    contract_factory(rows=[f"R{i}" for i in range(6)], cols=["C1", "C2"], min_count=99)
    with pytest.raises(LedgerError) as info:
        resume_run(ledger, run_id)
    assert info.value.code == "E_LEDGER_RESUME"
    # The interrupted recording is still there and can be aborted instead.
    ledger.abort_run(run_id)
    assert ledger.pending_runs() == []


def test_cli_ledger_recovery_workflow(tmp_path, capsys, monkeypatch):
    db = str(tmp_path / "ledger.sqlite3")
    out = str(tmp_path / "out")
    ledger = Ledger(db, chunk_size=8)
    _crash_on_third_chunk(ledger, monkeypatch)
    with pytest.raises(RuntimeError):
        execute_run(DEMO, out, ledger)
    monkeypatch.undo()

    assert cli_main(["ledger", "pending", "--db", db, "--json"]) == 0
    pending = json.loads(capsys.readouterr().out)
    assert len(pending) == 1
    run_id = pending[0]["run_id"]
    assert pending[0]["cells_committed"] > 0

    assert cli_main(["runs", "--db", db, "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == []

    assert cli_main(["ledger", "resume", "--run", run_id, "--db", db, "--json"]) == 0
    resumed = json.loads(capsys.readouterr().out)
    assert resumed["status"] == "ok"

    assert cli_main(["runs", "--db", db, "--json"]) == 0
    assert [run["id"] for run in json.loads(capsys.readouterr().out)] == [run_id]

    assert cli_main(["ledger", "pending", "--db", db, "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == []


def test_cli_ledger_abort(tmp_path, capsys, monkeypatch):
    db = str(tmp_path / "ledger.sqlite3")
    ledger = Ledger(db, chunk_size=8)
    _crash_on_third_chunk(ledger, monkeypatch)
    with pytest.raises(RuntimeError):
        execute_run(DEMO, tmp_path / "out", ledger)
    monkeypatch.undo()
    run_id = ledger.pending_runs()[0]["run_id"]

    assert cli_main(["ledger", "abort", "--run", run_id, "--db", db, "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == {"run_id": run_id, "aborted": True}
    assert Ledger(db).pending_runs() == []
    # Aborting a sealed run fails with a stable exit code.
    outcome = execute_run(DEMO, tmp_path / "out2", Ledger(db))
    assert cli_main(["ledger", "abort", "--run", outcome.run_id, "--db", db]) == 2
