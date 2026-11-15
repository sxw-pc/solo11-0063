"""Compare ledger write strategies for large matrices.

Path A: one SQLite transaction, chunked ``executemany`` from a generator
        (bounded memory, but the writer lock spans the whole load and a
        crash rolls everything back).
Path B: the production protocol — short per-chunk transactions into a
        hidden recording state, then one verify-and-seal transaction.

Both paths are measured under WAL (production setting) and under the legacy
rollback journal (DELETE) to expose reader/writer blocking differences. Each
leg runs in a subprocess so peak-RSS numbers are clean. A probe thread
continuously reads a pre-existing sealed run during the write and samples
the WAL size.

Usage:
    python3 benchmarks/bench_ledger.py                      # full matrix, markdown table
    python3 benchmarks/bench_ledger.py run A wal 100000     # one leg, JSON on stdout
"""

from __future__ import annotations

import json
import os
import resource
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import tracemalloc
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cellcloak.ledger import Ledger, cell_stream_expectation  # noqa: E402
from cellcloak.model import cell_fingerprint  # noqa: E402

CHUNK = 4096
SIZES = (100_000, 1_000_000)
LEGS = (("A", "wal"), ("B", "wal"), ("A", "delete"), ("B", "delete"))


def _tuples(table_id: str, total: int, cols: int = 100):
    rows = total // cols
    for r in range(rows):
        for c in range(cols):
            yield (table_id, f"r{r:06d}", f"c{c:03d}", (r * 31 + c * 7) % 41, "none", "safe")


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


def _run_record(run_id: str, cell_count: int) -> dict:
    return {
        "id": run_id,
        "dataset_name": "bench",
        "status": "ok",
        "started_at": "2026-01-01T00:00:00Z",
        "finished_at": "2026-01-01T00:00:01Z",
        "contract_hash": "ab" * 32,
        "contract_path": "/tmp/c.yml",
        "output_dir": "/tmp/out",
        "summary": {"cell_count": cell_count},
    }


def _table_record(cell_count: int) -> dict:
    return {
        "table_id": "bench",
        "row_count": cell_count // 100,
        "col_count": 100,
        "cell_count": cell_count,
        "primary_count": 0,
        "secondary_count": 0,
        "suppression_ratio": 0.0,
        "table_status": "ok",
        "block_reasons": [],
        "verify_passed": True,
        "meta": {},
    }


class ReadProbe:
    """Read latency + WAL growth against a sealed neighbour during the write.

    Uses raw sqlite3 so it never touches the database's journal mode.
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.stop = threading.Event()
        self.latencies: list[float] = []
        self.wal_peak = 0
        self.thread = threading.Thread(target=self._loop, daemon=True)

    def _loop(self) -> None:
        conn = sqlite3.connect(self.db_path, timeout=30)
        wal = str(self.db_path) + "-wal"
        try:
            while not self.stop.is_set():
                start = time.perf_counter()
                conn.execute("SELECT id FROM runs WHERE sealed = 1 ORDER BY id DESC LIMIT 20").fetchall()
                conn.execute("SELECT * FROM tables WHERE run_id = 'run-seeded'").fetchall()
                self.latencies.append(time.perf_counter() - start)
                try:
                    self.wal_peak = max(self.wal_peak, os.path.getsize(wal))
                except OSError:
                    pass
        finally:
            conn.close()

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.stop.set()
        self.thread.join()
        return False

    def summary(self) -> dict:
        if not self.latencies:
            return {"reads": 0, "avg_ms": 0.0, "p95_ms": 0.0, "max_ms": 0.0}
        ordered = sorted(self.latencies)
        p95 = ordered[max(0, int(len(ordered) * 0.95) - 1)]
        return {
            "reads": len(ordered),
            "avg_ms": round(sum(ordered) / len(ordered) * 1000, 2),
            "p95_ms": round(p95 * 1000, 2),
            "max_ms": round(max(ordered) * 1000, 2),
        }


def _seed(db_path: Path) -> None:
    ledger = Ledger(db_path, chunk_size=CHUNK)
    expect = cell_stream_expectation("run-seeded", _tuples("bench", 100), CHUNK)
    ledger.begin_run(_run_record("run-seeded", 100), [_table_record(100)], [], [], cell_expect=expect)
    ledger.stream_cells("run-seeded", _records(_tuples("bench", 100)))
    ledger.seal_run("run-seeded")


def _set_journal(db_path: Path, mode: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"PRAGMA journal_mode={mode.upper()}")


def run_path_a(db_path: Path, cells: int) -> None:
    """Single transaction, chunked executemany, sealed from the start."""

    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        run = _run_record("run-bench", cells)
        conn.execute(
            "INSERT INTO runs (id, dataset_name, status, started_at, finished_at,"
            " contract_hash, contract_path, output_dir, summary_json, sealed, seal_expect_json)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, '')",
            (run["id"], run["dataset_name"], run["status"], run["started_at"],
             run["finished_at"], run["contract_hash"], run["contract_path"],
             run["output_dir"], json.dumps(run["summary"])),
        )
        table = _table_record(cells)
        conn.execute(
            "INSERT INTO tables VALUES (?, ?, ?, ?, ?, 0, 0, 0.0, 'ok', '[]', 1, '{}')",
            (run["id"], table["table_id"], table["row_count"], table["col_count"], cells),
        )
        batch: list[tuple] = []
        for record in _records(_tuples("bench", cells)):
            batch.append((
                run["id"], record["table_id"], record["row_key"], record["col_key"],
                record["raw_count"], record["suppression_type"], record["risk_category"],
                record["fingerprint"],
            ))
            if len(batch) >= CHUNK:
                conn.executemany("INSERT INTO cells VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
                batch.clear()
        if batch:
            conn.executemany("INSERT INTO cells VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
        conn.commit()
    finally:
        conn.close()


def run_path_b(db_path: Path, cells: int) -> None:
    """Production protocol: begin -> chunked short transactions -> seal."""

    ledger = Ledger(db_path, chunk_size=CHUNK)
    expect = cell_stream_expectation("run-bench", _tuples("bench", cells), CHUNK)
    ledger.begin_run(_run_record("run-bench", cells), [_table_record(cells)], [], [], cell_expect=expect)
    ledger.stream_cells("run-bench", _records(_tuples("bench", cells)))
    ledger.seal_run("run-bench")


def run_one(path: str, journal: str, cells: int) -> dict:
    with tempfile.TemporaryDirectory(prefix="cellcloak-bench-") as tmp:
        db_path = Path(tmp) / "ledger.sqlite3"
        _seed(db_path)
        _set_journal(db_path, journal)
        tracemalloc.start()
        started = time.perf_counter()
        with ReadProbe(db_path) as probe:
            if path == "A":
                run_path_a(db_path, cells)
            else:
                run_path_b(db_path, cells)
        elapsed = time.perf_counter() - started
        _, peak_py = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        return {
            "path": path,
            "journal": journal,
            "cells": cells,
            "seconds": round(elapsed, 2),
            "peak_python_mb": round(peak_py / 1024 / 1024, 1),
            "peak_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1),
            "wal_peak_mb": round(probe.wal_peak / 1024 / 1024, 1),
            "read": probe.summary(),
        }


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "run":
        print(json.dumps(run_one(sys.argv[2], sys.argv[3], int(sys.argv[4]))))
        return
    sizes = (int(sys.argv[1]),) if len(sys.argv) > 1 else SIZES
    results = []
    for cells in sizes:
        for path, journal in LEGS:
            output = subprocess.run(
                [sys.executable, str(Path(__file__)), "run", path, journal, str(cells)],
                check=True, capture_output=True, text=True,
            )
            results.append(json.loads(output.stdout.strip()))
    print("| cells | path | journal | wall s | peak Py MB | peak RSS MB | WAL peak MB | reads | read avg ms | read p95 ms | read max ms |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for item in results:
        read = item["read"]
        print(
            f"| {item['cells']:,} | {item['path']} | {item['journal']} | {item['seconds']} "
            f"| {item['peak_python_mb']} | {item['peak_rss_mb']} | {item['wal_peak_mb']} "
            f"| {read['reads']} | {read['avg_ms']} | {read['p95_ms']} | {read['max_ms']} |"
        )


if __name__ == "__main__":
    main()
