"""SQLite audit ledger. Raw cell counts live only in this local database.

Recording protocol (schema v1)
------------------------------
A run is written in three phases so that cell streaming stays memory-bounded
and readers never observe a partial matrix:

1. ``begin_run`` commits the run row (``sealed = 0``), tables, checks,
   artifacts and a seal expectation (cell count, chained coordinate digest,
   child-row counts, chunk size) in one small transaction.
2. ``write_cell_chunk`` / ``stream_cells`` commit cells in bounded chunks.
   Each chunk carries a stable sequence number, its cell count, a digest of
   its own coordinate sequence and a chained digest linking it to the
   previous chunk and to the run id. A chunk and its cells commit or roll
   back together.
3. ``seal_run`` verifies, in a single transaction, the cumulative cell
   count, chunk contiguity, the chained coordinate digest and the
   run/table/check/artifact associations, then flips ``sealed = 1``.

Public reads (``list_runs``, ``get_run_detail``, ``load_for_verification``)
only ever see sealed runs. An interrupted recording can be inspected with
``pending_runs``/``recording_state`` and either resumed (the pipeline
re-generates the deterministic cell stream; committed chunks are verified
and skipped) or discarded with ``abort_run`` (foreign keys cascade).
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

from .errors import LedgerError

SCHEMA_VERSION = 1
DEFAULT_CHUNK_SIZE = 4096

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    contract_hash TEXT NOT NULL,
    contract_path TEXT NOT NULL,
    output_dir TEXT NOT NULL,
    summary_json TEXT NOT NULL,
    sealed INTEGER NOT NULL DEFAULT 0,
    seal_expect_json TEXT NOT NULL DEFAULT '',
    sealed_at TEXT
);
CREATE TABLE IF NOT EXISTS tables (
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
CREATE TABLE IF NOT EXISTS cells (
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
CREATE TABLE IF NOT EXISTS checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    table_id TEXT,
    name TEXT NOT NULL,
    result TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    source TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS artifacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    kind TEXT NOT NULL,
    sha256 TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS run_chunks (
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    chunk_seq INTEGER NOT NULL,
    cell_count INTEGER NOT NULL,
    chunk_digest TEXT NOT NULL,
    chain_digest TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY (run_id, chunk_seq)
);
CREATE INDEX IF NOT EXISTS idx_runs_started ON runs(started_at);
CREATE INDEX IF NOT EXISTS idx_checks_run ON checks(run_id);
"""

# Columns safe to expose through the public read API. Internal bookkeeping
# (sealed, seal_expect_json, sealed_at) and the cells/run_chunks tables are
# deliberately excluded so raw counts and recording state never leak into
# public run details.
_RUN_PUBLIC_COLUMNS = (
    "id, dataset_name, status, started_at, finished_at, contract_hash, "
    "contract_path, output_dir, summary_json"
)

# A cell's digest material: table id, row key, col key, raw count,
# suppression type, risk category. The fingerprint is derivable from the
# coordinate alone, so it adds nothing to the digest.
CellMaterial = tuple[str, str, str, int, str, str]


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _loads(value: str, fallback: Any) -> Any:
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return fallback


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _feed(hasher: "hashlib._Hash", value: str) -> None:
    data = value.encode("utf-8")
    hasher.update(len(data).to_bytes(8, "big"))
    hasher.update(data)


def genesis_digest(run_id: str) -> str:
    """Chain seed bound to one run, so chunks can never validate across runs."""

    hasher = hashlib.sha256()
    _feed(hasher, "cellcloak-cell-chain-v1")
    _feed(hasher, run_id)
    return hasher.hexdigest()


def cell_material(cell: Mapping[str, Any] | Sequence[Any]) -> CellMaterial:
    if isinstance(cell, Mapping):
        return (
            str(cell["table_id"]),
            str(cell["row_key"]),
            str(cell["col_key"]),
            int(cell["raw_count"]),
            str(cell["suppression_type"]),
            str(cell["risk_category"]),
        )
    return (
        str(cell[0]),
        str(cell[1]),
        str(cell[2]),
        int(cell[3]),
        str(cell[4]),
        str(cell[5]),
    )


def _feed_cell(hasher: "hashlib._Hash", material: CellMaterial) -> None:
    table_id, row_key, col_key, raw_count, suppression_type, risk_category = material
    _feed(hasher, table_id)
    _feed(hasher, row_key)
    _feed(hasher, col_key)
    _feed(hasher, str(raw_count))
    _feed(hasher, suppression_type)
    _feed(hasher, risk_category)


def chunk_digest(cells: Iterable[Mapping[str, Any] | Sequence[Any]]) -> str:
    """Digest of one chunk's ordered coordinate/count sequence."""

    hasher = hashlib.sha256()
    _feed(hasher, "cellcloak-chunk-v1")
    for cell in cells:
        _feed_cell(hasher, cell_material(cell))
    return hasher.hexdigest()


def chain_digest(previous_chain: str, chunk: str) -> str:
    hasher = hashlib.sha256()
    _feed(hasher, "cellcloak-chain-link-v1")
    _feed(hasher, previous_chain)
    _feed(hasher, chunk)
    return hasher.hexdigest()


def _batched(iterable: Iterable[Any], size: int) -> Iterator[list[Any]]:
    batch: list[Any] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def cell_stream_expectation(
    run_id: str,
    cells: Iterable[Mapping[str, Any] | Sequence[Any]],
    chunk_size: int,
) -> dict[str, Any]:
    """Fold a full cell stream into its seal expectation in O(chunk) memory.

    The digest depends on chunk boundaries, so the same ``chunk_size`` must be
    used for the expectation pass and the actual chunked write.
    """

    chain = genesis_digest(run_id)
    total = 0
    for batch in _batched(cells, chunk_size):
        chain = chain_digest(chain, chunk_digest(batch))
        total += len(batch)
    return {"cell_count": total, "digest": chain}


class Ledger:
    def __init__(self, db_path: str | Path, *, chunk_size: int = DEFAULT_CHUNK_SIZE) -> None:
        self.db_path = Path(db_path).expanduser().resolve()
        self.chunk_size = max(1, int(chunk_size))
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            with self._connection() as conn:
                # WAL lets readers inspect sealed runs while a recording is
                # streaming chunks in the background.
                conn.execute("PRAGMA journal_mode=WAL")
                conn.executescript(SCHEMA)
                self._migrate(conn)
        except (OSError, sqlite3.Error) as exc:
            raise LedgerError("E_LEDGER_DB", "cannot initialise local SQLite ledger", path=self.db_path) from exc

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= SCHEMA_VERSION:
            return
        run_columns = {row[1] for row in conn.execute("PRAGMA table_info(runs)")}
        if "sealed" not in run_columns:
            conn.execute("ALTER TABLE runs ADD COLUMN sealed INTEGER NOT NULL DEFAULT 0")
            conn.execute("ALTER TABLE runs ADD COLUMN seal_expect_json TEXT NOT NULL DEFAULT ''")
            conn.execute("ALTER TABLE runs ADD COLUMN sealed_at TEXT")
            # Runs written before chunking existed committed atomically in a
            # single transaction, so they are complete by construction.
            conn.execute("UPDATE runs SET sealed = 1")
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
        except sqlite3.Error as exc:
            raise LedgerError("E_LEDGER_DB", "cannot open local SQLite ledger", path=self.db_path) from exc
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        except sqlite3.IntegrityError:
            conn.rollback()
            raise
        except sqlite3.Error as exc:
            conn.rollback()
            raise LedgerError("E_LEDGER_DB", "SQLite operation failed", path=self.db_path) from exc
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Recording: begin -> chunk* -> seal
    # ------------------------------------------------------------------

    def begin_run(
        self,
        run: Mapping[str, Any],
        tables: Sequence[Mapping[str, Any]],
        checks: Sequence[Mapping[str, Any]],
        artifacts: Sequence[Mapping[str, Any]],
        *,
        cell_expect: Mapping[str, Any],
    ) -> None:
        """Commit the run shell and its small child rows, still unsealed."""

        expect = {
            "cell_count": int(cell_expect["cell_count"]),
            "digest": str(cell_expect["digest"]),
            "table_count": len(tables),
            "check_count": len(checks),
            "artifact_count": len(artifacts),
            "chunk_size": self.chunk_size,
        }
        try:
            with self._connection() as conn:
                conn.execute(
                    """INSERT INTO runs
                    (id, dataset_name, status, started_at, finished_at, contract_hash,
                     contract_path, output_dir, summary_json, sealed, seal_expect_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)""",
                    (
                        run["id"],
                        run["dataset_name"],
                        run["status"],
                        run["started_at"],
                        run["finished_at"],
                        run["contract_hash"],
                        run["contract_path"],
                        run["output_dir"],
                        _dumps(run.get("summary", {})),
                        _dumps(expect),
                    ),
                )
                conn.executemany(
                    """INSERT INTO tables
                    (run_id, table_id, row_count, col_count, cell_count, primary_count,
                     secondary_count, suppression_ratio, table_status, block_reasons_json,
                     verify_passed, meta_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [
                        (
                            run["id"], item["table_id"], item["row_count"], item["col_count"],
                            item["cell_count"], item["primary_count"], item["secondary_count"],
                            item["suppression_ratio"], item["table_status"],
                            _dumps(item.get("block_reasons", [])), int(item["verify_passed"]),
                            _dumps(item.get("meta", {})),
                        )
                        for item in tables
                    ],
                )
                conn.executemany(
                    """INSERT INTO checks
                    (run_id, table_id, name, result, evidence_json, source)
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    [
                        (
                            run["id"], item.get("table_id"), item["name"], item["result"],
                            _dumps(item.get("evidence", {})), item.get("source", "run"),
                        )
                        for item in checks
                    ],
                )
                conn.executemany(
                    "INSERT INTO artifacts (run_id, path, kind, sha256) VALUES (?, ?, ?, ?)",
                    [(run["id"], item["path"], item["kind"], item["sha256"]) for item in artifacts],
                )
        except sqlite3.IntegrityError as exc:
            raise LedgerError("E_RUN_DUPLICATE", "run ID already exists", path=self.db_path) from exc

    def _run_write_state(self, conn: sqlite3.Connection, run_id: str) -> sqlite3.Row:
        row = conn.execute(
            "SELECT sealed, seal_expect_json FROM runs WHERE id = ?", (run_id,)
        ).fetchone()
        if row is None:
            raise LedgerError("E_RUN_NOT_FOUND", "run is not present in the local ledger", table_id=run_id)
        if row["sealed"]:
            raise LedgerError(
                "E_RUN_SEALED",
                "run is already sealed; sealed audit records are immutable",
                table_id=run_id,
            )
        return row

    def _write_chunk(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        chunk_seq: int,
        cells: Sequence[Mapping[str, Any]],
    ) -> str:
        """Write one chunk inside ``conn``'s transaction. Caller commits."""

        if not cells:
            raise ValueError("cell chunks must not be empty")
        self._run_write_state(conn, run_id)
        digest = chunk_digest(cells)
        last = conn.execute(
            "SELECT chunk_seq, chain_digest FROM run_chunks WHERE run_id = ? "
            "ORDER BY chunk_seq DESC LIMIT 1",
            (run_id,),
        ).fetchone()
        next_seq = 0 if last is None else int(last["chunk_seq"]) + 1
        if chunk_seq < next_seq:
            existing = conn.execute(
                "SELECT cell_count, chunk_digest FROM run_chunks WHERE run_id = ? AND chunk_seq = ?",
                (run_id, chunk_seq),
            ).fetchone()
            if existing is not None and int(existing["cell_count"]) == len(cells) and existing["chunk_digest"] == digest:
                return "duplicate"
            raise LedgerError(
                "E_LEDGER_CHUNK_CONFLICT",
                f"chunk {chunk_seq} is already committed with different content",
                table_id=run_id,
            )
        if chunk_seq > next_seq:
            raise LedgerError(
                "E_LEDGER_CHUNK_SEQ",
                f"chunk sequence gap: expected {next_seq}, got {chunk_seq}",
                table_id=run_id,
            )
        previous_chain = genesis_digest(run_id) if last is None else str(last["chain_digest"])
        chain = chain_digest(previous_chain, digest)
        conn.execute(
            """INSERT INTO run_chunks
            (run_id, chunk_seq, cell_count, chunk_digest, chain_digest, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (run_id, chunk_seq, len(cells), digest, chain, _utcnow()),
        )
        conn.executemany(
            """INSERT INTO cells
            (run_id, table_id, row_key, col_key, raw_count, suppression_type,
             risk_category, fingerprint)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                (
                    run_id,
                    item["table_id"],
                    item["row_key"],
                    item["col_key"],
                    item["raw_count"],
                    item["suppression_type"],
                    item["risk_category"],
                    item["fingerprint"],
                )
                for item in cells
            ),
        )
        return "written"

    def write_cell_chunk(
        self,
        run_id: str,
        chunk_seq: int,
        cells: Sequence[Mapping[str, Any]],
    ) -> str:
        """Commit one cell chunk atomically.

        Returns ``"written"`` or ``"duplicate"`` (an identical chunk was
        already committed; the retry is dropped). Conflicting duplicates,
        sequence gaps and writes to sealed runs are rejected explicitly.
        """

        try:
            with self._connection() as conn:
                return self._write_chunk(conn, run_id, chunk_seq, cells)
        except sqlite3.IntegrityError as exc:
            raise LedgerError(
                "E_LEDGER_CHUNK_CONFLICT",
                "chunk cells clash with already committed coordinates",
                table_id=run_id,
            ) from exc

    def stream_cells(
        self,
        run_id: str,
        cells: Iterable[Mapping[str, Any]],
    ) -> dict[str, int]:
        """Write a cell iterable in bounded chunks, one transaction per chunk.

        The chunk size recorded at ``begin_run`` is reused so a resumed run
        keeps its original chunk boundaries. Peak extra memory is one chunk.
        """

        state = self.recording_state(run_id)
        if state is None:
            raise LedgerError("E_RUN_NOT_FOUND", "run is not present in the local ledger", table_id=run_id)
        if state["sealed"]:
            raise LedgerError(
                "E_RUN_SEALED",
                "run is already sealed; sealed audit records are immutable",
                table_id=run_id,
            )
        chunk_size = int(state["expect"].get("chunk_size") or self.chunk_size)
        written = skipped = 0
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            for seq, batch in enumerate(_batched(cells, chunk_size)):
                try:
                    result = self._write_chunk(conn, run_id, seq, batch)
                except BaseException:
                    conn.rollback()
                    raise
                else:
                    conn.commit()
                if result == "written":
                    written += 1
                else:
                    skipped += 1
        except sqlite3.IntegrityError as exc:
            raise LedgerError(
                "E_LEDGER_CHUNK_CONFLICT",
                "chunk cells clash with already committed coordinates",
                table_id=run_id,
            ) from exc
        except sqlite3.Error as exc:
            raise LedgerError("E_LEDGER_DB", "SQLite operation failed", path=self.db_path) from exc
        finally:
            conn.close()
        return {"chunks_written": written, "chunks_skipped": skipped}

    def seal_run(self, run_id: str) -> None:
        """Verify the recording and mark the run sealed in one transaction.

        Sealing requires: contiguous chunks, cumulative cell count equal to
        the expectation and to the actual cell rows, the chained coordinate
        digest equal to the expectation, and intact run/table/check/artifact
        associations. Any failure rolls back and the run stays unsealed.
        """

        with self._connection() as conn:
            row = conn.execute(
                "SELECT sealed, seal_expect_json FROM runs WHERE id = ?", (run_id,)
            ).fetchone()
            if row is None:
                raise LedgerError("E_RUN_NOT_FOUND", "run is not present in the local ledger", table_id=run_id)
            if row["sealed"]:
                return
            expect = _loads(row["seal_expect_json"], {})
            problems: list[str] = []
            aggregate = conn.execute(
                "SELECT COUNT(*) AS n, COALESCE(SUM(cell_count), 0) AS total, "
                "COALESCE(MAX(chunk_seq), -1) AS max_seq FROM run_chunks WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if int(aggregate["n"]) != int(aggregate["max_seq"]) + 1:
                problems.append(
                    f"chunk sequence gap: {aggregate['n']} chunks but max seq {aggregate['max_seq']}"
                )
            expected_cells = int(expect.get("cell_count", -1))
            if int(aggregate["total"]) != expected_cells:
                problems.append(f"chunk cell total {aggregate['total']} != expected {expected_cells}")
            stored_cells = conn.execute(
                "SELECT COUNT(*) AS n FROM cells WHERE run_id = ?", (run_id,)
            ).fetchone()
            if int(stored_cells["n"]) != expected_cells:
                problems.append(f"stored cell rows {stored_cells['n']} != expected {expected_cells}")
            chain = genesis_digest(run_id)
            for chunk in conn.execute(
                "SELECT chunk_seq, chunk_digest, chain_digest FROM run_chunks "
                "WHERE run_id = ? ORDER BY chunk_seq",
                (run_id,),
            ):
                chain = chain_digest(chain, str(chunk["chunk_digest"]))
                if chain != chunk["chain_digest"]:
                    problems.append(f"chunk chain broken at seq {chunk['chunk_seq']}")
            if chain != expect.get("digest"):
                problems.append("coordinate digest does not match the seal expectation")
            table_agg = conn.execute(
                "SELECT COUNT(*) AS n, COALESCE(SUM(cell_count), 0) AS total "
                "FROM tables WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if int(table_agg["n"]) != int(expect.get("table_count", -1)):
                problems.append("table rows do not match the seal expectation")
            if expected_cells >= 0 and int(table_agg["total"]) != expected_cells:
                problems.append("table cell counts do not add up to the expected cells")
            for key, table in (("check_count", "checks"), ("artifact_count", "artifacts")):
                count = conn.execute(
                    f"SELECT COUNT(*) AS n FROM {table} WHERE run_id = ?", (run_id,)
                ).fetchone()
                if int(count["n"]) != int(expect.get(key, -1)):
                    problems.append(f"{table} rows do not match the seal expectation")
            orphans = conn.execute(
                "SELECT COUNT(*) AS n FROM cells c WHERE c.run_id = ? AND NOT EXISTS "
                "(SELECT 1 FROM tables t WHERE t.run_id = c.run_id AND t.table_id = c.table_id)",
                (run_id,),
            ).fetchone()
            if int(orphans["n"]):
                problems.append(f"{orphans['n']} cells reference no table of this run")
            if problems:
                raise LedgerError(
                    "E_LEDGER_SEAL",
                    "refusing to seal run: " + "; ".join(problems),
                    table_id=run_id,
                )
            conn.execute(
                "UPDATE runs SET sealed = 1, sealed_at = ? WHERE id = ?",
                (_utcnow(), run_id),
            )

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------

    def pending_runs(self) -> list[dict[str, Any]]:
        """Interrupted recordings, newest first. Internal/recovery use only."""

        with self._connection() as conn:
            rows = conn.execute(
                """SELECT r.id, r.dataset_name, r.started_at, r.contract_path, r.output_dir,
                          r.seal_expect_json,
                          (SELECT COUNT(*) FROM run_chunks c WHERE c.run_id = r.id) AS chunks_committed,
                          (SELECT COALESCE(SUM(c.cell_count), 0) FROM run_chunks c
                            WHERE c.run_id = r.id) AS cells_committed
                FROM runs r WHERE r.sealed = 0 ORDER BY r.id"""
            ).fetchall()
        pending: list[dict[str, Any]] = []
        for row in rows:
            expect = _loads(row["seal_expect_json"], {})
            expected_cells = int(expect.get("cell_count", 0))
            chunk_size = max(1, int(expect.get("chunk_size", self.chunk_size)))
            pending.append(
                {
                    "run_id": row["id"],
                    "dataset_name": row["dataset_name"],
                    "started_at": row["started_at"],
                    "contract_path": row["contract_path"],
                    "output_dir": row["output_dir"],
                    "chunks_committed": int(row["chunks_committed"]),
                    "cells_committed": int(row["cells_committed"]),
                    "expected_cells": expected_cells,
                    "expected_chunks": -(-expected_cells // chunk_size),
                }
            )
        return pending

    def recording_state(self, run_id: str) -> dict[str, Any] | None:
        """Internal recording state for resume; ``None`` when unknown."""

        with self._connection() as conn:
            row = conn.execute(
                """SELECT id, dataset_name, status, started_at, finished_at, contract_hash,
                          contract_path, output_dir, summary_json, sealed, seal_expect_json
                FROM runs WHERE id = ?""",
                (run_id,),
            ).fetchone()
            if row is None:
                return None
            chunks = conn.execute(
                "SELECT COUNT(*) AS n, COALESCE(MAX(chunk_seq), -1) AS max_seq "
                "FROM run_chunks WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        state = dict(row)
        state["summary"] = _loads(state.pop("summary_json"), {})
        state["sealed"] = bool(state.pop("sealed"))
        state["expect"] = _loads(state.pop("seal_expect_json"), {})
        state["chunks_committed"] = int(chunks["n"])
        state["next_chunk_seq"] = int(chunks["max_seq"]) + 1
        return state

    def load_recording_tables(self, run_id: str) -> list[dict[str, Any]]:
        """Stored table rows of a recording run, for resume verification."""

        with self._connection() as conn:
            rows = conn.execute(
                "SELECT * FROM tables WHERE run_id = ? ORDER BY table_id", (run_id,)
            ).fetchall()
        tables: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["block_reasons"] = _loads(item.pop("block_reasons_json"), [])
            item["meta"] = _loads(item.pop("meta_json"), {})
            item["verify_passed"] = bool(item["verify_passed"])
            tables.append(item)
        return tables

    def abort_run(self, run_id: str) -> None:
        """Delete an unsealed recording and all of its staged rows."""

        with self._connection() as conn:
            row = conn.execute("SELECT sealed FROM runs WHERE id = ?", (run_id,)).fetchone()
            if row is None:
                raise LedgerError("E_RUN_NOT_FOUND", "run is not present in the local ledger", table_id=run_id)
            if row["sealed"]:
                raise LedgerError(
                    "E_RUN_SEALED",
                    "sealed runs are immutable audit records; refusing abort",
                    table_id=run_id,
                )
            conn.execute("DELETE FROM runs WHERE id = ?", (run_id,))

    # ------------------------------------------------------------------
    # Public reads: sealed runs only
    # ------------------------------------------------------------------

    def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._connection() as conn:
            rows = conn.execute(
                f"SELECT {_RUN_PUBLIC_COLUMNS} FROM runs WHERE sealed = 1 "
                "ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            summary = _loads(item.pop("summary_json"), {})
            item["summary"] = summary
            item["suppression_ratio"] = summary.get("suppression_ratio", 0.0)
            item["primary_count"] = summary.get("primary_count", 0)
            item["secondary_count"] = summary.get("secondary_count", 0)
            item["table_count"] = summary.get("table_count", 0)
            item["cell_count"] = summary.get("cell_count", 0)
            item["blocked_reasons"] = summary.get("block_reasons", [])
            result.append(item)
        return result

    def get_run_detail(self, run_id: str) -> dict[str, Any] | None:
        with self._connection() as conn:
            run_row = conn.execute(
                f"SELECT {_RUN_PUBLIC_COLUMNS} FROM runs WHERE id = ? AND sealed = 1",
                (run_id,),
            ).fetchone()
            if run_row is None:
                return None
            table_rows = conn.execute(
                "SELECT * FROM tables WHERE run_id = ? ORDER BY table_id", (run_id,)
            ).fetchall()
            check_rows = conn.execute(
                "SELECT * FROM checks WHERE run_id = ? ORDER BY id", (run_id,)
            ).fetchall()
            artifact_rows = conn.execute(
                "SELECT path, kind, sha256 FROM artifacts WHERE run_id = ? ORDER BY path", (run_id,)
            ).fetchall()
        run = dict(run_row)
        run["summary"] = _loads(run.pop("summary_json"), {})
        tables = []
        for row in table_rows:
            item = dict(row)
            item["block_reasons"] = _loads(item.pop("block_reasons_json"), [])
            item["meta"] = _loads(item.pop("meta_json"), {})
            item["verify_passed"] = bool(item["verify_passed"])
            tables.append(item)
        checks = []
        for row in check_rows:
            item = dict(row)
            item["evidence"] = _loads(item.pop("evidence_json"), {})
            checks.append(item)
        return {
            "run": run,
            "tables": tables,
            "checks": checks,
            "artifacts": [dict(row) for row in artifact_rows],
        }

    def load_for_verification(self, run_id: str) -> dict[str, Any]:
        """Internal-only read including raw counts for independent re-verification."""

        detail = self.get_run_detail(run_id)
        if detail is None:
            raise LedgerError("E_RUN_NOT_FOUND", "run is not present in the local ledger", table_id=run_id)
        with self._connection() as conn:
            rows = conn.execute(
                "SELECT table_id, row_key, col_key, raw_count, suppression_type, risk_category, fingerprint "
                "FROM cells WHERE run_id = ? ORDER BY table_id, row_key, col_key",
                (run_id,),
            ).fetchall()
        by_table: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            by_table.setdefault(row["table_id"], []).append(dict(row))
        for table in detail["tables"]:
            table["cells"] = by_table.get(table["table_id"], [])
        return detail

    def update_status(self, run_id: str, status: str) -> None:
        with self._connection() as conn:
            cursor = conn.execute(
                "UPDATE runs SET status = ? WHERE id = ? AND sealed = 1", (status, run_id)
            )
            if cursor.rowcount == 0:
                raise LedgerError("E_RUN_NOT_FOUND", "run is not present in the local ledger", table_id=run_id)

    def _require_sealed(self, conn: sqlite3.Connection, run_id: str) -> None:
        row = conn.execute("SELECT sealed FROM runs WHERE id = ?", (run_id,)).fetchone()
        if row is None or not row["sealed"]:
            raise LedgerError("E_RUN_NOT_FOUND", "run is not present in the local ledger", table_id=run_id)

    def clear_reverify_checks(self, run_id: str) -> None:
        with self._connection() as conn:
            self._require_sealed(conn, run_id)
            conn.execute("DELETE FROM checks WHERE run_id = ? AND source = 'reverify'", (run_id,))

    def add_checks(self, run_id: str, checks: Sequence[Mapping[str, Any]], source: str = "reverify") -> None:
        try:
            with self._connection() as conn:
                self._require_sealed(conn, run_id)
                conn.executemany(
                    """INSERT INTO checks
                    (run_id, table_id, name, result, evidence_json, source)
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    [
                        (
                            run_id, item.get("table_id"), item["name"], item["result"],
                            _dumps(item.get("evidence", {})), source,
                        )
                        for item in checks
                    ],
                )
        except sqlite3.IntegrityError as exc:
            raise LedgerError("E_RUN_NOT_FOUND", "run is not present in the local ledger", table_id=run_id) from exc
