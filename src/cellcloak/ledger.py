"""SQLite audit ledger. Raw cell counts live only in this local database."""

from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from .errors import LedgerError

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
    summary_json TEXT NOT NULL
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
CREATE TABLE IF NOT EXISTS verify_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    attempt_id TEXT NOT NULL UNIQUE,
    sequence_no INTEGER NOT NULL,
    source TEXT NOT NULL,
    status TEXT NOT NULL,
    result TEXT,
    snapshot_sha TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    detail_json TEXT NOT NULL,
    UNIQUE (run_id, sequence_no)
);
CREATE TABLE IF NOT EXISTS checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    attempt_pk INTEGER REFERENCES verify_attempts(id) ON DELETE CASCADE,
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
"""

# Indexes referencing the post-migration checks.attempt_pk column are created
# only after _migrate has upgraded pre-attempt databases.
POST_MIGRATION_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_runs_started ON runs(started_at);
CREATE INDEX IF NOT EXISTS idx_checks_run ON checks(run_id);
CREATE INDEX IF NOT EXISTS idx_attempts_run ON verify_attempts(run_id);
CREATE INDEX IF NOT EXISTS idx_checks_attempt ON checks(attempt_pk);
"""

SCHEMA_VERSION = 1


def _loads(value: str, fallback: Any) -> Any:
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return fallback


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def new_attempt_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"att-{stamp}-{secrets.token_hex(3)}"


def canonical_snapshot(
    run: Mapping[str, Any],
    tables: Sequence[Mapping[str, Any]],
    cells: Sequence[Mapping[str, Any]],
) -> str:
    """Deterministic SHA-256 over the exact verification inputs of a run.

    The run status is deliberately excluded: it is the *projection* of
    verification, so reverifying identical inputs produces the same snapshot.
    """

    run_part = {
        key: run[key]
        for key in (
            "id",
            "dataset_name",
            "started_at",
            "finished_at",
            "contract_hash",
            "contract_path",
            "output_dir",
            "summary_json",
        )
    }
    table_parts = [
        {
            "table_id": item["table_id"],
            "row_count": item["row_count"],
            "col_count": item["col_count"],
            "cell_count": item["cell_count"],
            "primary_count": item["primary_count"],
            "secondary_count": item["secondary_count"],
            "suppression_ratio": item["suppression_ratio"],
            "table_status": item["table_status"],
            "block_reasons_json": item["block_reasons_json"],
            "verify_passed": int(item["verify_passed"]),
            "meta_json": item["meta_json"],
        }
        for item in tables
    ]
    cell_parts = [
        {
            "table_id": item["table_id"],
            "row_key": item["row_key"],
            "col_key": item["col_key"],
            "raw_count": item["raw_count"],
            "suppression_type": item["suppression_type"],
            "risk_category": item["risk_category"],
            "fingerprint": item["fingerprint"],
        }
        for item in cells
    ]
    canonical = json.dumps(
        {"run": run_part, "tables": table_parts, "cells": cell_parts},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class Ledger:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path).expanduser().resolve()
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            with self._connection() as conn:
                conn.executescript(SCHEMA)
                self._migrate(conn)
                conn.executescript(POST_MIGRATION_INDEXES)
        except (OSError, sqlite3.Error) as exc:
            raise LedgerError("E_LEDGER_DB", "cannot initialise local SQLite ledger", path=self.db_path) from exc

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
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------------ schema

    def _migrate(self, conn: sqlite3.Connection) -> None:
        """Bring pre-attempt databases forward without losing old evidence."""

        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= SCHEMA_VERSION:
            return
        check_columns = {row["name"] for row in conn.execute("PRAGMA table_info(checks)").fetchall()}
        if "attempt_pk" not in check_columns:
            conn.execute("ALTER TABLE checks ADD COLUMN attempt_pk INTEGER")

        run_ids = [row["id"] for row in conn.execute("SELECT id FROM runs ORDER BY id").fetchall()]
        for run_id in run_ids:
            run_row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
            if run_row is None:
                continue
            legacy_groups = (
                ("run", "run", 1),
                ("reverify", "legacy", 2),
            )
            for old_source, attempt_source, fallback_seq in legacy_groups:
                orphan_rows = conn.execute(
                    "SELECT * FROM checks WHERE run_id = ? AND attempt_pk IS NULL AND source = ? "
                    "ORDER BY id",
                    (run_id, old_source),
                ).fetchall()
                if not orphan_rows:
                    continue
                tables = self._read_tables(conn, run_id)
                cells = self._read_cells(conn, run_id)
                snapshot_sha = canonical_snapshot(dict(run_row), tables, cells)
                sequence_no = conn.execute(
                    "SELECT COALESCE(MAX(sequence_no), 0) + 1 FROM verify_attempts WHERE run_id = ?",
                    (run_id,),
                ).fetchone()[0]
                # Deterministic id keeps re-runs of the migration stable.
                attempt_id = f"att-legacy-{fallback_seq}-{run_id}"
                migrated_result = (
                    "fail" if any(row["result"] == "fail" for row in orphan_rows) else "pass"
                )
                detail = {
                    "migrated_from_source": old_source,
                    "note": "imported from a pre-attempt ledger; original timestamps are unavailable",
                }
                cursor = conn.execute(
                    """INSERT INTO verify_attempts
                    (run_id, attempt_id, sequence_no, source, status, result, snapshot_sha,
                     started_at, finished_at, detail_json)
                    VALUES (?, ?, ?, ?, 'completed', ?, ?, ?, ?, ?)""",
                    (
                        run_id,
                        attempt_id,
                        sequence_no,
                        attempt_source,
                        migrated_result,
                        snapshot_sha,
                        run_row["finished_at"],
                        run_row["finished_at"],
                        _dumps(detail),
                    ),
                )
                attempt_pk = cursor.lastrowid
                conn.execute(
                    "UPDATE checks SET attempt_pk = ? WHERE run_id = ? AND attempt_pk IS NULL "
                    "AND source = ?",
                    (attempt_pk, run_id, old_source),
                )
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

    # ------------------------------------------------------------------ reads

    def _read_tables(self, conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
        return [dict(row) for row in conn.execute(
            "SELECT * FROM tables WHERE run_id = ? ORDER BY table_id", (run_id,)
        ).fetchall()]

    def _read_cells(self, conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
        return [dict(row) for row in conn.execute(
            "SELECT * FROM cells WHERE run_id = ? ORDER BY table_id, row_key, col_key",
            (run_id,),
        ).fetchall()]

    def _read_verification_state(self, conn: sqlite3.Connection, run_id: str) -> dict[str, Any]:
        run_row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        if run_row is None:
            raise LedgerError("E_RUN_NOT_FOUND", "run is not present in the local ledger", table_id=run_id)
        table_rows = self._read_tables(conn, run_id)
        cell_rows = self._read_cells(conn, run_id)
        artifact_rows = conn.execute(
            "SELECT path, kind, sha256 FROM artifacts WHERE run_id = ? ORDER BY path", (run_id,)
        ).fetchall()
        tables: list[dict[str, Any]] = []
        by_table: dict[str, list[dict[str, Any]]] = {}
        for row in cell_rows:
            by_table.setdefault(row["table_id"], []).append(dict(row))
        for row in table_rows:
            item = dict(row)
            item["block_reasons"] = _loads(item.pop("block_reasons_json"), [])
            item["meta"] = _loads(item.pop("meta_json"), {})
            item["verify_passed"] = bool(item["verify_passed"])
            item["cells"] = by_table.get(item["table_id"], [])
            tables.append(item)
        run = dict(run_row)
        run["summary"] = _loads(run.pop("summary_json"), {})
        snapshot_sha = canonical_snapshot(dict(run_row), [dict(r) for r in table_rows], cell_rows)
        return {
            "run": run,
            "tables": tables,
            "artifacts": [dict(row) for row in artifact_rows],
            "snapshot_sha": snapshot_sha,
        }

    def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._connection() as conn:
            rows = conn.execute("SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
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

    def _read_attempts(self, conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
        attempt_rows = conn.execute(
            "SELECT * FROM verify_attempts WHERE run_id = ? ORDER BY sequence_no", (run_id,)
        ).fetchall()
        check_rows = conn.execute(
            "SELECT * FROM checks WHERE run_id = ? ORDER BY id", (run_id,)
        ).fetchall()
        checks_by_attempt: dict[int, list[dict[str, Any]]] = {}
        for row in check_rows:
            item = dict(row)
            item["evidence"] = _loads(item.pop("evidence_json"), {})
            checks_by_attempt.setdefault(item["attempt_pk"], []).append(item)
        attempts: list[dict[str, Any]] = []
        for row in attempt_rows:
            item = dict(row)
            item["detail"] = _loads(item.pop("detail_json"), {})
            item["checks"] = checks_by_attempt.get(item["id"], [])
            attempts.append(item)
        return attempts

    def get_run_detail(self, run_id: str) -> dict[str, Any] | None:
        with self._connection() as conn:
            run_row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
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
            attempts = self._read_attempts(conn, run_id)
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
            "attempts": attempts,
            "artifacts": [dict(row) for row in artifact_rows],
        }

    def load_for_verification(self, run_id: str) -> dict[str, Any]:
        """Internal-only read including raw counts for independent re-verification."""

        with self._connection() as conn:
            return self._read_verification_state(conn, run_id)

    # ------------------------------------------------------------------ writes

    def record_run(
        self,
        run: Mapping[str, Any],
        tables: Sequence[Mapping[str, Any]],
        cells: Sequence[Mapping[str, Any]],
        checks: Sequence[Mapping[str, Any]],
        artifacts: Sequence[Mapping[str, Any]],
    ) -> None:
        try:
            with self._connection() as conn:
                conn.execute(
                    """INSERT INTO runs
                    (id, dataset_name, status, started_at, finished_at, contract_hash,
                     contract_path, output_dir, summary_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                    ),
                )
                table_rows = []
                for item in tables:
                    block_json = _dumps(item.get("block_reasons", []))
                    meta_json = _dumps(item.get("meta", {}))
                    conn.execute(
                        """INSERT INTO tables
                        (run_id, table_id, row_count, col_count, cell_count, primary_count,
                         secondary_count, suppression_ratio, table_status, block_reasons_json,
                         verify_passed, meta_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            run["id"], item["table_id"], item["row_count"], item["col_count"],
                            item["cell_count"], item["primary_count"], item["secondary_count"],
                            item["suppression_ratio"], item["table_status"],
                            block_json, int(item["verify_passed"]), meta_json,
                        ),
                    )
                    stored = dict(item)
                    stored["block_reasons_json"] = block_json
                    stored["meta_json"] = meta_json
                    stored["verify_passed"] = int(item["verify_passed"])
                    table_rows.append(stored)
                cell_rows = []
                for item in cells:
                    conn.execute(
                        """INSERT INTO cells
                        (run_id, table_id, row_key, col_key, raw_count, suppression_type,
                         risk_category, fingerprint)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            run["id"], item["table_id"], item["row_key"], item["col_key"],
                            item["raw_count"], item["suppression_type"],
                            item["risk_category"], item["fingerprint"],
                        ),
                    )
                    cell_rows.append(dict(item))
                conn.executemany(
                    "INSERT INTO artifacts (run_id, path, kind, sha256) VALUES (?, ?, ?, ?)",
                    [(run["id"], item["path"], item["kind"], item["sha256"]) for item in artifacts],
                )

                run_row = conn.execute("SELECT * FROM runs WHERE id = ?", (run["id"],)).fetchone()
                snapshot_sha = canonical_snapshot(dict(run_row), table_rows, cell_rows)
                started_at = _utc_now()
                cursor = conn.execute(
                    """INSERT INTO verify_attempts
                    (run_id, attempt_id, sequence_no, source, status, result, snapshot_sha,
                     started_at, finished_at, detail_json)
                    VALUES (?, ?, 1, 'run', 'completed', ?, ?, ?, ?, ?)""",
                    (
                        run["id"],
                        new_attempt_id(),
                        "fail" if run["status"] in {"verify_failed", "blocked"} else "pass",
                        snapshot_sha,
                        started_at,
                        started_at,
                        _dumps({"note": "verification executed during the original run"}),
                    ),
                )
                attempt_pk = cursor.lastrowid
                conn.executemany(
                    """INSERT INTO checks
                    (run_id, attempt_pk, table_id, name, result, evidence_json, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    [
                        (
                            run["id"], attempt_pk, item.get("table_id"), item["name"], item["result"],
                            _dumps(item.get("evidence", {})), item.get("source", "run"),
                        )
                        for item in checks
                    ],
                )
        except sqlite3.IntegrityError as exc:
            raise LedgerError("E_RUN_DUPLICATE", "run ID or cell already exists", path=self.db_path) from exc

    def begin_verification(self, run_id: str) -> dict[str, Any]:
        """Open an immutable attempt row and snapshot the inputs in one read transaction."""

        with self._connection() as conn:
            state = self._read_verification_state(conn, run_id)
            sequence_no = conn.execute(
                "SELECT COALESCE(MAX(sequence_no), 0) + 1 FROM verify_attempts WHERE run_id = ?",
                (run_id,),
            ).fetchone()[0]
            attempt_id = new_attempt_id()
            cursor = conn.execute(
                """INSERT INTO verify_attempts
                (run_id, attempt_id, sequence_no, source, status, result, snapshot_sha,
                 started_at, finished_at, detail_json)
                VALUES (?, ?, ?, 'reverify', 'interrupted', NULL, ?, ?, NULL, ?)""",
                (
                    run_id,
                    attempt_id,
                    sequence_no,
                    state["snapshot_sha"],
                    _utc_now(),
                    _dumps({"note": "attempt started; awaiting sealed result"}),
                ),
            )
            attempt_pk = cursor.lastrowid
        state["attempt_pk"] = attempt_pk
        state["attempt_id"] = attempt_id
        state["sequence_no"] = sequence_no
        return state

    def add_checks(
        self,
        run_id: str,
        checks: Sequence[Mapping[str, Any]],
        source: str = "reverify",
        *,
        conn: sqlite3.Connection | None = None,
        attempt_pk: int | None = None,
    ) -> None:
        """Insert check evidence. Public form opens its own transaction.

        The internal form (``conn`` supplied) participates in the caller's
        transaction so checks, attempt sealing and status projection commit
        atomically.
        """

        rows = [
            (
                run_id,
                attempt_pk,
                item.get("table_id"),
                item["name"],
                item["result"],
                _dumps(item.get("evidence", {})),
                source,
            )
            for item in checks
        ]
        statement = (
            "INSERT INTO checks (run_id, attempt_pk, table_id, name, result, evidence_json, source) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        if conn is not None:
            conn.executemany(statement, rows)
            return
        try:
            with self._connection() as own_conn:
                own_conn.executemany(statement, rows)
        except sqlite3.IntegrityError as exc:
            raise LedgerError("E_RUN_NOT_FOUND", "run is not present in the local ledger", table_id=run_id) from exc

    def update_status(
        self,
        run_id: str,
        status: str,
        *,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        statement = "UPDATE runs SET status = ? WHERE id = ?"
        if conn is not None:
            cursor = conn.execute(statement, (status, run_id))
            if cursor.rowcount == 0:
                raise LedgerError("E_RUN_NOT_FOUND", "run is not present in the local ledger", table_id=run_id)
            return
        with self._connection() as own_conn:
            cursor = own_conn.execute(statement, (status, run_id))
            if cursor.rowcount == 0:
                raise LedgerError("E_RUN_NOT_FOUND", "run is not present in the local ledger", table_id=run_id)

    def complete_verification(
        self,
        run_id: str,
        attempt_pk: int,
        attempt_id: str,
        expected_snapshot_sha: str,
        checks: Sequence[Mapping[str, Any]],
        passed: bool,
        projected_status: str,
        table_results: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        """Seal an attempt atomically: checks, result, status projection in one transaction.

        Re-reads the input snapshot before committing. If the run/table/cell
        facts changed while the computation ran, the attempt is sealed as
        ``stale`` without touching the run status and a retryable error fires.
        """

        with self._connection() as conn:
            attempt_row = conn.execute(
                "SELECT * FROM verify_attempts WHERE id = ? AND run_id = ?",
                (attempt_pk, run_id),
            ).fetchone()
            if attempt_row is None:
                raise LedgerError("E_RUN_NOT_FOUND", "verification attempt is not present in the local ledger", table_id=run_id)
            run_row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
            if run_row is None:
                raise LedgerError("E_RUN_NOT_FOUND", "run is not present in the local ledger", table_id=run_id)
            current_tables = self._read_tables(conn, run_id)
            current_cells = self._read_cells(conn, run_id)
            current_snapshot = canonical_snapshot(dict(run_row), current_tables, current_cells)
            finished_at = _utc_now()
            if current_snapshot != expected_snapshot_sha:
                # No checks, no status projection: seal the attempt as stale
                # inside this transaction and signal the caller to raise.
                conn.execute(
                    """UPDATE verify_attempts
                    SET status = 'stale', result = NULL, finished_at = ?, detail_json = ?
                    WHERE id = ?""",
                    (
                        finished_at,
                        _dumps(
                            {
                                "expected_snapshot_sha": expected_snapshot_sha,
                                "actual_snapshot_sha": current_snapshot,
                                "note": "inputs changed between snapshot read and sealing; no status projection",
                            }
                        ),
                        attempt_pk,
                    ),
                )
                return {
                    "attempt_pk": attempt_pk,
                    "attempt_id": attempt_id,
                    "snapshot_sha": expected_snapshot_sha,
                    "actual_snapshot_sha": current_snapshot,
                    "finished_at": finished_at,
                    "stale": True,
                }

            # These public methods are monkeypatchable interruption seams; with
            # ``conn`` supplied they still participate in this single transaction.
            self.add_checks(run_id, checks, source="reverify", conn=conn, attempt_pk=attempt_pk)
            self.update_status(run_id, projected_status, conn=conn)
            conn.execute(
                """UPDATE verify_attempts
                SET status = 'completed', result = ?, finished_at = ?, detail_json = ?
                WHERE id = ?""",
                (
                    "pass" if passed else "fail",
                    finished_at,
                    _dumps(
                        {
                            "tables": list(table_results),
                            "check_count": len(checks),
                            "projected_status": projected_status,
                        }
                    ),
                    attempt_pk,
                ),
            )
        return {
            "attempt_pk": attempt_pk,
            "attempt_id": attempt_id,
            "snapshot_sha": expected_snapshot_sha,
            "actual_snapshot_sha": expected_snapshot_sha,
            "finished_at": finished_at,
            "stale": False,
        }

    def mark_attempt_interrupted(self, attempt_pk: int) -> None:
        """Best-effort closure of an attempt that never reached sealing.

        Interrupted is an unfinished terminal state, so ``finished_at`` stays
        NULL; only the explanatory detail is recorded.
        """

        with self._connection() as conn:
            conn.execute(
                """UPDATE verify_attempts
                SET detail_json = ?
                WHERE id = ? AND status = 'interrupted'""",
                (
                    _dumps({"note": "attempt did not reach the sealing transaction"}),
                    attempt_pk,
                ),
            )
