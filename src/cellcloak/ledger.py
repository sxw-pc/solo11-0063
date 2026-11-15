"""SQLite audit ledger. Raw cell counts live only in this local database."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
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
    summary_json TEXT NOT NULL,
    input_set_json TEXT NOT NULL DEFAULT ''
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
CREATE INDEX IF NOT EXISTS idx_runs_started ON runs(started_at);
CREATE INDEX IF NOT EXISTS idx_checks_run ON checks(run_id);
"""


def _loads(value: str, fallback: Any) -> Any:
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return fallback


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class Ledger:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path).expanduser().resolve()
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            with self._connection() as conn:
                conn.executescript(SCHEMA)
                self._migrate(conn)
        except (OSError, sqlite3.Error) as exc:
            raise LedgerError("E_LEDGER_DB", "cannot initialise local SQLite ledger", path=self.db_path) from exc

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        """Add newer nullable/defaulted columns to pre-existing ledgers.

        Historical runs remain readable: their ``input_set_json`` stays empty
        and is surfaced as an unpinned legacy input set.
        """

        columns = {row["name"] for row in conn.execute("PRAGMA table_info(runs)")}
        if "input_set_json" not in columns:
            conn.execute("ALTER TABLE runs ADD COLUMN input_set_json TEXT NOT NULL DEFAULT ''")

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
                     contract_path, output_dir, summary_json, input_set_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                        _dumps(run.get("input_set", {})),
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
                    """INSERT INTO cells
                    (run_id, table_id, row_key, col_key, raw_count, suppression_type,
                     risk_category, fingerprint)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    [
                        (
                            run["id"], item["table_id"], item["row_key"], item["col_key"],
                            item["raw_count"], item["suppression_type"],
                            item["risk_category"], item["fingerprint"],
                        )
                        for item in cells
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
            raise LedgerError("E_RUN_DUPLICATE", "run ID or cell already exists", path=self.db_path) from exc

    def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._connection() as conn:
            rows = conn.execute("SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            summary = _loads(item.pop("summary_json"), {})
            item["summary"] = summary
            item["input_set"] = self._input_set(item.pop("input_set_json", ""))
            item["suppression_ratio"] = summary.get("suppression_ratio", 0.0)
            item["primary_count"] = summary.get("primary_count", 0)
            item["secondary_count"] = summary.get("secondary_count", 0)
            item["table_count"] = summary.get("table_count", 0)
            item["cell_count"] = summary.get("cell_count", 0)
            item["blocked_reasons"] = summary.get("block_reasons", [])
            result.append(item)
        return result

    @staticmethod
    def _input_set(value: str) -> dict[str, Any]:
        record = _loads(value, {})
        if not isinstance(record, dict) or not record:
            return {
                "pinned": False,
                "mode": "legacy",
                "manifest_hash": None,
                "manifest_path": None,
                "members": [],
                "note": "input set was not recorded for this historical run",
            }
        return record

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
        run = dict(run_row)
        run["summary"] = _loads(run.pop("summary_json"), {})
        run["input_set"] = self._input_set(run.pop("input_set_json", ""))
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
            cursor = conn.execute("UPDATE runs SET status = ? WHERE id = ?", (status, run_id))
            if cursor.rowcount == 0:
                raise LedgerError("E_RUN_NOT_FOUND", "run is not present in the local ledger", table_id=run_id)

    def clear_reverify_checks(self, run_id: str) -> None:
        with self._connection() as conn:
            conn.execute("DELETE FROM checks WHERE run_id = ? AND source = 'reverify'", (run_id,))

    def add_checks(self, run_id: str, checks: Sequence[Mapping[str, Any]], source: str = "reverify") -> None:
        try:
            with self._connection() as conn:
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
