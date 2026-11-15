"""Stable, user-facing error types and codes."""

from __future__ import annotations

from pathlib import Path


class CellcloakError(Exception):
    """Base exception carrying a stable code and optional locator."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        path: str | Path | None = None,
        table_id: str | None = None,
        field: str | None = None,
        cell: tuple[str, str] | None = None,
    ) -> None:
        self.code = code
        self.message = message
        self.path = str(path) if path is not None else None
        self.table_id = table_id
        self.field = field
        self.cell = cell
        super().__init__(self.render())

    def render(self) -> str:
        locators: list[str] = []
        if self.table_id:
            locators.append(f"table={self.table_id}")
        if self.field:
            locators.append(f"field={self.field}")
        if self.cell:
            locators.append(f"cell={self.cell[0]}/{self.cell[1]}")
        if self.path:
            locators.append(f"file={self.path}")
        suffix = f" [{', '.join(locators)}]" if locators else ""
        return f"{self.code}: {self.message}{suffix}"


class ContractError(CellcloakError):
    """A publication contract is unreadable or invalid."""


class IngestError(CellcloakError):
    """An aggregated CSV cannot be loaded safely."""


class LedgerError(CellcloakError):
    """The local audit ledger cannot be read or written."""


class PipelineError(CellcloakError):
    """The pipeline cannot produce a trustworthy result."""


class VerifySnapshotError(PipelineError):
    """A verification attempt's inputs changed between the read and the commit."""

    def __init__(self, run_id: str, attempt_id: int, expected_sha: str, actual_sha: str) -> None:
        super().__init__(
            "E_VERIFY_SNAPSHOT_CONFLICT",
            "verification input snapshot changed during the attempt; the attempt was "
            "marked stale and the run status was not updated, retry verification",
            table_id=run_id,
        )
        self.attempt_id = attempt_id
        self.expected_sha = expected_sha
        self.actual_sha = actual_sha
