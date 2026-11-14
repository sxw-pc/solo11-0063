"""Stable in-memory models shared by the pipeline stages."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Iterator

RISK_SAFE = "safe"
RISK_SMALL = "small_count"
RISK_SAMPLING_ZERO = "sampling_zero"
RISK_FORCED = "forced"
RISK_BLOCKED = "blocked"
RISK_LABELS = (
    RISK_SAFE,
    RISK_SMALL,
    RISK_SAMPLING_ZERO,
    RISK_FORCED,
    RISK_BLOCKED,
)

SUPPRESSION_NONE = "none"
SUPPRESSION_PRIMARY = "primary"
SUPPRESSION_SECONDARY = "secondary"
SUPPRESSION_LABELS = (
    SUPPRESSION_NONE,
    SUPPRESSION_PRIMARY,
    SUPPRESSION_SECONDARY,
)

Coordinate = tuple[str, str]


def cell_fingerprint(table_id: str, row: str, col: str) -> str:
    """A stable coordinate fingerprint that deliberately excludes the count."""

    material = "\x1f".join(("cellcloak-cell-v1", table_id, row, col))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Cell:
    row: str
    col: str
    count: int

    @property
    def coordinate(self) -> Coordinate:
        return (self.row, self.col)

    def fingerprint(self, table_id: str) -> str:
        return cell_fingerprint(table_id, self.row, self.col)


@dataclass(frozen=True)
class SuppressionReason:
    kind: str
    risk_category: str
    detail: str
    round_no: int = 0

    def __post_init__(self) -> None:
        if self.kind not in (SUPPRESSION_PRIMARY, SUPPRESSION_SECONDARY):
            raise ValueError(f"invalid suppression kind: {self.kind}")


@dataclass(frozen=True)
class TableModel:
    table_id: str
    row_dim: str
    col_dim: str
    value_dim: str
    row_keys: tuple[str, ...]
    col_keys: tuple[str, ...]
    counts: dict[Coordinate, int]
    source_name: str
    source_sha256: str

    def count(self, row: str, col: str) -> int:
        return self.counts[(row, col)]

    def coordinates(self) -> Iterator[Coordinate]:
        for row in self.row_keys:
            for col in self.col_keys:
                yield (row, col)

    def cells(self) -> Iterator[Cell]:
        for row in self.row_keys:
            for col in self.col_keys:
                yield Cell(row, col, self.counts[(row, col)])

    def row_total(self, row: str) -> int:
        return sum(self.counts[(row, col)] for col in self.col_keys)

    def col_total(self, col: str) -> int:
        return sum(self.counts[(row, col)] for row in self.row_keys)

    def total(self) -> int:
        return sum(self.counts.values())

    def row_count(self) -> int:
        return len(self.row_keys)

    def col_count(self) -> int:
        return len(self.col_keys)

    def cell_count(self) -> int:
        return len(self.row_keys) * len(self.col_keys)

    def fingerprint(self) -> str:
        material = "\x1f".join(
            (
                "cellcloak-table-v1",
                self.table_id,
                self.source_sha256,
                ",".join(self.row_keys),
                ",".join(self.col_keys),
            )
        )
        return hashlib.sha256(material.encode("utf-8")).hexdigest()
