"""Independent margin and single-unknown reconstruction checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from .model import (
    SUPPRESSION_PRIMARY,
    SUPPRESSION_SECONDARY,
    SuppressionReason,
    TableModel,
    cell_fingerprint,
)

Coordinate = tuple[str, str]


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class CellEvidence:
    row: str
    col: str
    fingerprint: str
    suppression_type: str
    risk_category: str
    row_suppressed_count: int
    col_suppressed_count: int
    directly_inferable: bool
    deduced_via: tuple[str, ...]


@dataclass(frozen=True)
class VerifyResult:
    passed: bool
    checks: tuple[CheckResult, ...]
    evidence: tuple[CellEvidence, ...]


def _margins_consistent(model: TableModel) -> bool:
    row_totals = {row: model.row_total(row) for row in model.row_keys}
    col_totals = {col: model.col_total(col) for col in model.col_keys}
    grand = model.total()
    return (
        sum(row_totals.values()) == grand
        and sum(col_totals.values()) == grand
        and all(sum(model.count(row, col) for col in model.col_keys) == row_totals[row] for row in model.row_keys)
        and all(sum(model.count(row, col) for row in model.row_keys) == col_totals[col] for col in model.col_keys)
    )


def _deductions(
    model: TableModel, suppressed: Mapping[Coordinate, SuppressionReason]
) -> set[Coordinate]:
    unknown = set(suppressed)
    deduced: set[Coordinate] = set()
    changed = True
    while changed:
        changed = False
        for row in model.row_keys:
            candidates = [col for col in model.col_keys if (row, col) in unknown]
            if len(candidates) == 1:
                coordinate = (row, candidates[0])
                unknown.remove(coordinate)
                deduced.add(coordinate)
                changed = True
        for col in model.col_keys:
            candidates = [row for row in model.row_keys if (row, col) in unknown]
            if len(candidates) == 1:
                coordinate = (candidates[0], col)
                unknown.remove(coordinate)
                deduced.add(coordinate)
                changed = True
    return deduced


def verify_table(
    model: TableModel,
    suppressed: Mapping[Coordinate, SuppressionReason],
    *,
    primary_coords: Iterable[Coordinate] | None = None,
    max_suppression_ratio: float | None = None,
) -> VerifyResult:
    checks: list[CheckResult] = []
    checks.append(
        CheckResult(
            "margin_consistency",
            _margins_consistent(model),
            "recomputed row, column, and grand margins agree with the source matrix",
        )
    )

    invalid = [
        coordinate
        for coordinate in suppressed
        if coordinate[0] not in model.row_keys
        or coordinate[1] not in model.col_keys
        or suppressed[coordinate].kind not in (SUPPRESSION_PRIMARY, SUPPRESSION_SECONDARY)
    ]
    checks.append(
        CheckResult(
            "suppression_map_valid",
            not invalid,
            "all suppressed cells are in range and use a known suppression type",
        )
    )

    primary_set = set(primary_coords) if primary_coords is not None else None
    if primary_set is None:
        checks.append(CheckResult("primary_coverage", True, "primary coordinates were not supplied"))
    else:
        missing = primary_set - set(suppressed)
        checks.append(
            CheckResult(
                "primary_coverage",
                not missing,
                f"{len(missing)} primary risk cell(s) are absent from the suppression map",
            )
        )

    if max_suppression_ratio is None:
        checks.append(CheckResult("suppression_ratio_limit", True, "limit was not supplied"))
    else:
        ratio = len(suppressed) / model.cell_count() if model.cell_count() else 0.0
        checks.append(
            CheckResult(
                "suppression_ratio_limit",
                ratio <= max_suppression_ratio + 1e-12,
                "suppression ratio is within the contract limit",
            )
        )

    single_rows = [
        row
        for row in model.row_keys
        if sum((row, col) in suppressed for col in model.col_keys) == 1
    ]
    single_cols = [
        col
        for col in model.col_keys
        if sum((row, col) in suppressed for row in model.row_keys) == 1
    ]
    checks.append(
        CheckResult(
            "single_unknown_rows",
            not single_rows,
            f"{len(single_rows)} row(s) contain exactly one suppressed cell",
        )
    )
    checks.append(
        CheckResult(
            "single_unknown_columns",
            not single_cols,
            f"{len(single_cols)} column(s) contain exactly one suppressed cell",
        )
    )

    deduced = _deductions(model, suppressed)
    checks.append(
        CheckResult(
            "reconstruction_attack",
            not deduced,
            f"iterative margin inspection recovered {len(deduced)} suppressed cell(s)",
        )
    )

    evidence: list[CellEvidence] = []
    coordinates = set(model.coordinates())
    for coordinate in sorted(suppressed):
        if coordinate not in coordinates:
            continue
        row, col = coordinate
        reason = suppressed[coordinate]
        evidence.append(
            CellEvidence(
                row=row,
                col=col,
                fingerprint=cell_fingerprint(model.table_id, row, col),
                suppression_type=reason.kind,
                risk_category=reason.risk_category,
                row_suppressed_count=sum((row, item) in suppressed for item in model.col_keys),
                col_suppressed_count=sum((item, col) in suppressed for item in model.row_keys),
                directly_inferable=coordinate in deduced,
                deduced_via=("row/column margin cascade",) if coordinate in deduced else (),
            )
        )
    return VerifyResult(
        passed=all(check.passed for check in checks),
        checks=tuple(checks),
        evidence=tuple(evidence),
    )
