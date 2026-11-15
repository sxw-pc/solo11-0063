"""Independent margin and single-unknown reconstruction checks.

``verify_table`` is the unchanged single-table verifier.  ``verify_joint``
adds an independent cross-table pass over explicitly declared shared-cell
relations: aliases are treated as one unknown value, public aliases expose
their hidden counterparts immediately, and margin deductions in any member
table cascade through the whole declared group.  It never trusts the
cross-table solver's trigger chain.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from .contract import SharedGroup
from .model import (
    SUPPRESSION_PRIMARY,
    SUPPRESSION_SECONDARY,
    SuppressionReason,
    TableModel,
    cell_fingerprint,
)

Coordinate = tuple[str, str]
Node = tuple[str, str, str]  # (table_id, row, col)


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


@dataclass(frozen=True)
class Deduction:
    table_id: str
    row: str
    col: str
    via: str


@dataclass(frozen=True)
class JointVerifyResult:
    passed: bool
    per_table: Mapping[str, VerifyResult]
    checks: tuple[CheckResult, ...]
    deductions: tuple[Deduction, ...]
    evidence: tuple[CellEvidence, ...]

    @property
    def failed_checks(self) -> tuple[CheckResult, ...]:
        return tuple(check for check in self.checks if not check.passed)


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


@dataclass(frozen=True)
class CrossCellEvidence:
    table_id: str
    row: str
    col: str
    fingerprint: str
    suppression_type: str
    risk_category: str
    recovered: bool
    recovered_via: tuple[str, ...]


def _cross_table_deductions(
    groups: Sequence[SharedGroup],
    models: Mapping[str, TableModel],
    suppressions: Mapping[str, Mapping[Coordinate, SuppressionReason]],
    public_zeros: Mapping[str, Iterable[Coordinate]] | None = None,
) -> dict[Node, tuple[str, ...]]:
    """Independent alias + margin closure over the whole release.

    Returns every suppressed node the public release can recover, with the
    reason chain that recovered it.  The routine re-derives everything from
    the published matrix and the declared relation; it never imports the
    suppression solver's cross-table events.
    """

    published_zeros = {
        (table_id, row, col)
        for table_id, coordinates in (public_zeros or {}).items()
        for (row, col) in coordinates
        if (row, col) not in suppressions.get(table_id, {})
    }
    unknown: set[Node] = {
        (table_id, row, col)
        for table_id, hidden in suppressions.items()
        for (row, col) in hidden
        if (table_id, row, col) not in published_zeros
    }
    recovered: dict[Node, tuple[str, ...]] = {}

    def recover(node: Node, via: str) -> None:
        if node not in unknown:
            return
        unknown.remove(node)
        recovered[node] = (via,)

    # A structural zero is public even though its risk label is `safe`.
    for node in published_zeros:
        if node in unknown:
            recover(node, "declared structural zero is published as 0")

    def alias_step() -> bool:
        progress = False
        for group in groups:
            members = [(cell.table_id, cell.row, cell.col) for cell in group.cells]
            names = [
                f"{table_id}:{row}/{col}"
                for table_id, row, col in members
                if (table_id, row, col) not in unknown
            ]
            for node in members:
                if node not in unknown:
                    continue
                source = next(
                    (name for name in names if name != f"{node[0]}:{node[1]}/{node[2]}"),
                    None,
                )
                if source is None:
                    continue
                recover(
                    node,
                    f"shared relation {group.name!r}: {source} is published",
                )
                progress = True
        return progress

    def margin_step() -> bool:
        progress = False
        for table_id, model in models.items():
            for row in model.row_keys:
                hidden = [
                    col
                    for col in model.col_keys
                    if (table_id, row, col) in unknown
                ]
                if len(hidden) == 1:
                    recover(
                        (table_id, row, hidden[0]),
                        f"row margin of table {table_id!r} leaves one unknown",
                    )
                    progress = True
            for col in model.col_keys:
                hidden = [
                    row
                    for row in model.row_keys
                    if (table_id, row, col) in unknown
                ]
                if len(hidden) == 1:
                    recover(
                        (table_id, hidden[0], col),
                        f"column margin of table {table_id!r} leaves one unknown",
                    )
                    progress = True
        return progress

    # A published alias exposes every hidden sibling immediately.
    alias_step()
    changed = True
    while changed:
        changed = margin_step()
        if alias_step():
            changed = True
    return recovered


def verify_joint(
    groups: Sequence[SharedGroup],
    models: Mapping[str, TableModel],
    suppressions: Mapping[str, Mapping[Coordinate, SuppressionReason]],
    *,
    primary_coords: Mapping[str, Iterable[Coordinate]] | None = None,
    max_suppression_ratios: Mapping[str, float] | None = None,
    public_zero_coords: Mapping[str, Iterable[Coordinate]] | None = None,
) -> JointVerifyResult:
    """Verify the whole declared release, not each table in isolation."""

    primaries = primary_coords or {}
    ratios = max_suppression_ratios or {}
    public_zeros = public_zero_coords or {}
    per_table: dict[str, VerifyResult] = {}
    checks: list[CheckResult] = []
    for table_id, model in models.items():
        per_table[table_id] = verify_table(
            model,
            suppressions.get(table_id, {}),
            primary_coords=primaries.get(table_id, ()),
            max_suppression_ratio=ratios.get(table_id),
        )

    groups = tuple(groups or ())
    missing_groups = [
        group.name
        for group in groups
        if any(table_id not in models for table_id in group.table_ids)
    ]
    checks.append(
        CheckResult(
            "shared_relations_resolvable",
            not missing_groups,
            "all shared relations reference tables present in this release"
            if not missing_groups
            else "missing table(s) for shared relation(s): " + ", ".join(missing_groups),
        )
    )

    incomplete = [
        f"{group.name!r}"
        for group in groups
        if any(
            (cell.row, cell.col) in suppressions.get(cell.table_id, {})
            for cell in group.cells
        )
        and not all(
            (cell.row, cell.col) in suppressions.get(cell.table_id, {})
            for cell in group.cells
        )
    ]
    checks.append(
        CheckResult(
            "shared_suppression_completeness",
            not incomplete,
            "every shared relation is suppressed in all member tables or in none"
            if not incomplete
            else "shared relation(s) partially suppressed: " + ", ".join(incomplete),
        )
    )

    unknown_coordinates = [
        f"{cell.table_id}:{cell.row}/{cell.col}"
        for group in groups
        for cell in group.cells
        if cell.table_id in models
        and (
            cell.row not in models[cell.table_id].row_keys
            or cell.col not in models[cell.table_id].col_keys
        )
    ]
    checks.append(
        CheckResult(
            "shared_coordinates_in_range",
            not unknown_coordinates,
            "every declared shared coordinate exists in its restored table"
            if not unknown_coordinates
            else "shared coordinate(s) outside the restored matrix: "
            + ", ".join(unknown_coordinates),
        )
    )

    exposed = list(incomplete)
    checks.append(
        CheckResult(
            "shared_alias_exposure",
            not exposed,
            "no published alias exposes a suppressed sibling"
            if not exposed
            else "public alias exposes a hidden value in: " + ", ".join(exposed),
        )
    )

    published_zero = {
        (table_id, row, col)
        for table_id, coordinates in public_zeros.items()
        for (row, col) in coordinates
    }
    zero_exposures = [
        f"{group.name!r}"
        for group in groups
        if any(
            (cell.table_id, cell.row, cell.col) in published_zero
            for cell in group.cells
        )
        and any(
            (cell.table_id, cell.row, cell.col) in published_zero
            and (cell.row, cell.col) not in suppressions.get(cell.table_id, {})
            for cell in group.cells
        )
        and any(
            (cell.row, cell.col) in suppressions.get(cell.table_id, {})
            for cell in group.cells
        )
    ]
    checks.append(
        CheckResult(
            "shared_public_zero_exposure",
            not zero_exposures,
            "no published structural zero aliases a suppressed cell"
            if not zero_exposures
            else "published structural zero exposes a hidden alias in: "
            + ", ".join(zero_exposures),
        )
    )

    recovered = _cross_table_deductions(groups, models, suppressions, public_zeros)
    checks.append(
        CheckResult(
            "cross_table_reconstruction",
            not recovered,
            f"cross-table alias and margin closure recovered {len(recovered)} "
            "suppressed cell(s)",
        )
    )

    group_ratio_failures: list[str] = []
    for group in groups:
        for table_id in group.table_ids:
            limit = ratios.get(table_id)
            if limit is None or table_id not in models:
                continue
            model = models[table_id]
            hidden = suppressions.get(table_id, {})
            ratio = len(hidden) / model.cell_count() if model.cell_count() else 0.0
            if ratio > limit + 1e-12:
                group_ratio_failures.append(
                    f"{group.name!r}:{table_id} ({ratio:.4f} > {limit:.4f})"
                )
    checks.append(
        CheckResult(
            "shared_group_ratio_limits",
            not group_ratio_failures,
            "every member table stays within its own suppression-ratio limit"
            if not group_ratio_failures
            else "joint safety constraint exceeded: " + ", ".join(group_ratio_failures),
        )
    )

    evidence: list[CrossCellEvidence] = []
    for table_id, hidden in suppressions.items():
        model = models.get(table_id)
        if model is None:
            continue
        for coordinate in sorted(hidden):
            row, col = coordinate
            if row not in model.row_keys or col not in model.col_keys:
                continue
            reason = hidden[coordinate]
            node = (table_id, row, col)
            evidence.append(
                CrossCellEvidence(
                    table_id=table_id,
                    row=row,
                    col=col,
                    fingerprint=cell_fingerprint(table_id, row, col),
                    suppression_type=reason.kind,
                    risk_category=reason.risk_category,
                    recovered=node in recovered,
                    recovered_via=recovered.get(node, ()),
                )
            )

    deductions = tuple(
        Deduction(table_id=table_id, row=row, col=col, via=via[0])
        for (table_id, row, col), via in sorted(recovered.items())
    )
    return JointVerifyResult(
        passed=all(check.passed for check in checks)
        and all(result.passed for result in per_table.values()),
        per_table=per_table,
        checks=tuple(checks),
        deductions=deductions,
        evidence=tuple(evidence),
    )
