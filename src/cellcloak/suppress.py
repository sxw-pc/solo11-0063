"""Deterministic primary and complementary cell suppression.

The single-table solver remains the authority for a table that is not linked
to any other table.  Its primitives are exposed so the cross-table solver can
reuse the exact same candidate ordering and scan semantics when it propagates
a suppression through an explicitly declared shared-cell relation.
"""

from __future__ import annotations

from dataclasses import dataclass

from .contract import PrivacyPolicy, TableSpec
from .model import (
    RISK_SAFE,
    SUPPRESSION_PRIMARY,
    SUPPRESSION_SECONDARY,
    SuppressionReason,
    TableModel,
)
from .risk import RiskAssessment


@dataclass(frozen=True)
class BlockReason:
    code: str
    detail: str


@dataclass
class SuppressionResult:
    table_id: str
    suppressed: dict[tuple[str, str], SuppressionReason]
    primary_count: int
    secondary_count: int
    rounds: int
    blocked: bool
    block_reasons: tuple[BlockReason, ...]
    suppression_ratio: float
    suppressed_count_sum_ratio: float
    max_row_suppression_ratio: float
    max_col_suppression_ratio: float


def _ratios(
    model: TableModel, suppressed: dict[tuple[str, str], SuppressionReason]
) -> tuple[float, float, float, float]:
    total_cells = model.cell_count()
    cell_ratio = len(suppressed) / total_cells if total_cells else 0.0
    total_count = model.total()
    hidden_sum = sum(model.count(row, col) for row, col in suppressed)
    count_ratio = hidden_sum / total_count if total_count else 0.0
    row_ratio = max(
        (sum((row, col) in suppressed for col in model.col_keys) / len(model.col_keys)
         for row in model.row_keys),
        default=0.0,
    )
    col_ratio = max(
        (sum((row, col) in suppressed for row in model.row_keys) / len(model.row_keys)
         for col in model.col_keys),
        default=0.0,
    )
    return cell_ratio, count_ratio, row_ratio, col_ratio


def ratio_stats(
    model: TableModel, suppressed: dict[tuple[str, str], SuppressionReason]
) -> tuple[float, float, float, float]:
    """Public accessor used by the cross-table solver and verifier."""

    return _ratios(model, suppressed)


def summarize(
    model: TableModel,
    suppressed: dict[tuple[str, str], SuppressionReason],
    *,
    rounds: int,
    block_reasons: list[BlockReason],
) -> SuppressionResult:
    """Rebuild a result object from a possibly propagated suppression map."""

    ratio, count_ratio, row_ratio, col_ratio = _ratios(model, suppressed)
    primary_count = sum(reason.kind == SUPPRESSION_PRIMARY for reason in suppressed.values())
    secondary_count = sum(reason.kind == SUPPRESSION_SECONDARY for reason in suppressed.values())
    return SuppressionResult(
        table_id=model.table_id,
        suppressed=dict(suppressed),
        primary_count=primary_count,
        secondary_count=secondary_count,
        rounds=rounds,
        blocked=bool(block_reasons),
        block_reasons=tuple(block_reasons),
        suppression_ratio=ratio,
        suppressed_count_sum_ratio=count_ratio,
        max_row_suppression_ratio=row_ratio,
        max_col_suppression_ratio=col_ratio,
    )


def candidate_key(
    model: TableModel, spec: TableSpec, coordinate: tuple[str, str]
) -> tuple[float, float, str, str]:
    row, col = coordinate
    return (
        float(model.count(row, col)),
        spec.loss_weights.get(coordinate, 1.0),
        row,
        col,
    )


def _best_candidate(
    model: TableModel,
    spec: TableSpec,
    suppressed: dict[tuple[str, str], SuppressionReason],
    row: str | None = None,
    col: str | None = None,
) -> tuple[str, str] | None:
    if row is not None:
        candidates = [(row, item) for item in model.col_keys if (row, item) not in suppressed]
    elif col is not None:
        candidates = [(item, col) for item in model.row_keys if (item, col) not in suppressed]
    else:
        candidates = []
    return min(candidates, key=lambda item: candidate_key(model, spec, item), default=None)


def primary_map(assessment: RiskAssessment) -> dict[tuple[str, str], SuppressionReason]:
    """Build the primary suppression map from an independent risk assessment."""

    finding_by_coord = {finding.coordinate: finding for finding in assessment.findings}
    suppressed: dict[tuple[str, str], SuppressionReason] = {}
    for coordinate in sorted(assessment.primary_coords):
        finding = finding_by_coord[coordinate]
        suppressed[coordinate] = SuppressionReason(
            kind=SUPPRESSION_PRIMARY,
            risk_category=finding.category,
            detail=f"primary rule: {finding.reason}",
            round_no=0,
        )
    return suppressed


def secondary_reason(
    finding_by_coord: dict[tuple[str, str], object],
    coordinate: tuple[str, str],
    trigger: str,
    round_no: int,
) -> SuppressionReason:
    """A complementary suppression reason using the target cell's own category."""

    finding = finding_by_coord[coordinate]
    return SuppressionReason(
        kind=SUPPRESSION_SECONDARY,
        risk_category=getattr(finding, "category", RISK_SAFE) or RISK_SAFE,
        detail=f"complementary suppression for {trigger}",
        round_no=round_no,
    )


def complementary_pass(
    model: TableModel,
    spec: TableSpec,
    suppressed: dict[tuple[str, str], SuppressionReason],
    finding_by_coord: dict[tuple[str, str], object],
    round_no: int,
) -> tuple[int, list[BlockReason]]:
    """Run one ordered row-then-column complementary scan.

    Every chosen cell is applied immediately, exactly as in the single-table
    solver, so one choice can close both its row and its column before more
    suppression is added.  Returns the number of additions and any impossible
    margins encountered during the pass.
    """

    additions = 0
    impossible: list[BlockReason] = []
    for row in model.row_keys:
        hidden = [col for col in model.col_keys if (row, col) in suppressed]
        if len(hidden) != 1:
            continue
        candidate = _best_candidate(model, spec, suppressed, row=row)
        if candidate is None:
            impossible.append(
                BlockReason("E_SUPPRESSION_NO_CANDIDATE", f"row {row!r} has no visible cell to suppress")
            )
        else:
            suppressed[candidate] = secondary_reason(
                finding_by_coord, candidate, f"row {row!r}", round_no
            )
            additions += 1
    for col in model.col_keys:
        hidden = [row for row in model.row_keys if (row, col) in suppressed]
        if len(hidden) != 1:
            continue
        candidate = _best_candidate(model, spec, suppressed, col=col)
        if candidate is None:
            impossible.append(
                BlockReason("E_SUPPRESSION_NO_CANDIDATE", f"column {col!r} has no visible cell to suppress")
            )
        else:
            suppressed[candidate] = secondary_reason(
                finding_by_coord, candidate, f"column {col!r}", round_no
            )
            additions += 1
    return additions, impossible


def has_single_unknown(model: TableModel, suppressed: dict[tuple[str, str], SuppressionReason]) -> bool:
    """True when any row or column exposes exactly one suppressed cell."""

    for row in model.row_keys:
        if sum((row, col) in suppressed for col in model.col_keys) == 1:
            return True
    for col in model.col_keys:
        if sum((row, col) in suppressed for row in model.row_keys) == 1:
            return True
    return False


@dataclass(frozen=True)
class ComplementClosure:
    rounds: int
    block_reasons: tuple[BlockReason, ...]
    additions: tuple[tuple[int, tuple[str, str]], ...]


def close_complementary(
    model: TableModel,
    spec: TableSpec,
    suppressed: dict[tuple[str, str], SuppressionReason],
    assessment: RiskAssessment,
    privacy: PrivacyPolicy,
    *,
    start_round: int = 0,
    round_budget: int | None = None,
) -> ComplementClosure:
    """Close single-unknown margins from an existing suppression map.

    Used after cross-table propagation adds aliases to a table.  The scan and
    ratio semantics match ``suppress_table``; only the starting point and the
    remaining per-table round budget differ.
    """

    budget = privacy.max_suppression_rounds if round_budget is None else round_budget
    finding_by_coord = {finding.coordinate: finding for finding in assessment.findings}
    block_reasons: list[BlockReason] = []
    additions: list[tuple[int, tuple[str, str]]] = []
    rounds_used = 0

    def too_large() -> bool:
        ratio, _, _, _ = _ratios(model, suppressed)
        return ratio > privacy.max_suppression_ratio + 1e-12

    if not has_single_unknown(model, suppressed):
        return ComplementClosure(0, (), ())
    if budget <= 0:
        return ComplementClosure(
            0,
            (
                BlockReason(
                    "E_SUPPRESSION_MAX_ROUNDS",
                    "cross-table propagation consumed max_suppression_rounds before margins stabilised",
                ),
            ),
            (),
        )

    for offset in range(1, budget + 1):
        rounds_used = offset
        before = set(suppressed)
        impossible: list[BlockReason] = []
        added_count, impossible = complementary_pass(
            model, spec, suppressed, finding_by_coord, start_round + offset
        )
        for coordinate in sorted(set(suppressed) - before):
            additions.append((start_round + offset, coordinate))
        if impossible:
            block_reasons.extend(impossible)
            break
        if too_large():
            block_reasons.append(
                BlockReason(
                    "E_SUPPRESSION_RATIO_EXCEEDED",
                    "suppression exceeds max_suppression_ratio while closing visible margins",
                )
            )
            break
        if added_count == 0:
            break
    else:
        block_reasons.append(
            BlockReason(
                "E_SUPPRESSION_MAX_ROUNDS",
                "complementary suppression did not stabilise within max_suppression_rounds",
            )
        )
    return ComplementClosure(rounds_used, tuple(block_reasons), tuple(additions))


def suppress_table(
    model: TableModel,
    spec: TableSpec,
    assessment: RiskAssessment,
    privacy: PrivacyPolicy,
) -> SuppressionResult:
    suppressed = primary_map(assessment)
    finding_by_coord = {finding.coordinate: finding for finding in assessment.findings}
    block_reasons: list[BlockReason] = []
    rounds = 0

    def too_large(current: float) -> bool:
        return current > privacy.max_suppression_ratio + 1e-12

    ratio, count_ratio, row_ratio, col_ratio = _ratios(model, suppressed)
    if too_large(ratio):
        block_reasons.append(
            BlockReason(
                "E_SUPPRESSION_RATIO_EXCEEDED",
                "primary suppression already exceeds max_suppression_ratio",
            )
        )
    else:
        for round_no in range(1, privacy.max_suppression_rounds + 1):
            rounds = round_no
            impossible: list[BlockReason] = []
            # Rows and columns are scanned in declared order, and each chosen
            # cell is applied immediately so one choice can close both its row
            # and its column before more suppression is added.
            additions, impossible = complementary_pass(
                model, spec, suppressed, finding_by_coord, round_no
            )
            ratio, count_ratio, row_ratio, col_ratio = _ratios(model, suppressed)
            if impossible:
                block_reasons.extend(impossible)
                break
            if too_large(ratio):
                block_reasons.append(
                    BlockReason(
                        "E_SUPPRESSION_RATIO_EXCEEDED",
                        "suppression exceeds max_suppression_ratio while closing visible margins",
                    )
                )
                break
            if additions == 0:
                break
        else:
            block_reasons.append(
                BlockReason(
                    "E_SUPPRESSION_MAX_ROUNDS",
                    "complementary suppression did not stabilise within max_suppression_rounds",
                )
            )

    return summarize(model, suppressed, rounds=rounds, block_reasons=block_reasons)
