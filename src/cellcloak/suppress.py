"""Deterministic primary and complementary cell suppression."""

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


def _candidate_key(
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
    return min(candidates, key=lambda item: _candidate_key(model, spec, item), default=None)


def suppress_table(
    model: TableModel,
    spec: TableSpec,
    assessment: RiskAssessment,
    privacy: PrivacyPolicy,
) -> SuppressionResult:
    suppressed: dict[tuple[str, str], SuppressionReason] = {}
    finding_by_coord = {finding.coordinate: finding for finding in assessment.findings}
    for coordinate in sorted(assessment.primary_coords):
        finding = finding_by_coord[coordinate]
        suppressed[coordinate] = SuppressionReason(
            kind=SUPPRESSION_PRIMARY,
            risk_category=finding.category,
            detail=f"primary rule: {finding.reason}",
            round_no=0,
        )
    block_reasons: list[BlockReason] = []
    rounds = 0

    def too_large(current: float) -> bool:
        return current > privacy.max_suppression_ratio + 1e-12

    def add_secondary(coordinate: tuple[str, str], trigger: str, round_no: int) -> None:
        finding = finding_by_coord[coordinate]
        suppressed[coordinate] = SuppressionReason(
            kind=SUPPRESSION_SECONDARY,
            risk_category=finding.category or RISK_SAFE,
            detail=f"complementary suppression for {trigger}",
            round_no=round_no,
        )

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
            additions = 0
            impossible: list[BlockReason] = []
            # Rows and columns are scanned in declared order, and each chosen
            # cell is applied immediately so one choice can close both its row
            # and its column before more suppression is added.
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
                    add_secondary(candidate, f"row {row!r}", round_no)
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
                    add_secondary(candidate, f"column {col!r}", round_no)
                    additions += 1
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

    ratio, count_ratio, row_ratio, col_ratio = _ratios(model, suppressed)
    primary_count = sum(reason.kind == SUPPRESSION_PRIMARY for reason in suppressed.values())
    secondary_count = sum(reason.kind == SUPPRESSION_SECONDARY for reason in suppressed.values())
    blocked = bool(block_reasons)
    return SuppressionResult(
        table_id=model.table_id,
        suppressed=suppressed,
        primary_count=primary_count,
        secondary_count=secondary_count,
        rounds=rounds,
        blocked=blocked,
        block_reasons=tuple(block_reasons),
        suppression_ratio=ratio,
        suppressed_count_sum_ratio=count_ratio,
        max_row_suppression_ratio=row_ratio,
        max_col_suppression_ratio=col_ratio,
    )
