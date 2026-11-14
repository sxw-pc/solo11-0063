"""Deterministic small-cell risk classification."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from .contract import PrivacyPolicy, TableSpec
from .errors import ContractError
from .model import (
    RISK_BLOCKED,
    RISK_FORCED,
    RISK_LABELS,
    RISK_SAFE,
    RISK_SAMPLING_ZERO,
    RISK_SMALL,
    TableModel,
    cell_fingerprint,
)

RISK_SEVERITY = (RISK_FORCED, RISK_SMALL, RISK_SAMPLING_ZERO, RISK_SAFE)


@dataclass(frozen=True)
class RiskFinding:
    row: str
    col: str
    count: int
    category: str
    reason: str
    fingerprint: str

    @property
    def coordinate(self) -> tuple[str, str]:
        return (self.row, self.col)


@dataclass(frozen=True)
class RiskAssessment:
    table_id: str
    findings: tuple[RiskFinding, ...]
    counts: Mapping[str, int]
    primary_coords: frozenset[tuple[str, str]]

    def finding(self, row: str, col: str) -> RiskFinding:
        for finding in self.findings:
            if finding.row == row and finding.col == col:
                return finding
        raise KeyError((row, col))

    def risk_level(self, blocked: bool = False) -> str:
        if blocked:
            return RISK_BLOCKED
        for category in RISK_SEVERITY:
            if self.counts.get(category, 0):
                return category
        return RISK_SAFE


def _validate_structural_zeros(model: TableModel, privacy: PrivacyPolicy) -> None:
    for ref in sorted(privacy.structural_zero_cells, key=lambda item: (item.table_id, item.row, item.col)):
        if ref.table_id != model.table_id:
            continue
        if model.count(ref.row, ref.col) != 0:
            raise ContractError(
                "E_STRUCTURAL_ZERO_VALUE",
                "declared structural zero is not zero in the source table",
                table_id=model.table_id,
                cell=(ref.row, ref.col),
                field="privacy.structural_zero_cells",
            )


def classify(
    model: TableModel,
    spec: TableSpec,
    privacy: PrivacyPolicy,
) -> RiskAssessment:
    _validate_structural_zeros(model, privacy)
    always = {(ref.row, ref.col) for ref in privacy.always_suppress if ref.table_id == model.table_id}
    structural = {
        (ref.row, ref.col)
        for ref in privacy.structural_zero_cells
        if ref.table_id == model.table_id
    }
    counts = {label: 0 for label in RISK_LABELS}
    findings: list[RiskFinding] = []
    primary: set[tuple[str, str]] = set()

    for cell in model.cells():
        coordinate = (cell.row, cell.col)
        if coordinate in always:
            category = RISK_FORCED
            reason = "forced by publication contract"
        elif cell.count == 0 and privacy.zero_policy == "structural" and coordinate in structural:
            category = RISK_SAFE
            reason = "declared structural zero"
        elif cell.count == 0:
            category = RISK_SAMPLING_ZERO
            reason = "zero count treated as sampling zero"
        elif cell.count < privacy.min_count:
            category = RISK_SMALL
            reason = "count is below the small-count threshold"
        else:
            category = RISK_SAFE
            reason = "passes the primary disclosure rule"
        if category != RISK_SAFE:
            primary.add(coordinate)
        counts[category] += 1
        findings.append(
            RiskFinding(
                row=cell.row,
                col=cell.col,
                count=cell.count,
                category=category,
                reason=reason,
                fingerprint=cell_fingerprint(model.table_id, cell.row, cell.col),
            )
        )
    counts[RISK_BLOCKED] = 0
    return RiskAssessment(
        table_id=model.table_id,
        findings=tuple(findings),
        counts=counts,
        primary_coords=frozenset(primary),
    )
