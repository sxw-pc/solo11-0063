"""Cross-table joint review for explicitly declared shared-cell relations.

Strategy (also recorded in every report):

``local_then_propagation_closure``
    1. Each table is first solved locally with the unchanged single-table
       solver, so a contract without shared relations is byte-for-byte
       unaffected.
    2. Every declared shared relation is then closed as an all-or-nothing
       alias: when any member cell is suppressed, every other member of the
       relation is suppressed in its own table (primary propagation for a
       primary source, secondary propagation for a secondary source).
    3. Each table that gained a propagated cell is re-closed on its own row
       and column margins.  Those complementary choices can themselves be
       members of a shared relation, which triggers another propagation
       round.  The loop runs to a fixed point, and the complete trigger chain
       is recorded.
    4. Per-table suppression-ratio limits stay exactly as written in the
       contract.  If joint propagation or its closure pushes any member table
       over its own limit, every table in that relation is blocked.

The alternative "solve jointly over all shared relations and margins at
once" would pick a different secondary set; this implementation deliberately
keeps the deterministic single-table ordering and adds closure on top, and
says so in the report instead of presenting the result as a global optimum.
"""

from __future__ import annotations

from dataclasses import dataclass

from .contract import Contract, SharedGroup
from .model import (
    RISK_SAFE,
    SUPPRESSION_PRIMARY,
    SUPPRESSION_SECONDARY,
    SuppressionReason,
    TableModel,
)
from .risk import RiskAssessment
from .suppress import (
    BlockReason,
    SuppressionResult,
    close_complementary,
    ratio_stats,
    suppress_table,
    summarize,
)

STRATEGY = "local_then_propagation_closure"

JOINT_RATIO_EXCEEDED = "E_JOINT_SUPPRESSION_RATIO_EXCEEDED"
JOINT_MAX_ROUNDS = "E_JOINT_SUPPRESSION_MAX_ROUNDS"
JOINT_GROUP_BLOCKED = "E_JOINT_GROUP_BLOCKED"
JOINT_ALIAS_EXPOSURE = "E_JOINT_ALIAS_EXPOSURE"


@dataclass(frozen=True)
class JointEvent:
    """One edge of the cross-table trigger chain."""

    round_no: int
    kind: str
    table_id: str
    row: str
    col: str
    group: str | None = None
    source_table_id: str | None = None
    source_row: str | None = None
    source_col: str | None = None
    source_kind: str | None = None
    detail: str = ""

    def as_record(self) -> dict[str, object]:
        record: dict[str, object] = {
            "round": self.round_no,
            "kind": self.kind,
            "table_id": self.table_id,
            "row": self.row,
            "col": self.col,
        }
        if self.group is not None:
            record["group"] = self.group
        if self.source_table_id is not None:
            record["source"] = {
                "table_id": self.source_table_id,
                "row": self.source_row,
                "col": self.source_col,
                "suppression_type": self.source_kind,
            }
        if self.detail:
            record["detail"] = self.detail
        return record


@dataclass
class GroupOutcome:
    name: str
    table_ids: tuple[str, ...]
    status: str
    events: tuple[JointEvent, ...] = ()
    block_reasons: tuple[dict[str, str], ...] = ()

    def as_record(self) -> dict[str, object]:
        return {
            "name": self.name,
            "table_ids": list(self.table_ids),
            "status": self.status,
            "trigger_chain": [event.as_record() for event in self.events],
            "block_reasons": list(self.block_reasons),
        }


@dataclass
class JointOutcome:
    suppressions: dict[str, SuppressionResult]
    groups: tuple[GroupOutcome, ...]
    events: tuple[JointEvent, ...]
    strategy: str = STRATEGY

    @property
    def blocked_tables(self) -> tuple[str, ...]:
        return tuple(
            table_id for table_id, result in self.suppressions.items() if result.blocked
        )


def _group_trigger(
    round_no: int,
    group: SharedGroup,
    source_table: str,
    source_row: str,
    source_col: str,
    source_kind: str,
    target_table: str,
    target_row: str,
    target_col: str,
) -> JointEvent:
    return JointEvent(
        round_no=round_no,
        kind="shared_propagation",
        group=group.name,
        table_id=target_table,
        row=target_row,
        col=target_col,
        source_table_id=source_table,
        source_row=source_row,
        source_col=source_col,
        source_kind=source_kind,
        detail=(
            f"{source_kind} suppression of {source_table}:{source_row}/{source_col} "
            f"propagates through shared relation {group.name!r}"
        ),
    )


def _events_for_group(group: SharedGroup, events: list[JointEvent]) -> tuple[JointEvent, ...]:
    selected = []
    for event in events:
        if event.group == group.name:
            selected.append(event)
            continue
        if event.table_id in group.table_ids and group.has(event.table_id, event.row, event.col):
            selected.append(event)
    return tuple(selected)


def joint_suppress(
    contract: Contract,
    models: dict[str, TableModel],
    assessments: dict[str, RiskAssessment],
) -> JointOutcome:
    """Solve every table locally, then close declared shared relations."""

    local = {
        spec.table_id: suppress_table(
            models[spec.table_id], spec, assessments[spec.table_id], contract.privacy
        )
        for spec in contract.tables
    }
    maps: dict[str, dict[tuple[str, str], SuppressionReason]] = {
        table_id: dict(result.suppressed) for table_id, result in local.items()
    }
    rounds_used: dict[str, int] = {
        table_id: result.rounds for table_id, result in local.items()
    }
    joint_blocks: dict[str, list[BlockReason]] = {table_id: [] for table_id in local}
    events: list[JointEvent] = []
    findings = {
        table_id: {finding.coordinate: finding for finding in assessment.findings}
        for table_id, assessment in assessments.items()
    }
    # A declared structural zero is intentionally published as 0.  If such a
    # cell is an alias of a hidden cell, the release already reveals that
    # hidden value, so it must be treated as a published sibling during
    # propagation instead of being silently ignored.
    public_zeros: dict[str, frozenset[tuple[str, str]]] = {
        table_id: assessment.public_zero_coords
        for table_id, assessment in assessments.items()
    }
    blocked_tables: set[str] = {table_id for table_id, result in local.items() if result.blocked}

    def mark_blocked(table_id: str, reason: BlockReason) -> None:
        joint_blocks[table_id].append(reason)
        blocked_tables.add(table_id)

    def ratio_exceeded(table_id: str) -> bool:
        ratio, _, _, _ = ratio_stats(models[table_id], maps[table_id])
        return ratio > contract.privacy.max_suppression_ratio + 1e-12

    max_rounds = contract.privacy.max_suppression_rounds
    exhausted = False

    # A declared structural zero is published as a literal 0.  If it aliases a
    # cell that is (or becomes) suppressed elsewhere, the release hands the
    # hidden value out directly.  Such a relation cannot be made safe by
    # adding more suppression, so it is blocked before propagation starts.
    for group in contract.shared_cells:
        published_zero_members = [
            cell
            for cell in group.cells
            if (cell.row, cell.col) in public_zeros.get(cell.table_id, frozenset())
            and (cell.row, cell.col) not in maps[cell.table_id]
        ]
        hidden_members = [
            cell
            for cell in group.cells
            if (cell.row, cell.col) in maps[cell.table_id]
        ]
        if published_zero_members and hidden_members:
            for table_id in group.table_ids:
                mark_blocked(
                    table_id,
                    BlockReason(
                        JOINT_ALIAS_EXPOSURE,
                        "shared relation publishes a structural zero next to a "
                        "suppressed alias, exposing the hidden value",
                    ),
                )
            events.append(
                JointEvent(
                    round_no=0,
                    kind="alias_exposure",
                    group=group.name,
                    table_id=published_zero_members[0].table_id,
                    row=published_zero_members[0].row,
                    col=published_zero_members[0].col,
                    detail=(
                        f"declared structural zero {published_zero_members[0].table_id}:"
                        f"{published_zero_members[0].row}/{published_zero_members[0].col} "
                        f"aliases suppressed cell "
                        f"{hidden_members[0].table_id}:{hidden_members[0].row}/"
                        f"{hidden_members[0].col}"
                    ),
                )
            )

    for round_no in range(1, max_rounds + 1):
        additions = 0
        touched: set[str] = set()
        # (1) Propagate suppressed aliases through every declared relation.
        for group in contract.shared_cells:
            if any(table_id in blocked_tables for table_id in group.table_ids):
                continue
            hidden = [
                cell
                for cell in group.cells
                if (cell.row, cell.col) in maps[cell.table_id]
            ]
            if not hidden:
                continue
            source = hidden[0]
            source_reason = maps[source.table_id][(source.row, source.col)]
            target_kind = (
                SUPPRESSION_PRIMARY
                if any(
                    maps[cell.table_id][(cell.row, cell.col)].kind == SUPPRESSION_PRIMARY
                    for cell in hidden
                )
                else SUPPRESSION_SECONDARY
            )
            for cell in group.cells:
                if (cell.row, cell.col) in maps[cell.table_id]:
                    continue
                finding = findings[cell.table_id].get((cell.row, cell.col))
                risk_category = getattr(finding, "category", RISK_SAFE) or RISK_SAFE
                maps[cell.table_id][(cell.row, cell.col)] = SuppressionReason(
                    kind=target_kind,
                    risk_category=risk_category,
                    detail=(
                        f"cross-table propagation via shared relation {group.name!r} "
                        f"from {source.table_id}:{source.row}/{source.col}"
                    ),
                    round_no=round_no,
                    propagated=True,
                )
                touched.add(cell.table_id)
                events.append(
                    _group_trigger(
                        round_no,
                        group,
                        source.table_id,
                        source.row,
                        source.col,
                        source_reason.kind,
                        cell.table_id,
                        cell.row,
                        cell.col,
                    )
                )
                additions += 1
        # (2) A propagated cell can push a table over its own ratio limit.
        for spec in contract.tables:
            if spec.table_id in blocked_tables:
                continue
            if ratio_exceeded(spec.table_id):
                mark_blocked(
                    spec.table_id,
                    BlockReason(
                        JOINT_RATIO_EXCEEDED,
                        "cross-table propagation exceeds this table's "
                        "max_suppression_ratio",
                    ),
                )
        # (3) Re-close row and column margins in every table that received a
        # propagated cell.  Untouched tables keep their local result exactly.
        for spec in contract.tables:
            table_id = spec.table_id
            if table_id in blocked_tables or table_id not in touched:
                continue
            budget = max_rounds - rounds_used[table_id]
            closure = close_complementary(
                models[table_id],
                spec,
                maps[table_id],
                assessments[table_id],
                contract.privacy,
                start_round=rounds_used[table_id],
                round_budget=budget,
            )
            rounds_used[table_id] += closure.rounds
            for reason in closure.block_reasons:
                code = {
                    "E_SUPPRESSION_RATIO_EXCEEDED": JOINT_RATIO_EXCEEDED,
                    "E_SUPPRESSION_MAX_ROUNDS": JOINT_MAX_ROUNDS,
                }.get(reason.code, reason.code)
                mark_blocked(table_id, BlockReason(code, reason.detail))
            for closure_round, coordinate in closure.additions:
                group = contract.group_of(table_id, coordinate[0], coordinate[1])
                events.append(
                    JointEvent(
                        round_no=round_no,
                        kind="complementary_suppression",
                        group=group.name if group is not None else None,
                        table_id=table_id,
                        row=coordinate[0],
                        col=coordinate[1],
                        detail=(
                            "complementary suppression selected in closure round "
                            f"{closure_round} while closing margins after "
                            "cross-table propagation"
                        ),
                    )
                )
                additions += 1
        if additions == 0:
            break
    else:
        exhausted = True
        for group in contract.shared_cells:
            for table_id in group.table_ids:
                mark_blocked(
                    table_id,
                    BlockReason(
                        JOINT_MAX_ROUNDS,
                        "cross-table propagation and complementary closure did "
                        "not stabilise within max_suppression_rounds",
                    ),
                )

    # Any table blocked by a joint constraint blocks its whole relation.  A
    # locally blocked member also makes the relation unpublishable, so its
    # siblings are marked as group-blocked rather than being reported as
    # individually verified.
    group_outcomes: list[GroupOutcome] = []
    for group in contract.shared_cells:
        joint_members = [
            table_id for table_id in group.table_ids if joint_blocks[table_id]
        ]
        locally_blocked = [
            table_id for table_id in group.table_ids if local[table_id].blocked
        ]
        offending = sorted(set(joint_members) | set(locally_blocked))
        if offending:
            for table_id in group.table_ids:
                if any(
                    reason.code == JOINT_GROUP_BLOCKED for reason in joint_blocks[table_id]
                ):
                    continue
                mark_blocked(
                    table_id,
                    BlockReason(
                        JOINT_GROUP_BLOCKED,
                        "shared relation blocked because member table(s) "
                        + ", ".join(offending)
                        + " failed a per-table or joint safety constraint",
                    ),
                )
        group_events = _events_for_group(group, events)
        group_reason_records: list[dict[str, str]] = []
        for table_id in group.table_ids:
            for reason in local[table_id].block_reasons:
                group_reason_records.append(
                    {
                        "table_id": table_id,
                        "code": reason.code,
                        "detail": f"local table constraint: {reason.detail}",
                    }
                )
            for reason in joint_blocks[table_id]:
                group_reason_records.append(
                    {
                        "table_id": table_id,
                        "code": reason.code,
                        "detail": reason.detail,
                    }
                )
        blocked_group = bool(locally_blocked or joint_members or exhausted)
        group_outcomes.append(
            GroupOutcome(
                name=group.name,
                table_ids=group.table_ids,
                status="blocked" if blocked_group else "ok",
                events=group_events,
                block_reasons=tuple(group_reason_records),
            )
        )

    suppressions: dict[str, SuppressionResult] = {}
    for spec in contract.tables:
        table_id = spec.table_id
        reasons = list(local[table_id].block_reasons) + joint_blocks[table_id]
        suppressions[table_id] = summarize(
            models[table_id],
            maps[table_id],
            rounds=rounds_used[table_id],
            block_reasons=reasons,
        )
    return JointOutcome(
        suppressions=suppressions,
        groups=tuple(group_outcomes),
        events=tuple(events),
    )
