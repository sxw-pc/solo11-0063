"""Joint cross-table review: propagation, closure, blocking, and compatibility."""

import csv
import json

from cellcloak.contract import load_contract
from cellcloak.ingest import load_table
from cellcloak.joint import STRATEGY, joint_suppress
from cellcloak.ledger import Ledger
from cellcloak.model import SUPPRESSION_PRIMARY, SUPPRESSION_SECONDARY
from cellcloak.pipeline import execute_run, reverify_run
from cellcloak.risk import classify
from cellcloak.suppress import suppress_table
from cellcloak.verify import verify_joint

ROOT = __import__("pathlib").Path(__file__).resolve().parents[1]


def _solve(contract_path):
    contract = load_contract(contract_path)
    models = {spec.table_id: load_table(spec) for spec in contract.tables}
    assessments = {
        spec.table_id: classify(models[spec.table_id], spec, contract.privacy)
        for spec in contract.tables
    }
    outcome = joint_suppress(contract, models, assessments)
    verification = verify_joint(
        contract.shared_cells,
        models,
        {table_id: result.suppressed for table_id, result in outcome.suppressions.items()},
        primary_coords={
            table_id: assessment.primary_coords
            for table_id, assessment in assessments.items()
        },
        max_suppression_ratios={
            spec.table_id: contract.privacy.max_suppression_ratio for spec in contract.tables
        },
        public_zero_coords={
            table_id: assessment.public_zero_coords
            for table_id, assessment in assessments.items()
        },
    )
    return contract, models, assessments, outcome, verification


def test_shared_fixture_propagates_primary_alias(shared_contract):
    contract, _, assessments, outcome, verification = _solve(shared_contract)
    region = outcome.suppressions["sex_by_region"]
    district = outcome.suppressions["sex_by_district"]
    assert region.blocked is False
    assert district.blocked is False
    # The local solver already suppressed R1/C1 in the region table.
    assert assessments["sex_by_region"].primary_coords == {("R1", "C1")}
    assert region.suppressed[("R1", "C1")].kind == SUPPRESSION_PRIMARY
    # Its alias in the district table must be hidden as well, as a primary.
    assert district.suppressed[("R3", "C3")].kind == SUPPRESSION_PRIMARY
    assert district.suppressed[("R3", "C3")].propagated
    assert verification.passed


def test_secondary_alias_is_propagated_as_secondary(shared_contract):
    _, _, _, outcome, _ = _solve(shared_contract)
    region = outcome.suppressions["sex_by_region"]
    district = outcome.suppressions["sex_by_district"]
    # Region R2/C2 is complementary in the local solve and aliases district R1/C1.
    assert region.suppressed[("R2", "C2")].kind == SUPPRESSION_SECONDARY
    assert district.suppressed[("R1", "C1")].kind == SUPPRESSION_SECONDARY
    assert any(
        event.kind == "shared_propagation"
        and event.table_id == "sex_by_district"
        and (event.row, event.col) == ("R1", "C1")
        for event in outcome.events
    )


def test_joint_closure_respects_every_member_margin(shared_contract):
    _, models, _, outcome, verification = _solve(shared_contract)
    for table_id, result in outcome.suppressions.items():
        model = models[table_id]
        for row in model.row_keys:
            assert sum((row, col) in result.suppressed for col in model.col_keys) != 1
        for col in model.col_keys:
            assert sum((row, col) in result.suppressed for row in model.row_keys) != 1
    assert verification.passed


def test_propagation_can_trigger_complementary_closure_back_and_forth(shared_contract):
    _, _, _, outcome, _ = _solve(shared_contract)
    kinds = [event.kind for event in outcome.events]
    assert kinds.count("shared_propagation") >= 2
    assert kinds.count("complementary_suppression") >= 1
    # A closure in the district table feeds the district_alias relation, which
    # propagates back into the region table in a later solver round.
    back_edges = [
        event
        for event in outcome.events
        if event.kind == "shared_propagation"
        and event.source_table_id == "sex_by_district"
        and event.table_id == "sex_by_region"
    ]
    assert back_edges
    assert all(event.round_no >= 2 for event in back_edges)
    assert max(event.round_no for event in outcome.events) >= 2
    assert outcome.strategy == STRATEGY


def test_three_table_chain_propagates_transitively(contract_factory, multi_table_factory):
    path = multi_table_factory(
        {
            "a": {("R1", "C1"): 2},
            "b": {},
            "c": {},
        },
        shared=[
            {
                "name": "ab",
                "cells": [
                    {"table": "a", "row": "R1", "col": "C1"},
                    {"table": "b", "row": "R1", "col": "C1"},
                ],
            },
            {
                "name": "bc",
                "cells": [
                    {"table": "b", "row": "R1", "col": "C2"},
                    {"table": "c", "row": "R3", "col": "C3"},
                ],
            },
        ],
    )
    contract, _, _, outcome, verification = _solve(path)
    assert outcome.suppressions["b"].suppressed[("R1", "C1")].propagated
    assert outcome.suppressions["c"].suppressed[("R3", "C3")].propagated
    # The chain required propagating a secondary from b to c.
    chain = [
        event
        for event in outcome.events
        if event.kind == "shared_propagation" and event.group == "bc"
    ]
    assert chain
    assert chain[0].source_table_id == "b"
    assert verification.passed


def test_direct_cross_table_recovery_is_detected(shared_contract):
    contract, models, assessments, outcome, _ = _solve(shared_contract)
    suppressions = {
        table_id: dict(result.suppressed)
        for table_id, result in outcome.suppressions.items()
    }
    # Publish the region alias again while its sibling stays hidden: anyone
    # holding the release can read the hidden district value straight off.
    suppressions["sex_by_region"].pop(("R1", "C1"))
    verification = verify_joint(
        contract.shared_cells,
        models,
        suppressions,
        primary_coords={
            table_id: assessment.primary_coords
            for table_id, assessment in assessments.items()
        },
        max_suppression_ratios={
            spec.table_id: contract.privacy.max_suppression_ratio
            for spec in contract.tables
        },
    )
    assert not verification.passed
    failed = {check.name for check in verification.checks if not check.passed}
    assert "shared_suppression_completeness" in failed
    assert "shared_alias_exposure" in failed
    assert "cross_table_reconstruction" in failed
    recovered = {
        (item.table_id, item.row, item.col) for item in verification.deductions
    }
    assert ("sex_by_district", "R3", "C3") in recovered


def test_alias_transitive_inference_is_detected(shared_contract):
    contract, models, assessments, outcome, _ = _solve(shared_contract)
    suppressions = {
        table_id: dict(result.suppressed)
        for table_id, result in outcome.suppressions.items()
    }
    suppressions["sex_by_region"].pop(("R2", "C2"))
    verification = verify_joint(
        contract.shared_cells,
        models,
        suppressions,
        primary_coords={
            table_id: assessment.primary_coords
            for table_id, assessment in assessments.items()
        },
        max_suppression_ratios={
            spec.table_id: contract.privacy.max_suppression_ratio
            for spec in contract.tables
        },
    )
    assert not verification.passed
    assert any(item.table_id == "sex_by_district" for item in verification.deductions)


def test_single_table_ratio_overflow_blocks_whole_group(multi_table_factory):
    path = multi_table_factory(
        {"a": {("R1", "C1"): 2}, "b": {}},
        row_order=("R1", "R2", "R3"),
        col_order=("C1", "C2", "C3"),
        shared=[
            {
                "name": "ab",
                "cells": [
                    {"table": "a", "row": "R1", "col": "C1"},
                    {"table": "b", "row": "R1", "col": "C1"},
                ],
            }
        ],
        max_ratio=0.2,
    )
    # Table b is a 3x3 table with a single propagated cell: 1/9 stays below
    # 0.2, while the receiving table's own closure would exceed its limit.
    payload = __import__("yaml").safe_load(path.read_text(encoding="utf-8"))
    (path.parent / "b.csv").write_text(
        "row,col,count\n"
        + "".join(
            f"{row},{col},100\n"
            for row in ("R1", "R2", "R3")
            for col in ("C1", "C2", "C3")
        ),
        encoding="utf-8",
    )
    path.write_text(
        __import__("yaml").safe_dump(payload, sort_keys=False), encoding="utf-8"
    )
    _, _, _, outcome, verification = _solve(path)
    # Local solve of table a already exceeds its own 0.2 limit (4/9), so the
    # relation is blocked locally before any propagation.  Table b must block
    # as a member of the same relation rather than reporting an isolated pass.
    assert outcome.suppressions["a"].blocked
    assert outcome.suppressions["b"].blocked
    assert any(
        reason.code == "E_JOINT_GROUP_BLOCKED"
        for reason in outcome.suppressions["b"].block_reasons
    )
    assert all(group.status == "blocked" for group in outcome.groups)
    # The verifier only judges recoverability; the run is blocked by the
    # solver's round limit above, not by a recoverable cell.
    assert verification.per_table["b"].passed


def test_joint_ratio_overflow_blocks_whole_group(tmp_path):
    """Propagation alone can push a receiving table over its own limit."""

    import yaml

    def write_table(name, rows, cols, small):
        lines = ["row,col,count"]
        for row in rows:
            for col in cols:
                lines.append(
                    f"{row},{col},{2 if (row, col) == small else 100}"
                )
        (tmp_path / f"{name}.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")

    big_rows = ("R1", "R2", "R3", "R4")
    big_cols = ("C1", "C2", "C3", "C4")
    small_rows = ("S1", "S2", "S3")
    small_cols = ("K1", "K2", "K3")
    write_table("a", big_rows, big_cols, ("R1", "C1"))
    write_table("b", small_rows, small_cols, None)
    payload = {
        "version": 1,
        "dataset_name": "joint_ratio",
        "privacy": {
            "min_count": 5,
            "zero_policy": "structural",
            "max_suppression_ratio": 0.35,
        },
        "tables": [
            {
                "id": "a",
                "source": "a.csv",
                "row_dim": "row",
                "col_dim": "col",
                "value": "count",
                "row_order": list(big_rows),
                "col_order": list(big_cols),
                "missing_cell_policy": "error",
            },
            {
                "id": "b",
                "source": "b.csv",
                "row_dim": "row",
                "col_dim": "col",
                "value": "count",
                "row_order": list(small_rows),
                "col_order": list(small_cols),
                "missing_cell_policy": "error",
            },
        ],
        "shared_cells": [
            {
                "name": "ab",
                "cells": [
                    {"table": "a", "row": "R1", "col": "C1"},
                    {"table": "b", "row": "S1", "col": "K1"},
                ],
            }
        ],
    }
    path = tmp_path / "contract.yml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    _, _, _, outcome, verification = _solve(path)
    # The 4x4 table solves locally at 4/16 = 0.25 and is only blocked because
    # its relation partner overflowed; it must not be reported as a passed
    # table, and its own ratio was never the cause.
    assert outcome.suppressions["a"].blocked
    assert all(
        reason.code == "E_JOINT_GROUP_BLOCKED"
        for reason in outcome.suppressions["a"].block_reasons
    )
    assert outcome.suppressions["a"].suppression_ratio == 0.25
    # The propagated cell forces closure in the 3x3 table to 4/9 = 0.444.
    assert outcome.suppressions["b"].blocked
    assert any(
        reason.code == "E_JOINT_SUPPRESSION_RATIO_EXCEEDED"
        for reason in outcome.suppressions["b"].block_reasons
    )
    # The whole relation is blocked; a does not report an isolated pass.
    assert any(
        reason.code == "E_JOINT_GROUP_BLOCKED"
        for reason in outcome.suppressions["a"].block_reasons
    )
    assert all(group.status == "blocked" for group in outcome.groups)
    assert not verification.passed
    failed = {check.name for check in verification.checks if not check.passed}
    assert "shared_group_ratio_limits" in failed
    # Table a's local solve is untouched by the ratio failure in b.
    assert verification.per_table["a"].passed
    assert not verification.per_table["b"].passed


def test_unshared_tables_are_byte_identical_to_local_solver(demo_contract):
    contract, models, assessments, outcome, _ = _solve(demo_contract)
    assert outcome.groups == ()
    assert outcome.events == ()
    for spec in contract.tables:
        local = suppress_table(
            models[spec.table_id], spec, assessments[spec.table_id], contract.privacy
        )
        joint = outcome.suppressions[spec.table_id]
        assert joint.suppressed == local.suppressed
        assert joint.primary_count == local.primary_count
        assert joint.secondary_count == local.secondary_count
        assert joint.suppression_ratio == local.suppression_ratio
        assert joint.blocked == local.blocked
        assert joint.rounds == local.rounds
        assert joint.block_reasons == local.block_reasons


def test_unshared_public_output_is_unchanged(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    outcome = execute_run(ROOT / "fixtures/demo/contract.yml", tmp_path / "out", ledger)
    assert outcome.status == "ok"
    assert outcome.report["shared_relations"] == []
    assert outcome.report["trigger_chain"] == []
    assert outcome.report["summary"]["shared_group_count"] == 0
    with (tmp_path / "out" / "public" / "region_by_age.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["0-17"] == "NA"
    assert rows[0]["65+"] == "NA"
    assert rows[2]["0-17"] == "20"


def test_shared_run_publishes_consistent_public_and_audit_outputs(tmp_path, shared_contract):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    outcome = execute_run(shared_contract, tmp_path / "out", ledger)
    assert outcome.status == "ok"
    report = outcome.report
    assert report["strategy"] == STRATEGY
    assert {group["name"] for group in report["shared_relations"]} == {
        "under18_region_alias",
        "age_band_alias",
        "district_alias",
    }
    assert report["trigger_chain"]
    assert report["joint_verification"]["passed"] is True
    assert report["summary"]["shared_group_count"] == 3
    assert report["summary"]["joint_verify_passed"] is True

    # The public CSVs show NA exactly where the shared suppression maps say.
    for table in report["tables"]:
        hidden = {
            (cell["row"], cell["col"]) for cell in table["suppressed_cells"]
        }
        with (tmp_path / "out" / "public" / f"{table['table_id']}.csv").open(
            encoding="utf-8", newline=""
        ) as handle:
            rows = list(csv.DictReader(handle))
        for row in rows[:-1]:
            for col in row:
                if col in ("row", "region", "district", "Total"):
                    continue
                expected = "NA" if (row[next(iter(set(row) & {"row", "region", "district"}))], col) in hidden else row[col]
                assert row[col] == expected

    on_disk = json.loads(
        (tmp_path / "out" / "audit" / "report.json").read_text(encoding="utf-8")
    )
    assert on_disk["strategy"] == STRATEGY
    assert on_disk["trigger_chain"] == report["trigger_chain"]
    assert "跨表" in (tmp_path / "out" / "audit" / "report.md").read_text(encoding="utf-8")
    assert "跨表" in (tmp_path / "out" / "audit" / "report.html").read_text(encoding="utf-8")


def test_blocked_group_publishes_no_csv(tmp_path, multi_table_factory):
    path = multi_table_factory(
        {"a": {("R1", "C1"): 2}, "b": {}},
        shared=[
            {
                "name": "ab",
                "cells": [
                    {"table": "a", "row": "R1", "col": "C1"},
                    {"table": "b", "row": "R1", "col": "C1"},
                ],
            }
        ],
        max_ratio=0.2,
    )
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    outcome = execute_run(path, tmp_path / "out", ledger)
    assert outcome.status == "blocked"
    assert sorted(p.name for p in (tmp_path / "out" / "public").glob("*.csv")) == []
    report = json.loads(
        (tmp_path / "out" / "audit" / "report.json").read_text(encoding="utf-8")
    )
    assert report["summary"]["shared_group_blocked_count"] == 1
    assert report["shared_relations"][0]["status"] == "blocked"
    blocked_ids = {
        reason["table_id"]
        for reason in report["summary"]["block_reasons"]
    }
    assert blocked_ids == {"a", "b"}


def test_ledger_and_reverify_share_the_joint_conclusion(tmp_path, shared_contract):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    outcome = execute_run(shared_contract, tmp_path / "out", ledger)
    detail = ledger.get_run_detail(outcome.run_id)
    names = {check["name"] for check in detail["checks"]}
    assert {
        "shared_relations_resolvable",
        "shared_suppression_completeness",
        "shared_alias_exposure",
        "cross_table_reconstruction",
        "shared_group_ratio_limits",
    } <= names
    assert any(check["name"] == "shared_relation_member" for check in detail["checks"])
    loaded = ledger.load_for_verification(outcome.run_id)
    district_table = next(
        table
        for table in loaded["tables"]
        if table["table_id"] == "sex_by_district"
    )
    moved = next(
        cell
        for cell in district_table["cells"]
        if cell["row_key"] == "R3" and cell["col_key"] == "C3"
    )
    assert moved["suppression_type"] == "primary"

    passed = reverify_run(ledger, outcome.run_id)
    assert passed.passed and passed.status == "ok"
    reverified = {
        check["name"]
        for check in ledger.get_run_detail(outcome.run_id)["checks"]
        if check["source"] == "reverify"
    }
    assert "cross_table_reconstruction" in reverified


def test_reverify_detects_published_alias_tampering(tmp_path, shared_contract):
    import sqlite3

    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    outcome = execute_run(shared_contract, tmp_path / "out", ledger)
    with sqlite3.connect(db) as connection:
        connection.execute(
            "UPDATE cells SET suppression_type='none' "
            "WHERE run_id=? AND table_id='sex_by_district' AND row_key='R3' AND col_key='C3'",
            (outcome.run_id,),
        )
    result = reverify_run(ledger, outcome.run_id)
    assert not result.passed
    assert result.status == "verify_failed"
    failed = {
        check["name"]
        for check in ledger.get_run_detail(outcome.run_id)["checks"]
        if check["source"] == "reverify" and check["result"] == "fail"
    }
    assert "cross_table_reconstruction" in failed
    assert "shared_alias_exposure" in failed


def test_alias_relation_can_span_more_than_two_tables(multi_table_factory):
    path = multi_table_factory(
        {"a": {("R1", "C1"): 2}, "b": {}, "c": {}},
        shared=[
            {
                "name": "abc",
                "cells": [
                    {"table": "a", "row": "R1", "col": "C1"},
                    {"table": "b", "row": "R2", "col": "C2"},
                    {"table": "c", "row": "R3", "col": "C3"},
                ],
            }
        ],
    )
    _, _, _, outcome, verification = _solve(path)
    assert outcome.suppressions["b"].suppressed[("R2", "C2")].kind == SUPPRESSION_PRIMARY
    assert outcome.suppressions["c"].suppressed[("R3", "C3")].kind == SUPPRESSION_PRIMARY
    assert verification.passed


def test_joint_verify_failure_is_not_reported_as_per_table_pass(tmp_path, shared_contract):
    """A failing whole-release check must fail the run and every member table."""

    import sqlite3

    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    outcome = execute_run(shared_contract, tmp_path / "out", ledger)
    assert outcome.status == "ok"

    # Remove only the *complementary* district margin cell, leaving every
    # single table still able to look superficially valid while the declared
    # relation is now partially public.
    with sqlite3.connect(db) as connection:
        connection.execute(
            "UPDATE cells SET suppression_type='none' WHERE run_id=? AND table_id=? "
            "AND row_key=? AND col_key=?",
            (outcome.run_id, "sex_by_district", "R3", "C3"),
        )
    result = reverify_run(ledger, outcome.run_id)
    assert not result.passed
    assert result.status == "verify_failed"
    assert all(not table["passed"] for table in result.tables)
    detail = ledger.get_run_detail(outcome.run_id)
    assert all(not table["verify_passed"] for table in detail["tables"])
    fail_checks = [
        check
        for check in detail["checks"]
        if check["source"] == "reverify"
        and check["result"] == "fail"
        and check["table_id"] is None
    ]
    assert any(
        check["name"] == "cross_table_reconstruction" for check in fail_checks
    )


def _zero_alias_contract(tmp_path, *, alias_suppressed: bool):
    """A published structural zero aliases a cell that may be suppressed."""

    import yaml

    def write(name, small, zero=None):
        lines = ["row,col,count"]
        for row in ("R1", "R2", "R3"):
            for col in ("C1", "C2", "C3"):
                value = 0 if (row, col) == zero else 2 if (row, col) == small else 100
                lines.append(f"{row},{col},{value}")
        (tmp_path / f"{name}.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")

    write("a", ("R1", "C1"), zero=("R2", "C2"))
    write("b", None)
    payload = {
        "version": 1,
        "dataset_name": "zero_alias",
        "privacy": {
            "min_count": 5,
            "zero_policy": "structural",
            "max_suppression_ratio": 0.6,
            "structural_zero_cells": [{"table": "a", "row": "R2", "col": "C2"}],
        },
        "tables": [
            {
                "id": "a",
                "source": "a.csv",
                "row_dim": "row",
                "col_dim": "col",
                "value": "count",
                "row_order": ["R1", "R2", "R3"],
                "col_order": ["C1", "C2", "C3"],
                "missing_cell_policy": "error",
            },
            {
                "id": "b",
                "source": "b.csv",
                "row_dim": "row",
                "col_dim": "col",
                "value": "count",
                "row_order": ["R1", "R2", "R3"],
                "col_order": ["C1", "C2", "C3"],
                "missing_cell_policy": "error",
            },
        ],
        "shared_cells": [
            {
                "name": "zero_alias",
                "cells": [
                    {"table": "a", "row": "R2", "col": "C2"},
                    {"table": "b", "row": "R1", "col": "C1"},
                ],
            }
        ],
    }
    if alias_suppressed:
        payload["shared_cells"].append(
            {
                "name": "hidden_alias",
                "cells": [
                    {"table": "b", "row": "R1", "col": "C1"},
                    {"table": "a", "row": "R1", "col": "C1"},
                ],
            }
        )
    path = tmp_path / "contract.yml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def test_public_structural_zero_aliasing_a_hidden_cell_blocks(tmp_path):
    # The zero is itself selected as a complementary cell locally, so it is
    # not actually published.  No exposure, and the relation resolves.
    path = _zero_alias_contract(tmp_path, alias_suppressed=False)
    _, _, _, outcome, verification = _solve(path)
    assert outcome.suppressions["a"].suppressed[("R2", "C2")].kind == SUPPRESSION_SECONDARY
    assert not outcome.suppressions["a"].blocked
    assert any(
        check.name == "shared_public_zero_exposure" and check.passed
        for check in verification.checks
    )


def test_published_zero_exposes_suppressed_alias_and_blocks(tmp_path):
    """A structural zero published next to a hidden alias leaks the value."""

    import yaml

    rows = ("R1", "R2", "R3", "R4")
    cols = ("C1", "C2", "C3", "C4")

    def write(name, small=None, zero=None):
        lines = ["row,col,count"]
        for row in rows:
            for col in cols:
                value = 0 if (row, col) == zero else 2 if (row, col) == small else 100
                lines.append(f"{row},{col},{value}")
        (tmp_path / f"{name}.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")

    # Table a publishes a structural zero; table b keeps the same business
    # cell as a small count, so its copy is primary-suppressed.
    write("a", small=("R1", "C1"), zero=("R4", "C4"))
    write("b", small=("R4", "C4"))
    payload = {
        "version": 1,
        "dataset_name": "zero_exposure",
        "privacy": {
            "min_count": 5,
            "zero_policy": "structural",
            "max_suppression_ratio": 0.6,
            "structural_zero_cells": [{"table": "a", "row": "R4", "col": "C4"}],
        },
        "tables": [
            {
                "id": "a",
                "source": "a.csv",
                "row_dim": "row",
                "col_dim": "col",
                "value": "count",
                "row_order": list(rows),
                "col_order": list(cols),
                "missing_cell_policy": "error",
            },
            {
                "id": "b",
                "source": "b.csv",
                "row_dim": "row",
                "col_dim": "col",
                "value": "count",
                "row_order": list(rows),
                "col_order": list(cols),
                "missing_cell_policy": "error",
            },
        ],
        "shared_cells": [
            {
                "name": "zero_vs_small",
                "cells": [
                    {"table": "a", "row": "R4", "col": "C4"},
                    {"table": "b", "row": "R4", "col": "C4"},
                ],
            }
        ],
    }
    path = tmp_path / "contract.yml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    _, _, _, outcome, verification = _solve(path)
    assert outcome.suppressions["a"].blocked
    assert outcome.suppressions["b"].blocked
    assert any(
        reason.code == "E_JOINT_ALIAS_EXPOSURE"
        for reason in outcome.suppressions["a"].block_reasons
    )
    assert not verification.passed
    failed = {check.name for check in verification.checks if not check.passed}
    assert "shared_public_zero_exposure" in failed


def test_joint_round_exhaustion_blocks_group(multi_table_factory):
    path = multi_table_factory(
        {"a": {("R1", "C1"): 2}, "b": {}},
        shared=[
            {
                "name": "ab",
                "cells": [
                    {"table": "a", "row": "R1", "col": "C1"},
                    {"table": "b", "row": "R1", "col": "C1"},
                ],
            }
        ],
        max_ratio=0.6,
        max_rounds=2,
    )
    _, _, _, outcome, verification = _solve(path)
    assert outcome.suppressions["b"].blocked
    assert any(
        reason.code == "E_JOINT_SUPPRESSION_MAX_ROUNDS"
        for reason in outcome.suppressions["b"].block_reasons
    )
    assert outcome.suppressions["a"].blocked
    assert all(
        reason.code == "E_JOINT_GROUP_BLOCKED"
        for reason in outcome.suppressions["a"].block_reasons
    )
    assert all(group.status == "blocked" for group in outcome.groups)
    # The verifier only judges recoverability; the run is blocked by the
    # solver's round limit above, not by a recoverable cell.
    assert verification.per_table["b"].passed


def test_joint_verify_flags_declared_coordinate_missing_from_matrix(shared_contract):
    contract, models, assessments, outcome, _ = _solve(shared_contract)
    # A restored ledger that lost a row would make a declared alias
    # unresolvable; the joint verifier must say so instead of passing.
    from cellcloak.model import TableModel

    region = models["sex_by_region"]
    truncated = TableModel(
        table_id=region.table_id,
        row_dim=region.row_dim,
        col_dim=region.col_dim,
        value_dim=region.value_dim,
        row_keys=region.row_keys[1:],
        col_keys=region.col_keys,
        counts={
            key: value
            for key, value in region.counts.items()
            if key[0] in region.row_keys[1:]
        },
        source_name=region.source_name,
        source_sha256=region.source_sha256,
    )
    suppressions = {
        table_id: dict(result.suppressed)
        for table_id, result in outcome.suppressions.items()
    }
    verification = verify_joint(
        contract.shared_cells,
        {"sex_by_region": truncated, "sex_by_district": models["sex_by_district"]},
        suppressions,
        primary_coords={
            table_id: assessment.primary_coords
            for table_id, assessment in assessments.items()
        },
        max_suppression_ratios={
            spec.table_id: contract.privacy.max_suppression_ratio
            for spec in contract.tables
        },
    )
    assert not verification.passed
    failed = {check.name for check in verification.checks if not check.passed}
    assert "shared_coordinates_in_range" in failed


def test_relation_with_more_than_two_members_propagates_to_all(multi_table_factory):
    path = multi_table_factory(
        {"a": {("R1", "C1"): 2}, "b": {}, "c": {}},
        shared=[
            {
                "name": "abc",
                "cells": [
                    {"table": "a", "row": "R1", "col": "C1"},
                    {"table": "b", "row": "R2", "col": "C2"},
                    {"table": "c", "row": "R3", "col": "C3"},
                ],
            }
        ],
        max_ratio=0.6,
    )
    _, _, _, outcome, verification = _solve(path)
    for table_id, coordinate in (
        ("a", ("R1", "C1")),
        ("b", ("R2", "C2")),
        ("c", ("R3", "C3")),
    ):
        reason = outcome.suppressions[table_id].suppressed[coordinate]
        assert reason.kind == SUPPRESSION_PRIMARY
    assert outcome.suppressions["b"].suppressed[("R2", "C2")].propagated
    assert outcome.suppressions["c"].suppressed[("R3", "C3")].propagated
    propagated = {
        (event.table_id, event.row, event.col)
        for event in outcome.events
        if event.kind == "shared_propagation"
    }
    assert {("b", "R2", "C2"), ("c", "R3", "C3")} <= propagated
    assert verification.passed
