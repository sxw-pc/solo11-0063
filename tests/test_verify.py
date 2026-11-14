from cellcloak.contract import load_contract
from cellcloak.ingest import load_table
from cellcloak.risk import classify
from cellcloak.suppress import suppress_table
from cellcloak.verify import verify_table


def _prepared(contract_path, table_id="region_by_age"):
    contract = load_contract(contract_path)
    spec = contract.table(table_id)
    model = load_table(spec)
    assessment = classify(model, spec, contract.privacy)
    suppression = suppress_table(model, spec, assessment, contract.privacy)
    return contract, model, assessment, suppression


def test_valid_suppression_passes_all_checks(demo_contract):
    contract, model, assessment, suppression = _prepared(demo_contract)
    result = verify_table(
        model,
        suppression.suppressed,
        primary_coords=assessment.primary_coords,
        max_suppression_ratio=contract.privacy.max_suppression_ratio,
    )
    assert result.passed
    assert all(check.passed for check in result.checks)
    assert {check.name for check in result.checks} == {
        "margin_consistency",
        "suppression_map_valid",
        "primary_coverage",
        "suppression_ratio_limit",
        "single_unknown_rows",
        "single_unknown_columns",
        "reconstruction_attack",
    }
    assert len(result.evidence) == 4
    for item in result.evidence:
        assert item.row_suppressed_count == 2
        assert item.col_suppressed_count == 2
        assert not item.directly_inferable


def test_removing_one_secondary_exposes_margin_attack(demo_contract):
    contract, model, assessment, suppression = _prepared(demo_contract)
    broken = dict(suppression.suppressed)
    broken.pop(("R02", "65+"))
    result = verify_table(
        model,
        broken,
        primary_coords=assessment.primary_coords,
        max_suppression_ratio=contract.privacy.max_suppression_ratio,
    )
    assert not result.passed
    failed = {check.name for check in result.checks if not check.passed}
    assert "single_unknown_rows" in failed
    assert "single_unknown_columns" in failed
    assert "reconstruction_attack" in failed
    assert any(item.directly_inferable for item in result.evidence)


def test_ratio_limit_is_checked_independently(demo_contract):
    contract, model, assessment, suppression = _prepared(demo_contract)
    result = verify_table(
        model,
        suppression.suppressed,
        primary_coords=assessment.primary_coords,
        max_suppression_ratio=0.1,
    )
    assert not result.passed
    ratio_check = next(check for check in result.checks if check.name == "suppression_ratio_limit")
    assert not ratio_check.passed


def test_primary_coverage_is_checked(demo_contract):
    contract, model, assessment, suppression = _prepared(demo_contract)
    broken = dict(suppression.suppressed)
    primary = next(iter(assessment.primary_coords))
    broken.pop(primary)
    result = verify_table(
        model,
        broken,
        primary_coords=assessment.primary_coords,
        max_suppression_ratio=contract.privacy.max_suppression_ratio,
    )
    coverage = next(check for check in result.checks if check.name == "primary_coverage")
    assert not coverage.passed


def test_margin_consistency_holds_for_unmodified_input(demo_contract):
    contract, model, _, _ = _prepared(demo_contract, "age_by_sex")
    result = verify_table(
        model,
        {},
        primary_coords=set(),
        max_suppression_ratio=contract.privacy.max_suppression_ratio,
    )
    assert result.passed
    margin = next(check for check in result.checks if check.name == "margin_consistency")
    assert margin.passed
    assert result.evidence == ()
