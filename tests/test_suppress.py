from cellcloak.contract import load_contract
from cellcloak.ingest import load_table
from cellcloak.model import SUPPRESSION_PRIMARY, SUPPRESSION_SECONDARY
from cellcloak.risk import classify
from cellcloak.suppress import suppress_table


def _run(path):
    contract = load_contract(path)
    spec = contract.tables[0]
    model = load_table(spec)
    assessment = classify(model, spec, contract.privacy)
    return suppress_table(model, spec, assessment, contract.privacy)


def test_demo_region_requires_complementary_suppression(demo_contract):
    contract = load_contract(demo_contract)
    spec = contract.table("region_by_age")
    model = load_table(spec)
    assessment = classify(model, spec, contract.privacy)
    result = suppress_table(model, spec, assessment, contract.privacy)
    assert not result.blocked
    assert result.primary_count == 1
    assert result.secondary_count == 3
    assert model.cell_count() == 16
    assert result.suppression_ratio == 0.25
    assert result.suppressed[("R01", "0-17")].kind == SUPPRESSION_PRIMARY
    assert set(result.suppressed) == {
        ("R01", "0-17"),
        ("R01", "65+"),
        ("R02", "0-17"),
        ("R02", "65+"),
    }
    for row in model.row_keys:
        assert sum((row, col) in result.suppressed for col in model.col_keys) != 1
    for col in model.col_keys:
        assert sum((row, col) in result.suppressed for row in model.row_keys) != 1
    assert 0 < result.suppressed_count_sum_ratio < 1
    assert result.max_row_suppression_ratio == 0.5
    assert result.max_col_suppression_ratio == 0.5


def test_suppression_is_deterministic(demo_contract):
    first = _run(demo_contract)
    second = _run(demo_contract)
    assert first.suppression_ratio == second.suppression_ratio
    assert first.blocked == second.blocked
    assert list(first.suppressed) == list(second.suppressed)
    assert {
        key: (value.kind, value.risk_category, value.round_no)
        for key, value in first.suppressed.items()
    } == {
        key: (value.kind, value.risk_category, value.round_no)
        for key, value in second.suppressed.items()
    }


def test_lower_loss_weight_breaks_equal_count_tie(contract_factory):
    path = contract_factory(
        "row,col,count\n"
        "R1,C1,1\nR1,C2,10\nR1,C3,10\n"
        "R2,C1,10\nR2,C2,10\nR2,C3,10\n"
        "R3,C1,10\nR3,C2,10\nR3,C3,10\n",
        rows=("R1", "R2", "R3"),
        cols=("C1", "C2", "C3"),
        loss_weights={("R1", "C2"): 5.0, ("R1", "C3"): 0.1},
        max_ratio=0.5,
    )
    result = _run(path)
    assert not result.blocked
    assert result.suppressed[("R1", "C3")].kind == SUPPRESSION_SECONDARY
    assert ("R1", "C2") not in result.suppressed


def test_ratio_limit_blocks_primary_suppression(contract_factory):
    path = contract_factory(
        "row,col,count\nR1,C1,1\nR1,C2,1\nR2,C1,1\nR2,C2,10\n",
        max_ratio=0.35,
    )
    result = _run(path)
    assert result.blocked
    assert result.suppression_ratio == 0.75
    assert [reason.code for reason in result.block_reasons] == [
        "E_SUPPRESSION_RATIO_EXCEEDED"
    ]


def test_single_cell_table_has_no_complement_candidate(contract_factory):
    path = contract_factory(
        "row,col,count\nR1,C1,1\n",
        rows=("R1",),
        cols=("C1",),
        max_ratio=1.0,
    )
    result = _run(path)
    assert result.blocked
    assert result.secondary_count == 0
    assert any(reason.code == "E_SUPPRESSION_NO_CANDIDATE" for reason in result.block_reasons)


def test_secondary_cells_keep_baseline_safe_category(demo_contract):
    result = _run(demo_contract)
    for coordinate, reason in result.suppressed.items():
        if reason.kind == SUPPRESSION_SECONDARY:
            assert coordinate not in {("R01", "0-17")}
            assert reason.risk_category == "safe"
            assert reason.round_no >= 1
