import pytest

from cellcloak.contract import load_contract
from cellcloak.errors import ContractError
from cellcloak.ingest import load_table
from cellcloak.model import (
    RISK_BLOCKED,
    RISK_FORCED,
    RISK_SAFE,
    RISK_SAMPLING_ZERO,
    RISK_SMALL,
    cell_fingerprint,
)
from cellcloak.risk import classify


def _prepare(path):
    contract = load_contract(path)
    spec = contract.tables[0]
    model = load_table(spec)
    return contract, spec, model


def test_risk_classes_and_forced_precedence(contract_factory):
    path = contract_factory(
        "row,col,count\nR1,C1,3\nR1,C2,0\nR2,C1,0\nR2,C2,10\n",
        structural=[("R1", "C2")],
        always=[("R2", "C2")],
    )
    contract, spec, model = _prepare(path)
    assessment = classify(model, spec, contract.privacy)
    assert assessment.finding("R1", "C1").category == RISK_SMALL
    assert assessment.finding("R1", "C2").category == RISK_SAFE
    assert assessment.finding("R2", "C1").category == RISK_SAMPLING_ZERO
    assert assessment.finding("R2", "C2").category == RISK_FORCED
    assert assessment.primary_coords == {("R1", "C1"), ("R2", "C1"), ("R2", "C2")}
    assert assessment.counts[RISK_SAFE] == 1
    assert assessment.risk_level() == RISK_FORCED
    assert assessment.risk_level(blocked=True) == RISK_BLOCKED


def test_protect_sampling_zero_overrides_structural_tag(contract_factory):
    path = contract_factory(
        "row,col,count\nR1,C1,10\nR1,C2,0\nR2,C1,10\nR2,C2,10\n",
        structural=[("R1", "C2")],
        zero_policy="protect_sampling",
    )
    contract, spec, model = _prepare(path)
    assessment = classify(model, spec, contract.privacy)
    assert assessment.finding("R1", "C2").category == RISK_SAMPLING_ZERO


def test_structural_zero_must_actually_be_zero(contract_factory):
    path = contract_factory(
        "row,col,count\nR1,C1,10\nR1,C2,4\nR2,C1,10\nR2,C2,10\n",
        structural=[("R1", "C2")],
    )
    contract, spec, model = _prepare(path)
    with pytest.raises(ContractError) as info:
        classify(model, spec, contract.privacy)
    assert info.value.code == "E_STRUCTURAL_ZERO_VALUE"
    assert info.value.cell == ("R1", "C2")


def test_fingerprint_is_stable_and_has_no_count_material(contract_factory):
    first = cell_fingerprint("t1", "R1", "C1")
    second = cell_fingerprint("t1", "R1", "C1")
    other = cell_fingerprint("t1", "R1", "C2")
    assert first == second
    assert first != other
    assert len(first) == 64
