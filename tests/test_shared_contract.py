"""Contract-level checks for explicit shared-cell relations."""

from pathlib import Path

import pytest
import yaml

from cellcloak.contract import load_contract
from cellcloak.errors import ContractError


def _reload(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


@pytest.fixture
def mutable_contract(shared_contract, tmp_path):
    """Copy the shared fixture into tmp_path so each test can mutate it."""

    target = tmp_path / "contract.yml"
    target.write_text(shared_contract.read_text(encoding="utf-8"), encoding="utf-8")
    for name in ("sex_by_region.csv", "sex_by_district.csv"):
        (tmp_path / name).write_text(
            (shared_contract.parent / name).read_text(encoding="utf-8"), encoding="utf-8"
        )
    return target


def _shared(shared_contract):
    return load_contract(shared_contract).shared_cells


def test_shared_relations_are_loaded_in_declared_order(shared_contract):
    groups = _shared(shared_contract)
    assert [group.name for group in groups] == [
        "under18_region_alias",
        "age_band_alias",
        "district_alias",
    ]
    first = groups[0]
    assert first.table_ids == ("sex_by_region", "sex_by_district")
    assert first.has("sex_by_region", "R1", "C1")
    assert first.has("sex_by_district", "R3", "C3")
    assert not first.has("sex_by_region", "R2", "C2")


def test_contract_indexes_groups_by_table_and_coordinate(shared_contract):
    contract = load_contract(shared_contract)
    assert {group.name for group in contract.shared_groups_for("sex_by_region")} == {
        "under18_region_alias",
        "age_band_alias",
        "district_alias",
    }
    assert contract.group_of("sex_by_district", "R3", "C3").name == "under18_region_alias"
    assert contract.group_of("sex_by_district", "R4", "C4") is None


def test_demo_contract_without_relations_has_empty_declaration(demo_contract):
    assert load_contract(demo_contract).shared_cells == ()


def test_unknown_table_is_rejected(mutable_contract):
    shared_contract = mutable_contract
    payload = _reload(shared_contract)
    payload["shared_cells"].append(
        {
            "name": "dangling",
            "cells": [
                {"table": "sex_by_region", "row": "R4", "col": "C4"},
                {"table": "missing", "row": "R1", "col": "C1"},
            ],
        }
    )
    shared_contract.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(ContractError) as info:
        load_contract(shared_contract)
    assert info.value.code == "E_SHARED_REFERENCE"
    assert info.value.shared_name == "dangling"


def test_unknown_coordinate_is_rejected(mutable_contract):
    shared_contract = mutable_contract
    payload = _reload(shared_contract)
    payload["shared_cells"].append(
        {
            "name": "bad_coordinate",
            "cells": [
                {"table": "sex_by_region", "row": "R4", "col": "C4"},
                {"table": "sex_by_district", "row": "R9", "col": "C3"},
            ],
        }
    )
    shared_contract.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(ContractError) as info:
        load_contract(shared_contract)
    assert info.value.code == "E_SHARED_COORDINATE"
    assert info.value.cell == ("R9", "C3")


def test_duplicate_group_name_is_rejected(mutable_contract):
    shared_contract = mutable_contract
    payload = _reload(shared_contract)
    payload["shared_cells"].append(
        {
            "name": "under18_region_alias",
            "cells": [
                {"table": "sex_by_region", "row": "R2", "col": "C2"},
                {"table": "sex_by_district", "row": "R2", "col": "C2"},
            ],
        }
    )
    shared_contract.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(ContractError) as info:
        load_contract(shared_contract)
    assert info.value.code == "E_SHARED_GROUP_DUPLICATE"


def test_duplicate_alias_in_one_relation_is_rejected(mutable_contract):
    shared_contract = mutable_contract
    payload = _reload(shared_contract)
    payload["shared_cells"][0]["cells"].append(
        {"table": "sex_by_region", "row": "R1", "col": "C1"}
    )
    shared_contract.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(ContractError) as info:
        load_contract(shared_contract)
    assert info.value.code == "E_SHARED_CELL_DUPLICATE"
    assert info.value.cell == ("R1", "C1")


def test_cell_cannot_belong_to_two_relations(mutable_contract):
    shared_contract = mutable_contract
    payload = _reload(shared_contract)
    payload["shared_cells"].append(
        {
            "name": "conflicting",
            "cells": [
                {"table": "sex_by_region", "row": "R1", "col": "C1"},
                {"table": "sex_by_district", "row": "R4", "col": "C4"},
            ],
        }
    )
    shared_contract.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(ContractError) as info:
        load_contract(shared_contract)
    assert info.value.code == "E_SHARED_CONFLICT"
    assert info.value.shared_name == "conflicting"
    assert info.value.table_id == "sex_by_region"


def test_relation_must_span_two_tables(mutable_contract):
    shared_contract = mutable_contract
    payload = _reload(shared_contract)
    payload["shared_cells"].append(
        {
            "name": "single_table",
            "cells": [
                {"table": "sex_by_region", "row": "R4", "col": "C4"},
                {"table": "sex_by_region", "row": "R4", "col": "C3"},
            ],
        }
    )
    shared_contract.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(ContractError) as info:
        load_contract(shared_contract)
    assert info.value.code == "E_SHARED_TABLE_DUPLICATE"


def test_same_table_cannot_repeat_inside_a_relation(mutable_contract):
    shared_contract = mutable_contract
    payload = _reload(shared_contract)
    payload["shared_cells"].append(
        {
            "name": "repeated_table",
            "cells": [
                {"table": "sex_by_region", "row": "R4", "col": "C4"},
                {"table": "sex_by_district", "row": "R4", "col": "C4"},
                {"table": "sex_by_region", "row": "R4", "col": "C3"},
            ],
        }
    )
    shared_contract.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(ContractError) as info:
        load_contract(shared_contract)
    assert info.value.code == "E_SHARED_TABLE_DUPLICATE"


def test_relation_needs_at_least_two_cells(mutable_contract):
    shared_contract = mutable_contract
    payload = _reload(shared_contract)
    payload["shared_cells"].append(
        {
            "name": "too_small",
            "cells": [{"table": "sex_by_region", "row": "R4", "col": "C4"}],
        }
    )
    shared_contract.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(ContractError) as info:
        load_contract(shared_contract)
    assert info.value.code == "E_SHARED_GROUP_SIZE"


def test_shared_cells_must_be_a_list(mutable_contract):
    shared_contract = mutable_contract
    payload = _reload(shared_contract)
    payload["shared_cells"] = {"name": "not-a-list"}
    shared_contract.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(ContractError) as info:
        load_contract(shared_contract)
    assert info.value.code == "E_CONTRACT_SCHEMA"
    assert info.value.field == "shared_cells"
