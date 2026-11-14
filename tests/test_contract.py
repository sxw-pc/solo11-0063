from pathlib import Path

import pytest
import yaml

from cellcloak.contract import load_contract
from cellcloak.errors import ContractError


def _write(path: Path, payload: dict) -> Path:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _minimal_table(**overrides):
    table = {
        "id": "t1",
        "source": "data.csv",
        "row_dim": "row",
        "col_dim": "col",
        "value": "count",
        "row_order": ["R1", "R2"],
        "col_order": ["C1", "C2"],
    }
    table.update(overrides)
    return table


def test_load_demo_contract(demo_contract):
    contract = load_contract(demo_contract)
    assert contract.version == 1
    assert contract.dataset_name == "synthetic_demo_2026"
    assert [table.table_id for table in contract.tables] == ["age_by_sex", "region_by_age"]
    assert contract.privacy.min_count == 5
    assert contract.privacy.zero_policy == "structural"
    assert len(contract.privacy.structural_zero_cells) == 1
    assert contract.privacy.max_suppression_ratio == 0.35
    assert contract.table("region_by_age").loss_weights[("R02", "65+")] == 0.5
    assert len(contract.contract_hash) == 64
    assert contract.table("region_by_age").source_path.is_file()


def test_contract_accepts_utf8_bom(contract_factory):
    path = contract_factory()
    path.write_bytes(b"\xef\xbb\xbf" + path.read_bytes())
    assert load_contract(path).dataset_name == "synthetic_test"


def test_unsupported_version(tmp_path):
    source = tmp_path / "data.csv"
    source.write_text("row,col,count\nR1,C1,10\n", encoding="utf-8")
    payload = {
        "version": 2,
        "dataset_name": "synthetic_test",
        "privacy": {"zero_policy": "structural", "min_count": 5},
        "tables": [_minimal_table()],
    }
    path = _write(tmp_path / "contract.yml", payload)
    with pytest.raises(ContractError) as info:
        load_contract(path)
    assert info.value.code == "E_VERSION_UNSUPPORTED"


def test_duplicate_table_id_is_rejected(tmp_path):
    (tmp_path / "data.csv").write_text("row,col,count\nR1,C1,10\n", encoding="utf-8")
    payload = {
        "version": 1,
        "dataset_name": "synthetic_test",
        "privacy": {"zero_policy": "structural"},
        "tables": [_minimal_table(), _minimal_table()],
    }
    path = _write(tmp_path / "contract.yml", payload)
    with pytest.raises(ContractError) as info:
        load_contract(path)
    assert info.value.code == "E_TABLE_DUPLICATE"
    assert info.value.table_id == "t1"


def test_parent_traversal_source_is_rejected(tmp_path):
    base = tmp_path / "cfg"
    base.mkdir()
    (tmp_path / "outside.csv").write_text("row,col,count\nR1,C1,10\n", encoding="utf-8")
    payload = {
        "version": 1,
        "dataset_name": "synthetic_test",
        "privacy": {"zero_policy": "structural"},
        "tables": [_minimal_table(source="../outside.csv")],
    }
    path = _write(base / "contract.yml", payload)
    with pytest.raises(ContractError) as info:
        load_contract(path)
    assert info.value.code == "E_SOURCE_PATH"


def test_missing_source_is_rejected(contract_factory):
    path = contract_factory()
    data = path.parent / "data.csv"
    data.unlink()
    with pytest.raises(ContractError) as info:
        load_contract(path)
    assert info.value.code == "E_SOURCE_MISSING"


def test_empty_order_and_bad_privacy_are_rejected(contract_factory):
    empty_order = contract_factory(rows=())
    with pytest.raises(ContractError) as first:
        load_contract(empty_order)
    assert first.value.code == "E_CONTRACT_SCHEMA"

    bad_min = contract_factory(min_count=0)
    with pytest.raises(ContractError) as second:
        load_contract(bad_min)
    assert second.value.code == "E_CONTRACT_SCHEMA"
    assert second.value.field == "privacy.min_count"


def test_unknown_cell_reference_is_rejected(contract_factory):
    path = contract_factory()
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    payload["privacy"]["structural_zero_cells"] = [
        {"table": "missing", "row": "R1", "col": "C1"}
    ]
    _write(path, payload)
    with pytest.raises(ContractError) as info:
        load_contract(path)
    assert info.value.code == "E_CONTRACT_REFERENCE"


def test_cell_reference_outside_orders_is_rejected(contract_factory):
    path = contract_factory()
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    payload["privacy"]["always_suppress"] = [
        {"table": "t1", "row": "R9", "col": "C1"}
    ]
    _write(path, payload)
    with pytest.raises(ContractError) as info:
        load_contract(path)
    assert info.value.code == "E_CELL_REFERENCE"
    assert info.value.cell == ("R9", "C1")


def test_unsafe_yaml_tag_is_rejected(tmp_path):
    path = tmp_path / "contract.yml"
    path.write_text("version: !!python/object/apply:os.system ['echo unsafe']\n", encoding="utf-8")
    with pytest.raises(ContractError) as info:
        load_contract(path)
    assert info.value.code == "E_CONTRACT_YAML"
