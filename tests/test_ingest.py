from pathlib import Path

import pytest

from cellcloak.contract import load_contract
from cellcloak.errors import IngestError
from cellcloak.ingest import load_table

INVALID_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "invalid"


def _model(contract_path):
    contract = load_contract(contract_path)
    return load_table(contract.tables[0])


def test_demo_table_is_complete_and_margins_balance(demo_contract):
    contract = load_contract(demo_contract)
    spec = contract.table("age_by_sex")
    model = load_table(spec)
    assert model.cell_count() == 12
    assert model.row_count() == 3
    assert model.col_count() == 4
    assert model.count("U", "65+") == 0
    assert model.row_total("F") == 195
    assert model.col_total("0-17") == 89
    assert model.total() == 404
    assert sum(model.row_total(row) for row in model.row_keys) == model.total()
    assert sum(model.col_total(col) for col in model.col_keys) == model.total()
    assert len(model.fingerprint()) == 64
    assert model.source_sha256


def test_utf8_bom_and_blank_lines_are_accepted(contract_factory):
    path = contract_factory(
        "\nrow,col,count\nR1,C1,10\n\nR1,C2,11\nR2,C1,12\nR2,C2,13\n\n"
    )
    data = path.parent / "data.csv"
    data.write_bytes(b"\xef\xbb\xbf" + data.read_bytes())
    model = _model(path)
    assert model.count("R2", "C2") == 13


def test_missing_cell_errors_by_default(contract_factory):
    path = contract_factory("row,col,count\nR1,C1,10\n")
    with pytest.raises(IngestError) as info:
        _model(path)
    assert info.value.code == "E_CSV_MISSING_CELL"
    assert info.value.cell == ("R1", "C2")


def test_missing_cells_can_be_filled_with_zero(contract_factory):
    path = contract_factory("row,col,count\nR1,C1,10\n", missing_policy="zero")
    model = _model(path)
    assert model.count("R1", "C2") == 0
    assert model.count("R2", "C1") == 0
    assert model.cell_count() == 4


@pytest.mark.parametrize(
    "text,code",
    [
        ("region,age_band,value\nR1,C1,10\n", "E_CSV_HEADER"),
        ("row,col,count\nR1,C1,-2\n", "E_CSV_VALUE"),
        ("row,col,count\nR1,C1,1.5\n", "E_CSV_VALUE"),
        ("row,col,count\nR1,C1,\n", "E_CSV_VALUE"),
        ("row,col,count\nR1,C1,5\nR1,C1,7\n", "E_CSV_DUPLICATE"),
        ("row,col,count\nR9,C1,5\n", "E_CSV_ROW_KEY"),
        ("row,col,count\nR1,C9,5\n", "E_CSV_COL_KEY"),
    ],
)
def test_invalid_csv_values_are_rejected(contract_factory, text, code):
    path = contract_factory(text)
    with pytest.raises(IngestError) as info:
        _model(path)
    assert info.value.code == code


def test_bundled_invalid_fixtures_are_rejected(contract_factory):
    path = contract_factory(
        "",
        rows=("R01",),
        cols=("0-17", "18-34"),
        row_dim="region",
        col_dim="age_band",
    )
    data = path.parent / "data.csv"
    data.write_text((INVALID_DIR / "negative_count.csv").read_text(encoding="utf-8"), encoding="utf-8")
    with pytest.raises(IngestError) as info:
        _model(path)
    assert info.value.code == "E_CSV_VALUE"
    assert info.value.cell == ("R01", "0-17")

    data.write_text((INVALID_DIR / "missing_column.csv").read_text(encoding="utf-8"), encoding="utf-8")
    with pytest.raises(IngestError) as missing:
        _model(path)
    assert missing.value.code == "E_CSV_HEADER"


def test_duplicate_header_is_rejected(contract_factory):
    path = contract_factory("row,row,count\nR1,R1,10\n")
    with pytest.raises(IngestError) as info:
        _model(path)
    assert info.value.code == "E_CSV_HEADER"
