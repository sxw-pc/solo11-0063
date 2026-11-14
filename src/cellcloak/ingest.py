"""Load long-format count CSVs and build a complete 2-D table model."""

from __future__ import annotations

import csv
import hashlib
import io
import re
from pathlib import Path

from .contract import TableSpec
from .errors import IngestError
from .model import TableModel

COUNT_RE = re.compile(r"^[0-9]+$")


def _err(code: str, message: str, **kwargs: object) -> IngestError:
    return IngestError(code, message, **kwargs)


def _decode(path: Path) -> tuple[str, str]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise _err("E_CSV_READ", "cannot read CSV", path=path) from exc
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise _err("E_CSV_ENCODING", "CSV must be UTF-8 or UTF-8-SIG", path=path) from exc
    return text, hashlib.sha256(raw).hexdigest()


def _headers(reader: csv.DictReader[str], spec: TableSpec) -> None:
    if reader.fieldnames is None:
        raise _err("E_CSV_HEADER", "CSV has no header row", table_id=spec.table_id, path=spec.source)
    reader.fieldnames = [name.strip() for name in reader.fieldnames]
    if len(set(reader.fieldnames)) != len(reader.fieldnames):
        raise _err("E_CSV_HEADER", "CSV header has duplicate columns", table_id=spec.table_id, path=spec.source)
    required = (spec.row_dim, spec.col_dim, spec.value)
    missing = [name for name in required if name not in reader.fieldnames]
    if missing:
        raise _err(
            "E_CSV_HEADER",
            f"CSV is missing required column(s): {', '.join(missing)}",
            table_id=spec.table_id,
            field=missing[0],
            path=spec.source,
        )


def _is_blank_row(row: dict[str | None, str | None]) -> bool:
    return all(value is None or not str(value).strip() for value in row.values())


def _parse_count(value: str | None, spec: TableSpec, line_no: int, cell: tuple[str, str]) -> int:
    text = (value or "").strip()
    if not text:
        raise _err("E_CSV_VALUE", f"count is empty on line {line_no}", table_id=spec.table_id, field=spec.value, cell=cell, path=spec.source)
    if not COUNT_RE.fullmatch(text):
        raise _err("E_CSV_VALUE", f"count must be a non-negative integer on line {line_no}", table_id=spec.table_id, field=spec.value, cell=cell, path=spec.source)
    return int(text)


def load_table(spec: TableSpec) -> TableModel:
    text, source_hash = _decode(spec.source_path)
    # Skip completely blank lines before the header; csv.DictReader skips
    # blank records inside the body but treats a leading blank line as data.
    while text and not text.splitlines()[0].strip():
        text = text.split("\n", 1)[1] if "\n" in text else ""
    counts: dict[tuple[str, str], int] = {}
    try:
        reader = csv.DictReader(io.StringIO(text, newline=""))
        _headers(reader, spec)
        for line_no, row in enumerate(reader, start=2):
            if _is_blank_row(row):
                continue
            if None in row:
                raise _err("E_CSV_HEADER", f"row on line {line_no} has extra fields", table_id=spec.table_id, path=spec.source)
            row_key = (row.get(spec.row_dim) or "").strip()
            col_key = (row.get(spec.col_dim) or "").strip()
            if not row_key:
                raise _err("E_CSV_VALUE", f"row dimension is empty on line {line_no}", table_id=spec.table_id, field=spec.row_dim, path=spec.source)
            if not col_key:
                raise _err("E_CSV_VALUE", f"column dimension is empty on line {line_no}", table_id=spec.table_id, field=spec.col_dim, path=spec.source)
            if row_key not in spec.row_order:
                raise _err("E_CSV_ROW_KEY", f"undeclared row key {row_key!r} on line {line_no}", table_id=spec.table_id, field=spec.row_dim, cell=(row_key, col_key), path=spec.source)
            if col_key not in spec.col_order:
                raise _err("E_CSV_COL_KEY", f"undeclared column key {col_key!r} on line {line_no}", table_id=spec.table_id, field=spec.col_dim, cell=(row_key, col_key), path=spec.source)
            coordinate = (row_key, col_key)
            if coordinate in counts:
                raise _err("E_CSV_DUPLICATE", f"duplicate cell on line {line_no}", table_id=spec.table_id, cell=coordinate, path=spec.source)
            counts[coordinate] = _parse_count(row.get(spec.value), spec, line_no, coordinate)
    except csv.Error as exc:
        raise _err("E_CSV_FORMAT", "malformed CSV", table_id=spec.table_id, path=spec.source) from exc

    for row in spec.row_order:
        for col in spec.col_order:
            coordinate = (row, col)
            if coordinate in counts:
                continue
            if spec.missing_cell_policy != "zero":
                raise _err(
                    "E_CSV_MISSING_CELL",
                    "CSV does not contain every row_order x col_order combination",
                    table_id=spec.table_id,
                    cell=coordinate,
                    path=spec.source,
                )
            counts[coordinate] = 0

    return TableModel(
        table_id=spec.table_id,
        row_dim=spec.row_dim,
        col_dim=spec.col_dim,
        value_dim=spec.value,
        row_keys=spec.row_order,
        col_keys=spec.col_order,
        counts=counts,
        source_name=spec.source,
        source_sha256=source_hash,
    )
