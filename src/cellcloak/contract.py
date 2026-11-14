"""YAML publication-contract loading and static validation."""

from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

from .config import DEFAULT_MAX_ROUNDS, MAX_ROUNDS_CEILING
from .errors import ContractError

TABLE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
ZERO_POLICIES = {"structural", "protect_sampling"}
MISSING_POLICIES = {"error", "zero"}


@dataclass(frozen=True)
class CellRef:
    table_id: str
    row: str
    col: str


@dataclass(frozen=True)
class TableSpec:
    table_id: str
    source: str
    source_path: Path
    row_dim: str
    col_dim: str
    value: str
    row_order: tuple[str, ...]
    col_order: tuple[str, ...]
    missing_cell_policy: str
    loss_weights: Mapping[tuple[str, str], float]


@dataclass(frozen=True)
class PrivacyPolicy:
    min_count: int
    zero_policy: str
    max_suppression_ratio: float
    structural_zero_cells: frozenset[CellRef]
    always_suppress: frozenset[CellRef]
    max_suppression_rounds: int


@dataclass(frozen=True)
class Contract:
    path: Path
    base_dir: Path
    version: int
    dataset_name: str
    privacy: PrivacyPolicy
    tables: tuple[TableSpec, ...]
    contract_hash: str

    def table(self, table_id: str) -> TableSpec:
        for spec in self.tables:
            if spec.table_id == table_id:
                return spec
        raise ContractError("E_TABLE_UNKNOWN", "table is not declared", table_id=table_id)


def _err(code: str, message: str, **kwargs: Any) -> ContractError:
    return ContractError(code, message, **kwargs)


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise _err("E_CONTRACT_SCHEMA", "must be a mapping", field=field)
    return value


def _string(
    value: Any, field: str, *, allow_empty: bool = False, table_id: str | None = None
) -> str:
    if not isinstance(value, str):
        raise _err("E_CONTRACT_SCHEMA", "must be a string", field=field, table_id=table_id)
    text = value.strip()
    if not text and not allow_empty:
        raise _err("E_CONTRACT_SCHEMA", "must not be empty", field=field, table_id=table_id)
    return text


def _integer(value: Any, field: str, default: int | None = None) -> int:
    if value is None and default is not None:
        return default
    if isinstance(value, bool) or not isinstance(value, int):
        raise _err("E_CONTRACT_SCHEMA", "must be an integer", field=field)
    return value


def _number(value: Any, field: str, default: float | None = None) -> float:
    if value is None and default is not None:
        return default
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _err("E_CONTRACT_SCHEMA", "must be a number", field=field)
    number = float(value)
    if not math.isfinite(number):
        raise _err("E_CONTRACT_SCHEMA", "must be finite", field=field)
    return number


def _order(value: Any, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise _err("E_CONTRACT_SCHEMA", "must be a non-empty list", field=field)
    result: list[str] = []
    for index, item in enumerate(value):
        text = _string(item, f"{field}[{index}]")
        if text in result:
            raise _err("E_CONTRACT_SCHEMA", f"duplicate value {text!r}", field=field)
        result.append(text)
    return tuple(result)


def _safe_source(value: Any, base_dir: Path, table_id: str) -> tuple[str, Path]:
    source = _string(value, f"tables.{table_id}.source")
    raw_parts = source.replace("\\", "/").split("/")
    if Path(source).is_absolute() or ".." in raw_parts or not source:
        raise _err(
            "E_SOURCE_PATH",
            "source must be a relative path without parent traversal",
            table_id=table_id,
            field="source",
        )
    resolved = (base_dir / source).resolve()
    try:
        resolved.relative_to(base_dir.resolve())
    except ValueError as exc:
        raise _err(
            "E_SOURCE_PATH", "source escapes the contract directory", table_id=table_id
        ) from exc
    if not resolved.is_file():
        raise _err(
            "E_SOURCE_MISSING",
            "source CSV does not exist",
            table_id=table_id,
            field=source,
            path=source,
        )
    return source, resolved


def _loss_weights(raw: Any, table_id: str, row_order: tuple[str, ...], col_order: tuple[str, ...]) -> dict[tuple[str, str], float]:
    if raw is None:
        return {}
    if not isinstance(raw, list):
        raise _err("E_CONTRACT_SCHEMA", "must be a list", table_id=table_id, field="loss_weights")
    result: dict[tuple[str, str], float] = {}
    for index, item in enumerate(raw):
        field = f"loss_weights[{index}]"
        entry = _mapping(item, field)
        row = _string(entry.get("row"), f"{field}.row")
        col = _string(entry.get("col"), f"{field}.col")
        weight = _number(entry.get("weight"), f"{field}.weight")
        if row not in row_order or col not in col_order:
            raise _err("E_CELL_REFERENCE", "weight cell is outside row_order/col_order", table_id=table_id, cell=(row, col))
        if weight < 0:
            raise _err("E_CONTRACT_SCHEMA", "weight must be non-negative", field=f"{field}.weight")
        if (row, col) in result:
            raise _err("E_CONTRACT_SCHEMA", "duplicate weight cell", table_id=table_id, cell=(row, col))
        result[(row, col)] = weight
    return result


def _parse_table(raw_table: Any, index: int, base_dir: Path, seen: set[str]) -> TableSpec:
    table = _mapping(raw_table, f"tables[{index}]")
    table_id = _string(table.get("id"), f"tables[{index}].id")
    if not TABLE_ID_RE.fullmatch(table_id):
        raise _err("E_CONTRACT_SCHEMA", "id must match [A-Za-z0-9][A-Za-z0-9_-]*", table_id=table_id, field="id")
    if table_id in seen:
        raise _err("E_TABLE_DUPLICATE", "table id is duplicated", table_id=table_id)
    seen.add(table_id)
    source, source_path = _safe_source(table.get("source"), base_dir, table_id)
    row_dim = _string(table.get("row_dim"), f"tables.{table_id}.row_dim", table_id=table_id)
    col_dim = _string(table.get("col_dim"), f"tables.{table_id}.col_dim", table_id=table_id)
    value_dim = _string(table.get("value"), f"tables.{table_id}.value", table_id=table_id)
    if len({row_dim, col_dim, value_dim}) != 3:
        raise _err("E_CONTRACT_SCHEMA", "row_dim, col_dim, and value must be distinct", table_id=table_id)
    row_order = _order(table.get("row_order"), f"tables.{table_id}.row_order")
    col_order = _order(table.get("col_order"), f"tables.{table_id}.col_order")
    missing = table.get("missing_cell_policy", "error")
    if missing not in MISSING_POLICIES:
        raise _err("E_CONTRACT_SCHEMA", "missing_cell_policy must be error or zero", table_id=table_id, field="missing_cell_policy")
    weights = _loss_weights(table.get("loss_weights"), table_id, row_order, col_order)
    return TableSpec(
        table_id=table_id,
        source=source,
        source_path=source_path,
        row_dim=row_dim,
        col_dim=col_dim,
        value=value_dim,
        row_order=row_order,
        col_order=col_order,
        missing_cell_policy=missing,
        loss_weights=weights,
    )


def _cell_refs(raw: Any, field: str, tables: Mapping[str, TableSpec]) -> frozenset[CellRef]:
    if raw is None:
        return frozenset()
    if not isinstance(raw, list):
        raise _err("E_CONTRACT_SCHEMA", "must be a list", field=field)
    refs: set[CellRef] = set()
    for index, item in enumerate(raw):
        entry = _mapping(item, f"{field}[{index}]")
        table_id = _string(entry.get("table"), f"{field}[{index}].table")
        row = _string(entry.get("row"), f"{field}[{index}].row")
        col = _string(entry.get("col"), f"{field}[{index}].col")
        spec = tables.get(table_id)
        if spec is None:
            raise _err("E_CONTRACT_REFERENCE", "cell references an unknown table", table_id=table_id, field=field)
        if row not in spec.row_order or col not in spec.col_order:
            raise _err("E_CELL_REFERENCE", "cell is outside row_order/col_order", table_id=table_id, cell=(row, col))
        ref = CellRef(table_id, row, col)
        if ref in refs:
            raise _err("E_CONTRACT_SCHEMA", "duplicate cell reference", table_id=table_id, cell=(row, col))
        refs.add(ref)
    return frozenset(refs)


def _parse_privacy(raw: Any, tables: Mapping[str, TableSpec]) -> PrivacyPolicy:
    privacy = _mapping(raw, "privacy")
    min_count = _integer(privacy.get("min_count"), "privacy.min_count", 5)
    if min_count < 1:
        raise _err("E_CONTRACT_SCHEMA", "min_count must be at least 1", field="privacy.min_count")
    zero_policy = _string(privacy.get("zero_policy"), "privacy.zero_policy")
    if zero_policy not in ZERO_POLICIES:
        raise _err("E_CONTRACT_SCHEMA", "zero_policy must be structural or protect_sampling", field="privacy.zero_policy")
    ratio = _number(privacy.get("max_suppression_ratio"), "privacy.max_suppression_ratio", 0.35)
    if not 0.0 <= ratio <= 1.0:
        raise _err("E_CONTRACT_SCHEMA", "max_suppression_ratio must be between 0 and 1", field="privacy.max_suppression_ratio")
    rounds = _integer(privacy.get("max_suppression_rounds"), "privacy.max_suppression_rounds", DEFAULT_MAX_ROUNDS)
    if not 1 <= rounds <= MAX_ROUNDS_CEILING:
        raise _err("E_CONTRACT_SCHEMA", "max_suppression_rounds is outside the supported range", field="privacy.max_suppression_rounds")
    return PrivacyPolicy(
        min_count=min_count,
        zero_policy=zero_policy,
        max_suppression_ratio=ratio,
        structural_zero_cells=_cell_refs(privacy.get("structural_zero_cells"), "privacy.structural_zero_cells", tables),
        always_suppress=_cell_refs(privacy.get("always_suppress"), "privacy.always_suppress", tables),
        max_suppression_rounds=rounds,
    )


def load_contract(contract_path: str | Path) -> Contract:
    path = Path(contract_path).expanduser().resolve()
    if not path.is_file():
        raise _err("E_CONTRACT_READ", "contract file does not exist", path=path)
    try:
        raw_bytes = path.read_bytes()
    except OSError as exc:
        raise _err("E_CONTRACT_READ", "cannot read contract file", path=path) from exc
    try:
        text = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise _err("E_CONTRACT_ENCODING", "contract must be UTF-8 or UTF-8-SIG", path=path) from exc
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise _err("E_CONTRACT_YAML", "invalid YAML", path=path) from exc
    root = _mapping(data, "document")
    version = _integer(root.get("version"), "version")
    if version != 1:
        raise _err("E_VERSION_UNSUPPORTED", "only contract version 1 is supported", field="version")
    dataset_name = _string(root.get("dataset_name"), "dataset_name")
    raw_tables = root.get("tables")
    if not isinstance(raw_tables, list) or not raw_tables:
        raise _err("E_CONTRACT_SCHEMA", "tables must be a non-empty list", field="tables")
    seen: set[str] = set()
    base_dir = path.parent
    tables = tuple(_parse_table(item, index, base_dir, seen) for index, item in enumerate(raw_tables))
    privacy = _parse_privacy(root.get("privacy"), {table.table_id: table for table in tables})
    if "privacy" not in root:
        raise _err("E_CONTRACT_SCHEMA", "privacy section is required", field="privacy")
    return Contract(
        path=path,
        base_dir=base_dir,
        version=version,
        dataset_name=dataset_name,
        privacy=privacy,
        tables=tables,
        contract_hash=hashlib.sha256(raw_bytes).hexdigest(),
    )
