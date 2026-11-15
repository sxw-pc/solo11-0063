"""Shared pytest fixtures for local, synthetic Cellcloak contracts."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Iterable

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
DEMO_DIR = ROOT / "fixtures" / "demo"
BLOCKED_DIR = ROOT / "fixtures" / "blocked"
INVALID_DIR = ROOT / "fixtures" / "invalid"
SHARED_DIR = ROOT / "fixtures" / "shared"


@pytest.fixture
def demo_contract() -> Path:
    return DEMO_DIR / "contract.yml"


@pytest.fixture
def blocked_contract() -> Path:
    return BLOCKED_DIR / "contract.yml"


@pytest.fixture
def shared_contract() -> Path:
    return SHARED_DIR / "contract.yml"


@pytest.fixture
def contract_factory(tmp_path: Path) -> Callable[..., Path]:
    def make(
        csv_text: str | None = None,
        *,
        rows: Iterable[str] = ("R1", "R2"),
        cols: Iterable[str] = ("C1", "C2"),
        table_id: str = "t1",
        min_count: int = 5,
        zero_policy: str = "structural",
        max_ratio: float = 0.35,
        structural: Iterable[tuple[str, str]] = (),
        always: Iterable[tuple[str, str]] = (),
        missing_policy: str = "error",
        loss_weights: dict[tuple[str, str], float] | None = None,
        extra_privacy: dict[str, Any] | None = None,
        row_dim: str = "row",
        col_dim: str = "col",
        value_dim: str = "count",
    ) -> Path:
        rows = tuple(rows)
        cols = tuple(cols)
        if csv_text is None:
            lines = [f"{row_dim},{col_dim},{value_dim}"]
            for row in rows:
                for col in cols:
                    lines.append(f"{row},{col},10")
            csv_text = "\n".join(lines) + "\n"
        source = tmp_path / "data.csv"
        source.write_text(csv_text, encoding="utf-8")
        privacy: dict[str, Any] = {
            "min_count": min_count,
            "zero_policy": zero_policy,
            "max_suppression_ratio": max_ratio,
            "structural_zero_cells": [
                {"table": table_id, "row": row, "col": col} for row, col in structural
            ],
            "always_suppress": [
                {"table": table_id, "row": row, "col": col} for row, col in always
            ],
        }
        if extra_privacy:
            privacy.update(extra_privacy)
        table: dict[str, Any] = {
            "id": table_id,
            "source": "data.csv",
            "row_dim": row_dim,
            "col_dim": col_dim,
            "value": value_dim,
            "row_order": list(rows),
            "col_order": list(cols),
            "missing_cell_policy": missing_policy,
        }
        if loss_weights:
            table["loss_weights"] = [
                {"row": row, "col": col, "weight": weight}
                for (row, col), weight in loss_weights.items()
            ]
        payload = {
            "version": 1,
            "dataset_name": "synthetic_test",
            "privacy": privacy,
            "tables": [table],
        }
        contract = tmp_path / "contract.yml"
        contract.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
        return contract

    return make


@pytest.fixture
def multi_table_factory(tmp_path: Path) -> Callable[..., Path]:
    """Build a contract with arbitrarily many synthetic tables and relations."""

    def make(
        tables: dict[str, dict[tuple[str, str], int]],
        *,
        row_order: Iterable[str] = ("R1", "R2", "R3"),
        col_order: Iterable[str] = ("C1", "C2", "C3"),
        shared: Iterable[dict[str, Any]] = (),
        min_count: int = 5,
        max_ratio: float = 0.5,
        zero_policy: str = "structural",
        max_rounds: int = 100,
    ) -> Path:
        rows = tuple(row_order)
        cols = tuple(col_order)
        table_payloads: list[dict[str, Any]] = []
        for table_id, counts in tables.items():
            filename = f"{table_id}.csv"
            lines = ["row,col,count"]
            for row in rows:
                for col in cols:
                    lines.append(f"{row},{col},{counts.get((row, col), 100)}")
            (tmp_path / filename).write_text("\n".join(lines) + "\n", encoding="utf-8")
            table_payloads.append(
                {
                    "id": table_id,
                    "source": filename,
                    "row_dim": "row",
                    "col_dim": "col",
                    "value": "count",
                    "row_order": list(rows),
                    "col_order": list(cols),
                    "missing_cell_policy": "error",
                }
            )
        payload: dict[str, Any] = {
            "version": 1,
            "dataset_name": "synthetic_multi",
            "privacy": {
                "min_count": min_count,
                "zero_policy": zero_policy,
                "max_suppression_ratio": max_ratio,
                "max_suppression_rounds": max_rounds,
            },
            "tables": table_payloads,
        }
        if shared:
            payload["shared_cells"] = list(shared)
        contract = tmp_path / "contract.yml"
        contract.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
        return contract

    return make
