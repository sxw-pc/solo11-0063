import sqlite3
from pathlib import Path

import pytest

from cellcloak.diffing import compare_runs
from cellcloak.errors import LedgerError
from cellcloak.ledger import Ledger
from cellcloak.pipeline import execute_run, reverify_run

ROOT = Path(__file__).resolve().parents[1]


def _demo(ledger, out):
    return execute_run(ROOT / "fixtures/demo/contract.yml", out, ledger)


def test_records_runs_cells_checks_and_artifacts(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    outcome = _demo(ledger, tmp_path / "out")
    runs = ledger.list_runs()
    assert len(runs) == 1
    assert runs[0]["id"] == outcome.run_id
    assert runs[0]["status"] == "ok"
    assert runs[0]["summary"]["table_count"] == 2

    detail = ledger.get_run_detail(outcome.run_id)
    assert detail is not None
    assert detail["run"]["dataset_name"] == "synthetic_demo_2026"
    assert len(detail["tables"]) == 2
    assert detail["checks"]
    assert detail["artifacts"]
    for artifact in detail["artifacts"]:
        assert len(artifact["sha256"]) == 64
        assert Path(artifact["path"]).is_file()
        assert "cells" not in detail["tables"][0]

    internal = ledger.load_for_verification(outcome.run_id)
    region = next(table for table in internal["tables"] if table["table_id"] == "region_by_age")
    assert len(region["cells"]) == 16
    raw = next(cell for cell in region["cells"] if cell["row_key"] == "R01" and cell["col_key"] == "0-17")
    assert raw["raw_count"] == 3
    assert raw["suppression_type"] == "primary"
    assert raw["risk_category"] == "small_count"


def test_repeated_runs_do_not_overwrite_records_and_can_be_compared(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    first = _demo(ledger, tmp_path / "out-a")
    second = _demo(ledger, tmp_path / "out-b")
    assert first.run_id != second.run_id
    runs = ledger.list_runs()
    assert len(runs) == 2
    assert runs[0]["id"] == max(first.run_id, second.run_id)
    comparison = compare_runs(ledger, first.run_id, second.run_id)
    assert comparison["summary"]["status_changed"] is False
    assert comparison["summary"]["public_hash_changed"] is False
    assert comparison["summary"]["suppression_ratio_delta"] == 0
    assert all(not table["changed"] for table in comparison["tables"])


def test_diff_detects_blocked_run_and_missing_public_output(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    good = _demo(ledger, tmp_path / "good")
    blocked = execute_run(ROOT / "fixtures/blocked/contract.yml", tmp_path / "blocked", ledger)
    comparison = compare_runs(ledger, good.run_id, blocked.run_id)
    assert comparison["summary"]["status_changed"] is True
    assert comparison["summary"]["public_hash_changed"] is True
    assert comparison["tables"][0]["only_in_a"] is True
    assert comparison["tables"][1]["only_in_b"] is True


def test_reverify_persists_checks_and_failure_status(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    outcome = _demo(ledger, tmp_path / "out")
    result = reverify_run(ledger, outcome.run_id)
    assert result.passed
    assert result.status == "ok"
    detail = ledger.get_run_detail(outcome.run_id)
    assert any(check["source"] == "reverify" for check in detail["checks"])

    blocked = execute_run(ROOT / "fixtures/blocked/contract.yml", tmp_path / "blocked", ledger)
    failed = reverify_run(ledger, blocked.run_id)
    assert not failed.passed
    assert failed.status == "blocked"
    detail = ledger.get_run_detail(blocked.run_id)
    assert any(check["source"] == "reverify" and check["result"] == "fail" for check in detail["checks"])


def test_reverify_failure_writes_verify_failed_status(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    ledger = Ledger(db)
    outcome = _demo(ledger, tmp_path / "out")
    with sqlite3.connect(db) as connection:
        connection.execute(
            "UPDATE cells SET suppression_type = 'none' "
            "WHERE run_id = ? AND table_id = 'region_by_age' AND row_key = 'R01' AND col_key = '65+'",
            (outcome.run_id,),
        )
    failed = reverify_run(ledger, outcome.run_id)
    assert not failed.passed
    assert failed.status == "verify_failed"
    assert ledger.get_run_detail(outcome.run_id)["run"]["status"] == "verify_failed"


def test_missing_run_errors_are_stable(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    assert ledger.get_run_detail("run-missing") is None
    with pytest.raises(LedgerError) as info:
        ledger.load_for_verification("run-missing")
    assert info.value.code == "E_RUN_NOT_FOUND"
    with pytest.raises(LedgerError) as compare_info:
        compare_runs(ledger, "run-missing", "run-other")
    assert compare_info.value.code == "E_RUN_NOT_FOUND"


def test_list_limit_and_ordering(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    first = _demo(ledger, tmp_path / "out-a")
    second = _demo(ledger, tmp_path / "out-b")
    assert first.run_id < second.run_id
    assert len(ledger.list_runs(limit=1)) == 1
