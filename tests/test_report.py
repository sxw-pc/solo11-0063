import csv
import json
from pathlib import Path

from cellcloak.ledger import Ledger
from cellcloak.pipeline import execute_run

ROOT = Path(__file__).resolve().parents[1]


def _run(contract: Path, out: Path, ledger: Ledger):
    return execute_run(contract, out, ledger)


def test_demo_outputs_public_and_audit_separately(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    outcome = _run(ROOT / "fixtures/demo/contract.yml", tmp_path / "out", ledger)
    assert outcome.status == "ok"
    public = tmp_path / "out" / "public"
    audit = tmp_path / "out" / "audit"
    assert {path.name for path in public.glob("*.csv")} == {
        "age_by_sex.csv",
        "region_by_age.csv",
    }
    assert {path.name for path in audit.iterdir()} == {
        "report.json",
        "report.md",
        "report.html",
    }
    assert (tmp_path / "out" / ".cellcloak-output.json").is_file()

    report = json.loads((audit / "report.json").read_text(encoding="utf-8"))
    assert report["run_id"] == outcome.run_id
    assert report["status"] == "ok"
    assert report["contract_version"] == 1
    assert len(report["contract_hash"]) == 64
    assert {item["table_id"] for item in report["inputs"]} == {"age_by_sex", "region_by_age"}
    assert report["summary"]["verify_passed"] is True
    assert report["summary"]["risk_class_counts"]["blocked"] == 0
    region = next(item for item in report["tables"] if item["table_id"] == "region_by_age")
    assert region["primary_count"] == 1
    assert region["secondary_count"] == 3
    assert region["info_loss"]["suppressed_cell_count"] == 4
    assert region["info_loss"]["max_row_suppression_ratio"] == 0.5
    for cell in region["suppressed_cells"]:
        assert "count" not in cell
        assert "raw_count" not in cell
        assert len(cell["fingerprint"]) == 64
    assert region["verification"]["passed"] is True

    with (public / "region_by_age.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["0-17"] == "NA"
    assert rows[0]["65+"] == "NA"
    assert rows[2]["0-17"] == "20"
    assert rows[-1]["Total"] == "199"

    html = (audit / "report.html").read_text(encoding="utf-8")
    assert "<style>" in html
    assert "<link" not in html
    assert "http://" not in html
    assert "https://" not in html
    assert "R01" in html
    assert (audit / "report.md").read_text(encoding="utf-8").startswith("# ")


def test_blocked_table_publishes_no_csv_and_cleans_stale_output(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    out = tmp_path / "out"
    _run(ROOT / "fixtures/demo/contract.yml", out, ledger)
    assert any((out / "public").glob("*.csv"))
    blocked = _run(ROOT / "fixtures/blocked/contract.yml", out, ledger)
    assert blocked.status == "blocked"
    assert list((out / "public").glob("*.csv")) == []
    report = json.loads((out / "audit" / "report.json").read_text(encoding="utf-8"))
    assert report["status"] == "blocked"
    assert report["summary"]["blocked_table_count"] == 1
    assert report["summary"]["block_reasons"][0]["code"] == "E_SUPPRESSION_RATIO_EXCEEDED"
    assert report["tables"][0]["table_status"] == "blocked"
    assert report["tables"][0]["suppressed_cells"]
    assert report["tables"][0]["verification"]["passed"] is False


def test_output_guard_refuses_unmarked_public_content(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    out = tmp_path / "out"
    (out / "public").mkdir(parents=True)
    (out / "public" / "user-file.csv").write_text("do not overwrite\n", encoding="utf-8")
    try:
        _run(ROOT / "fixtures/demo/contract.yml", out, ledger)
    except Exception as exc:
        assert getattr(exc, "code", "") == "E_OUTPUT_NOT_EMPTY"
    else:
        raise AssertionError("unmarked output directory should have been refused")
    assert (out / "public" / "user-file.csv").read_text(encoding="utf-8") == "do not overwrite\n"
