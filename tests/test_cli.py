import json
from pathlib import Path

from cellcloak.cli import main

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "fixtures" / "demo" / "contract.yml"
BLOCKED = ROOT / "fixtures" / "blocked" / "contract.yml"
SHARED = ROOT / "fixtures" / "shared" / "contract.yml"


def _json_output(capsys):
    return json.loads(capsys.readouterr().out)


def test_run_runs_and_verify_workflow(tmp_path, capsys):
    db = str(tmp_path / "ledger.sqlite3")
    out = str(tmp_path / "out")
    assert main(["run", "--contract", str(DEMO), "--out", out, "--db", db, "--json"]) == 0
    outcome = _json_output(capsys)
    assert outcome["status"] == "ok"
    assert outcome["summary"]["suppressed_cell_count"] == 4

    assert main(["runs", "--db", db, "--json"]) == 0
    runs = _json_output(capsys)
    assert len(runs) == 1
    assert runs[0]["id"] == outcome["run_id"]

    assert main(["verify", "--run", outcome["run_id"], "--db", db, "--json"]) == 0
    verification = _json_output(capsys)
    assert verification["passed"] is True
    assert verification["status"] == "ok"


def test_diff_cli(tmp_path, capsys):
    db = str(tmp_path / "ledger.sqlite3")
    for name in ("a", "b"):
        assert main([
            "run", "--contract", str(DEMO), "--out", str(tmp_path / name),
            "--db", db, "--json",
        ]) == 0
        _json_output(capsys)
    assert main(["runs", "--db", db, "--json"]) == 0
    runs = _json_output(capsys)
    assert main([
        "diff", "--run-a", runs[0]["id"], "--run-b", runs[1]["id"],
        "--db", db, "--json",
    ]) == 0
    comparison = _json_output(capsys)
    assert comparison["summary"]["public_hash_changed"] is False
    assert comparison["summary"]["suppression_ratio_delta"] == 0


def test_blocked_run_returns_exit_three(tmp_path, capsys):
    db = str(tmp_path / "ledger.sqlite3")
    out = tmp_path / "blocked-out"
    assert main(["run", "--contract", str(BLOCKED), "--out", str(out), "--db", db]) == 3
    output = capsys.readouterr().out
    assert "status: blocked" in output
    assert "E_SUPPRESSION_RATIO_EXCEEDED" in output
    assert list((out / "public").glob("*.csv")) == []


def test_bad_contract_returns_exit_two(tmp_path, capsys):
    contract = tmp_path / "contract.yml"
    contract.write_text("version: 9\ndataset_name: synthetic\n", encoding="utf-8")
    code = main([
        "run", "--contract", str(contract), "--out", str(tmp_path / "out"),
        "--db", str(tmp_path / "ledger.sqlite3"),
    ])
    captured = capsys.readouterr()
    assert code == 2
    assert "E_VERSION_UNSUPPORTED" in captured.err
    assert "Traceback" not in captured.err


def test_verify_missing_run_returns_exit_two(tmp_path, capsys):
    assert main([
        "verify", "--run", "run-missing", "--db", str(tmp_path / "ledger.sqlite3")
    ]) == 2
    assert "E_RUN_NOT_FOUND" in capsys.readouterr().err


def test_shared_run_and_verify_cli_report_joint_results(tmp_path, capsys):
    db = str(tmp_path / "ledger.sqlite3")
    out = str(tmp_path / "shared-out")
    assert main(["run", "--contract", str(SHARED), "--out", out, "--db", db]) == 0
    output = capsys.readouterr().out
    assert "status: ok" in output
    assert "shared relations: 3" in output
    assert "local_then_propagation_closure" in output

    assert main(["run", "--contract", str(SHARED), "--out", str(tmp_path / "again"),
                 "--db", db, "--json"]) == 0
    outcome = _json_output(capsys)
    assert outcome["summary"]["shared_group_count"] == 3
    assert outcome["summary"]["joint_verify_passed"] is True
    assert main(["verify", "--run", outcome["run_id"], "--db", db, "--json"]) == 0
    verification = _json_output(capsys)
    assert verification["passed"] is True


def test_shared_contract_errors_return_exit_two(tmp_path, capsys):
    contract = tmp_path / "contract.yml"
    source = tmp_path / "data.csv"
    source.write_text(
        "row,col,count\nR1,C1,10\nR1,C2,10\n", encoding="utf-8"
    )
    contract.write_text(
        "version: 1\n"
        "dataset_name: synthetic\n"
        "privacy:\n"
        "  zero_policy: structural\n"
        "tables:\n"
        "  - id: t1\n"
        "    source: data.csv\n"
        "    row_dim: row\n"
        "    col_dim: col\n"
        "    value: count\n"
        "    row_order: [R1, R2]\n"
        "    col_order: [C1, C2]\n"
        "shared_cells:\n"
        "  - name: broken\n"
        "    cells:\n"
        "      - {table: t1, row: R1, col: C1}\n"
        "      - {table: ghost, row: R2, col: C2}\n",
        encoding="utf-8",
    )
    code = main([
        "run", "--contract", str(contract), "--out", str(tmp_path / "out"),
        "--db", str(tmp_path / "ledger.sqlite3"),
    ])
    captured = capsys.readouterr()
    assert code == 2
    assert "E_SHARED_REFERENCE" in captured.err
    assert "Traceback" not in captured.err


def test_serve_refuses_non_loopback_and_invalid_port(tmp_path, capsys):
    db = str(tmp_path / "ledger.sqlite3")
    assert main(["serve", "--host", "0.0.0.0", "--port", "5017", "--db", db]) == 2
    assert "E_LOOPBACK_ONLY" in capsys.readouterr().err
    assert main(["serve", "--host", "127.0.0.1", "--port", "0", "--db", db]) == 2
    assert "E_SERVE_ARGUMENT" in capsys.readouterr().err
