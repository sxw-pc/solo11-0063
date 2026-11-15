import json
from pathlib import Path

from cellcloak.cli import main

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "fixtures" / "demo" / "contract.yml"
BLOCKED = ROOT / "fixtures" / "blocked" / "contract.yml"


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


def test_forget_cli_dry_run_purge_and_repeat(tmp_path, capsys):
    db = str(tmp_path / "ledger.sqlite3")
    out = tmp_path / "out"
    assert main(["run", "--contract", str(DEMO), "--out", str(out), "--db", db, "--json"]) == 0
    run_id = _json_output(capsys)["run_id"]

    assert main(["forget", "--run", run_id, "--reason", "retention-2026", "--db", db, "--dry-run"]) == 0
    dry = capsys.readouterr().out
    assert "dry-run" in dry
    assert "would_delete" in dry
    assert (out / "audit" / "report.json").is_file()

    assert main(["forget", "--run", run_id, "--reason", "retention-2026", "--db", db, "--json"]) == 0
    result = _json_output(capsys)
    assert result["state"] == "purged"
    assert result["result"]["outcome"] == "purged"
    assert result["db_rows"]["cells"] > 0
    assert not (out / "audit" / "report.json").exists()
    assert (out / "public" / "region_by_age.csv").is_file()

    assert main(["runs", "--db", db, "--json"]) == 0
    runs = _json_output(capsys)
    assert runs[0]["status"] == "purged"
    assert runs[0]["tombstone"]["reason_code"] == "retention-2026"

    assert main(["verify", "--run", run_id, "--db", db, "--json"]) == 4
    verification = _json_output(capsys)
    assert verification["status"] == "purged"
    assert verification["passed"] is False

    # Repeating the purge is an idempotent no-op.
    assert main(["forget", "--run", run_id, "--reason", "retention-2026", "--db", db]) == 0
    assert "already purged" in capsys.readouterr().out


def test_forget_cli_kept_files_exit_five(tmp_path, capsys):
    db = str(tmp_path / "ledger.sqlite3")
    out = tmp_path / "out"
    assert main(["run", "--contract", str(DEMO), "--out", str(out), "--db", db, "--json"]) == 0
    run_id = _json_output(capsys)["run_id"]
    (out / "audit" / "report.json").write_text('{"tampered": true}\n', encoding="utf-8")

    assert main(["forget", "--run", run_id, "--reason", "retention-2026", "--db", db]) == 5
    output = capsys.readouterr().out
    assert "kept_hash_mismatch" in output
    assert "purged_with_kept_files" in output


def test_forget_cli_errors(tmp_path, capsys):
    db = str(tmp_path / "ledger.sqlite3")
    assert main(["forget", "--run", "run-missing", "--reason", "retention-2026", "--db", db]) == 2
    assert "E_RUN_NOT_FOUND" in capsys.readouterr().err
    assert main(["forget", "--run", "run-missing", "--reason", "bad reason!", "--db", db]) == 2
    assert "E_FORGET_REASON" in capsys.readouterr().err


def test_serve_refuses_non_loopback_and_invalid_port(tmp_path, capsys):
    db = str(tmp_path / "ledger.sqlite3")
    assert main(["serve", "--host", "0.0.0.0", "--port", "5017", "--db", db]) == 2
    assert "E_LOOPBACK_ONLY" in capsys.readouterr().err
    assert main(["serve", "--host", "127.0.0.1", "--port", "0", "--db", db]) == 2
    assert "E_SERVE_ARGUMENT" in capsys.readouterr().err
