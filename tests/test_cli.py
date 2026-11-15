import json
import sqlite3
from pathlib import Path

from cellcloak.cli import main
from cellcloak.errors import VerifySnapshotError

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
    assert verification["attempt_status"] == "completed"
    assert verification["attempt_id"].startswith("att-")
    assert len(verification["snapshot_sha"]) == 64

    # Plain-text output also reports the attempt identity.
    assert main(["verify", "--run", outcome["run_id"], "--db", db]) == 0
    text = capsys.readouterr().out
    assert "attempt_id: att-" in text
    assert "attempt: completed" in text
    assert "snapshot:" in text


def test_verify_stale_snapshot_returns_retryable_payload(tmp_path, capsys, monkeypatch):
    db = tmp_path / "ledger.sqlite3"
    assert main(["run", "--contract", str(DEMO), "--out", str(tmp_path / "out"),
                 "--db", str(db), "--json"]) == 0
    _json_output(capsys)
    from cellcloak.ledger import Ledger

    run_id = Ledger(db).list_runs()[0]["id"]

    def flaky(ledger, run_id_arg):
        with sqlite3.connect(db) as conn:
            conn.execute(
                "UPDATE cells SET suppression_type='none' "
                "WHERE run_id = ? AND table_id = 'region_by_age' "
                "AND row_key = 'R01' AND col_key = '65+'",
                (run_id_arg,),
            )
        raise VerifySnapshotError(run_id_arg, -1, "a" * 64, "b" * 64)

    monkeypatch.setattr("cellcloak.cli.reverify_run", flaky)
    code = main(["verify", "--run", run_id, "--db", str(db), "--json"])
    assert code == 2
    payload = _json_output(capsys)
    assert payload["attempt_status"] == "stale"
    assert payload["retryable"] is True
    assert payload["error"] == "E_VERIFY_SNAPSHOT_CONFLICT"
    assert payload["passed"] is None

    code = main(["verify", "--run", run_id, "--db", str(db)])
    assert code == 2
    text = capsys.readouterr().out
    assert "attempt: stale" in text and "retry:" in text


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


def test_serve_refuses_non_loopback_and_invalid_port(tmp_path, capsys):
    db = str(tmp_path / "ledger.sqlite3")
    assert main(["serve", "--host", "0.0.0.0", "--port", "5017", "--db", db]) == 2
    assert "E_LOOPBACK_ONLY" in capsys.readouterr().err
    assert main(["serve", "--host", "127.0.0.1", "--port", "0", "--db", db]) == 2
    assert "E_SERVE_ARGUMENT" in capsys.readouterr().err
