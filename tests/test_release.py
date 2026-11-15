"""Release-generation commit protocol: interruption, retry, and readback."""

import hashlib
import json
import os
from pathlib import Path

import pytest

from cellcloak import report as report_module
from cellcloak.errors import LedgerError, PipelineError
from cellcloak.ledger import Ledger
from cellcloak.pipeline import execute_run
from cellcloak.report import MARKER_NAME, read_current_release

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "fixtures/demo/contract.yml"
BLOCKED = ROOT / "fixtures/blocked/contract.yml"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _assert_artifacts_verifiable(ledger: Ledger, run_id: str) -> None:
    """Every ledger artifact of the run still exists with a matching digest."""

    detail = ledger.get_run_detail(run_id)
    assert detail is not None
    assert detail["artifacts"], run_id
    for artifact in detail["artifacts"]:
        path = Path(artifact["path"])
        assert path.is_file(), artifact["path"]
        assert _sha256(path) == artifact["sha256"]


def _current_target(out: Path) -> str:
    return os.readlink(out / "current")


def test_single_run_commits_one_generation(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    out = tmp_path / "out"
    outcome = execute_run(DEMO, out, ledger, run_id="run-g1")
    assert outcome.status == "ok"

    # The whole release lives in one immutable generation directory.
    generation = out / "generations" / "run-g1"
    assert generation.is_dir()
    assert {path.name for path in (generation / "public").glob("*.csv")} == {
        "age_by_sex.csv",
        "region_by_age.csv",
    }
    assert {path.name for path in (generation / "audit").iterdir()} == {
        "report.json",
        "report.md",
        "report.html",
    }
    marker = json.loads((generation / MARKER_NAME).read_text(encoding="utf-8"))
    assert marker["version"] == 2
    assert marker["run_id"] == "run-g1"
    assert marker["status"] == "ok"
    assert sorted(marker["artifacts"]) == sorted(
        ["public/age_by_sex.csv", "public/region_by_age.csv",
         "audit/report.json", "audit/report.md", "audit/report.html"]
    )

    # The browsing paths resolve through the single `current` pointer.
    assert _current_target(out) == "generations/run-g1"
    assert (out / "public").is_symlink()
    assert (out / "audit").is_symlink()
    assert (out / MARKER_NAME).is_symlink()
    assert (out / "public" / "age_by_sex.csv").is_file()
    browsed = json.loads((out / "audit" / "report.json").read_text(encoding="utf-8"))
    assert browsed["run_id"] == "run-g1"

    # The ledger references the sealed files, and the digests match.
    detail = ledger.get_run_detail("run-g1")
    for artifact in detail["artifacts"]:
        path = Path(artifact["path"])
        assert path.parent.parent == generation
        assert path.is_relative_to(out / "generations")
    _assert_artifacts_verifiable(ledger, "run-g1")

    release = read_current_release(out)
    assert release is not None
    assert release["layout"] == "generations"
    assert release["run_id"] == "run-g1"
    assert release["status"] == "ok"
    assert all(path.is_file() for path in release["artifacts"])


def test_repeated_runs_flip_pointer_and_keep_old_generation(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    out = tmp_path / "out"
    first = execute_run(DEMO, out, ledger, run_id="run-first")
    second = execute_run(DEMO, out, ledger, run_id="run-second")
    assert first.run_id != second.run_id

    # Only the newest complete generation is the current release.
    assert _current_target(out) == "generations/run-second"
    release = read_current_release(out)
    assert release["run_id"] == "run-second"
    browsed = json.loads((out / "audit" / "report.json").read_text(encoding="utf-8"))
    assert browsed["run_id"] == "run-second"
    assert (out / "public" / "age_by_sex.csv").read_bytes() == (
        out / "generations" / "run-second" / "public" / "age_by_sex.csv"
    ).read_bytes()

    # The superseded generation stays on disk and stays verifiable: old
    # version readback works through both the ledger and its own marker.
    _assert_artifacts_verifiable(ledger, "run-first")
    _assert_artifacts_verifiable(ledger, "run-second")
    old_marker = json.loads(
        (out / "generations" / "run-first" / MARKER_NAME).read_text(encoding="utf-8")
    )
    assert old_marker["run_id"] == "run-first"
    assert (out / "generations" / "run-first" / "public" / "age_by_sex.csv").is_file()


@pytest.mark.parametrize("fail_at", range(1, 7))
def test_interruption_at_each_file_boundary_keeps_previous_release(tmp_path, monkeypatch, fail_at):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    out = tmp_path / "out"
    execute_run(DEMO, out, ledger, run_id="run-stable")
    stable_report = (out / "audit" / "report.json").read_bytes()

    # The demo contract performs six file writes in order: two public CSVs,
    # then audit JSON, Markdown, HTML, and finally the marker.
    real_csv = report_module._public_csv
    real_text = report_module._write_text
    counter = {"calls": 0}

    def _maybe_fail():
        counter["calls"] += 1
        if counter["calls"] == fail_at:
            raise RuntimeError(f"simulated crash at write {fail_at}")

    def counting_csv(*args, **kwargs):
        _maybe_fail()
        return real_csv(*args, **kwargs)

    def counting_text(*args, **kwargs):
        _maybe_fail()
        return real_text(*args, **kwargs)

    monkeypatch.setattr(report_module, "_public_csv", counting_csv)
    monkeypatch.setattr(report_module, "_write_text", counting_text)
    with pytest.raises(RuntimeError, match="simulated crash"):
        execute_run(DEMO, out, ledger, run_id="run-crash")

    # The half-written generation never became the current release...
    assert _current_target(out) == "generations/run-stable"
    release = read_current_release(out)
    assert release["run_id"] == "run-stable"
    assert (out / "audit" / "report.json").read_bytes() == stable_report

    # ...it has no ledger record and left no sealed or staged files behind...
    assert ledger.get_run_detail("run-crash") is None
    assert not (out / "generations" / "run-crash").exists()
    assert list((out / "generations").glob(".staging-*")) == []

    # ...and the previous release is still fully verifiable.
    _assert_artifacts_verifiable(ledger, "run-stable")

    # A retry with a fresh run ID recovers and becomes the current release.
    monkeypatch.undo()
    execute_run(DEMO, out, ledger, run_id="run-retry")
    assert _current_target(out) == "generations/run-retry"
    _assert_artifacts_verifiable(ledger, "run-retry")
    _assert_artifacts_verifiable(ledger, "run-stable")


def test_ledger_commit_failure_leaves_previous_release_current(tmp_path, monkeypatch):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    out = tmp_path / "out"
    execute_run(DEMO, out, ledger, run_id="run-committed")

    def failing_record(*args, **kwargs):
        raise LedgerError("E_LEDGER_DB", "simulated ledger outage")

    monkeypatch.setattr(Ledger, "record_run", failing_record)
    with pytest.raises(LedgerError):
        execute_run(DEMO, out, ledger, run_id="run-orphan")

    # The pointer never moved and the previous release is intact.
    assert _current_target(out) == "generations/run-committed"
    release = read_current_release(out)
    assert release["run_id"] == "run-committed"
    _assert_artifacts_verifiable(ledger, "run-committed")

    # The failed run is not in the ledger; its sealed generation stays on
    # disk as an unreferenced orphan that never becomes the current release.
    assert ledger.get_run_detail("run-orphan") is None
    orphan = out / "generations" / "run-orphan"
    assert orphan.is_dir()
    orphan_marker = json.loads((orphan / MARKER_NAME).read_text(encoding="utf-8"))
    assert orphan_marker["run_id"] == "run-orphan"

    # A later run still publishes normally; the orphan stays unreferenced.
    monkeypatch.undo()
    execute_run(DEMO, out, ledger, run_id="run-after")
    assert _current_target(out) == "generations/run-after"
    assert orphan.is_dir()
    assert read_current_release(out)["run_id"] == "run-after"


def test_publish_failure_keeps_ledger_and_previous_release_consistent(tmp_path, monkeypatch):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    out = tmp_path / "out"
    execute_run(DEMO, out, ledger, run_id="run-before")

    def failing_symlink(*args, **kwargs):
        raise OSError("simulated pointer-flip failure")

    monkeypatch.setattr(Path, "symlink_to", failing_symlink)
    with pytest.raises(PipelineError) as info:
        execute_run(DEMO, out, ledger, run_id="run-unpublished")
    assert info.value.code == "E_OUTPUT_PUBLISH"

    # The pointer flip failed, so the previous release is still current...
    assert _current_target(out) == "generations/run-before"
    assert read_current_release(out)["run_id"] == "run-before"
    _assert_artifacts_verifiable(ledger, "run-before")

    # ...but the run was committed to the ledger first, and its sealed
    # generation is complete and verifiable on disk.
    detail = ledger.get_run_detail("run-unpublished")
    assert detail is not None
    assert detail["run"]["status"] == "ok"
    _assert_artifacts_verifiable(ledger, "run-unpublished")


def test_blocked_run_publishes_audit_only_and_preserves_previous(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    out = tmp_path / "out"
    execute_run(DEMO, out, ledger, run_id="run-ok")
    blocked = execute_run(BLOCKED, out, ledger, run_id="run-blocked")
    assert blocked.status == "blocked"

    # The blocked run becomes current with audit reports but no public tables.
    assert _current_target(out) == "generations/run-blocked"
    assert list((out / "public").glob("*.csv")) == []
    report = json.loads((out / "audit" / "report.json").read_text(encoding="utf-8"))
    assert report["run_id"] == "run-blocked"
    assert report["status"] == "blocked"
    release = read_current_release(out)
    assert release["run_id"] == "run-blocked"
    assert release["status"] == "blocked"

    detail = ledger.get_run_detail("run-blocked")
    assert {artifact["kind"] for artifact in detail["artifacts"]} == {
        "audit_json",
        "audit_md",
        "audit_html",
    }
    _assert_artifacts_verifiable(ledger, "run-blocked")

    # The superseded ok generation keeps its public tables and verifies.
    assert (out / "generations" / "run-ok" / "public" / "age_by_sex.csv").is_file()
    _assert_artifacts_verifiable(ledger, "run-ok")


def test_legacy_marker_directory_is_read_and_adopted(tmp_path):
    out = tmp_path / "out"
    (out / "public").mkdir(parents=True)
    (out / "audit").mkdir()
    (out / "public" / "old.csv").write_text("legacy,csv\n", encoding="utf-8")
    (out / "audit" / "report.json").write_text('{"run_id": "run-legacy"}\n', encoding="utf-8")
    (out / MARKER_NAME).write_text(
        json.dumps(
            {
                "version": 1,
                "run_id": "run-legacy",
                "artifacts": ["public/old.csv", "audit/report.json"],
            }
        ),
        encoding="utf-8",
    )

    # Compatible read of the pre-existing marker directory.
    release = read_current_release(out)
    assert release is not None
    assert release["layout"] == "legacy"
    assert release["run_id"] == "run-legacy"
    assert release["generation_dir"] == out.resolve()
    assert [path.name for path in release["artifacts"]] == ["old.csv", "report.json"]
    assert all(path.is_file() for path in release["artifacts"])

    # Publishing adopts the directory: legacy content is archived whole, not
    # deleted, and the new run becomes the only current release.
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    execute_run(DEMO, out, ledger, run_id="run-new")
    assert _current_target(out) == "generations/run-new"
    assert read_current_release(out)["run_id"] == "run-new"
    archive = out / "generations" / "legacy-run-legacy"
    assert (archive / "public" / "old.csv").read_text(encoding="utf-8") == "legacy,csv\n"
    assert json.loads((archive / "audit" / "report.json").read_text(encoding="utf-8"))[
        "run_id"
    ] == "run-legacy"
    archived_marker = json.loads((archive / MARKER_NAME).read_text(encoding="utf-8"))
    assert archived_marker["run_id"] == "run-legacy"
    assert not (out / MARKER_NAME).is_file() or (out / MARKER_NAME).is_symlink()
    _assert_artifacts_verifiable(ledger, "run-new")


def test_invalid_legacy_marker_is_refused(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    out = tmp_path / "out"
    out.mkdir()
    marker = out / MARKER_NAME
    marker.write_text("not json{", encoding="utf-8")
    with pytest.raises(PipelineError) as info:
        execute_run(DEMO, out, ledger, run_id="run-x")
    assert info.value.code == "E_OUTPUT_MARKER"
    assert marker.read_text(encoding="utf-8") == "not json{"
    assert ledger.list_runs() == []

    marker.write_text(json.dumps({"version": 1, "run_id": "run-legacy", "artifacts": "nope"}),
                      encoding="utf-8")
    with pytest.raises(PipelineError) as info:
        execute_run(DEMO, out, ledger, run_id="run-y")
    assert info.value.code == "E_OUTPUT_MARKER"
    assert ledger.list_runs() == []


def test_stale_staging_directories_are_cleaned_on_next_run(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    out = tmp_path / "out"
    execute_run(DEMO, out, ledger, run_id="run-one")
    stale = out / "generations" / ".staging-run-interrupted"
    stale.mkdir()
    (stale / "partial.txt").write_text("half-written\n", encoding="utf-8")

    execute_run(DEMO, out, ledger, run_id="run-two")
    assert not stale.exists()
    assert _current_target(out) == "generations/run-two"
    _assert_artifacts_verifiable(ledger, "run-one")
    _assert_artifacts_verifiable(ledger, "run-two")


def test_duplicate_run_id_is_refused_before_anything_changes(tmp_path):
    ledger = Ledger(tmp_path / "ledger.sqlite3")
    out = tmp_path / "out"
    execute_run(DEMO, out, ledger, run_id="run-dup")
    with pytest.raises(PipelineError) as info:
        execute_run(DEMO, out, ledger, run_id="run-dup")
    assert info.value.code == "E_RUN_DUPLICATE"
    assert _current_target(out) == "generations/run-dup"
    assert len(ledger.list_runs()) == 1
    _assert_artifacts_verifiable(ledger, "run-dup")


def test_empty_output_directory_has_no_current_release(tmp_path):
    assert read_current_release(tmp_path / "missing") is None
    assert read_current_release(tmp_path) is None
