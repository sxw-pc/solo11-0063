"""Tests for the optional verified input-set manifest."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from pathlib import Path

import pytest
import yaml

from cellcloak import manifest as manifest_module
from cellcloak import pipeline as pipeline_module
from cellcloak.cli import main
from cellcloak.contract import load_contract
from cellcloak.errors import CellcloakError, ManifestError
from cellcloak.ledger import Ledger
from cellcloak.manifest import (
    MANIFEST_SHA_FIELD,
    create_manifest,
    load_manifest,
)
from cellcloak.pipeline import execute_run

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "fixtures" / "demo" / "contract.yml"

CSV_HEADER = "row,col,count\n"


def _write_contract(base: Path, tables: list[dict]) -> Path:
    payload = {
        "version": 1,
        "dataset_name": "synthetic_manifest_test",
        "privacy": {"min_count": 5, "zero_policy": "structural", "max_suppression_ratio": 1.0},
        "tables": tables,
    }
    path = base / "contract.yml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _table(table_id: str, source: str) -> dict:
    return {
        "id": table_id,
        "source": source,
        "row_dim": "row",
        "col_dim": "col",
        "value": "count",
        "row_order": ["R1", "R2"],
        "col_order": ["C1", "C2"],
        "missing_cell_policy": "error",
    }


def _csv(value: int = 10) -> str:
    return CSV_HEADER + f"R1,C1,{value}\nR1,C2,{value}\nR2,C1,{value}\nR2,C2,{value}\n"


def _multi(base: Path, *, values: tuple[int, int] = (10, 20)) -> Path:
    base.mkdir(parents=True, exist_ok=True)
    (base / "a.csv").write_text(_csv(values[0]), encoding="utf-8")
    (base / "b.csv").write_text(_csv(values[1]), encoding="utf-8")
    return _write_contract(base, [_table("ta", "a.csv"), _table("tb", "b.csv")])


def _manifest_path(base: Path) -> Path:
    return base / "input-manifest.yml"


def _create(contract_path: Path) -> Path:
    manifest = create_manifest(load_contract(contract_path), _manifest_path(contract_path.parent))
    return manifest.path


def _read_manifest(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _write_manifest(path: Path, document: dict) -> None:
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


def _resign(document: dict) -> dict:
    body = {key: value for key, value in document.items() if key != MANIFEST_SHA_FIELD}
    canonical = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    document[MANIFEST_SHA_FIELD] = hashlib.sha256(canonical).hexdigest()
    return document


def _run(contract: Path, out: Path, db: Path, manifest: Path | None, **kwargs):
    return execute_run(contract, out, Ledger(db), manifest_path=manifest, **kwargs)


def test_manifest_binds_normalized_contract_and_ordered_members(tmp_path):
    contract_path = _multi(tmp_path)
    manifest = create_manifest(load_contract(contract_path), _manifest_path(tmp_path))
    loaded = load_manifest(manifest.path)
    contract = load_contract(contract_path)
    assert loaded.manifest_hash == manifest.manifest_hash
    assert loaded.contract_hash == contract.normalized_hash
    assert [member.table_id for member in loaded.members] == ["ta", "tb"]
    assert [member.source for member in loaded.members] == ["a.csv", "b.csv"]
    assert all(member.sha256 == hashlib.sha256((tmp_path / member.source).read_bytes()).hexdigest() for member in loaded.members)
    assert [member.size for member in loaded.members] == [
        (tmp_path / "a.csv").stat().st_size,
        (tmp_path / "b.csv").stat().st_size,
    ]


def test_manifest_signature_is_verified(tmp_path):
    contract_path = _multi(tmp_path)
    path = _create(contract_path)
    document = _read_manifest(path)
    document["members"][0]["sha256"] = "0" * 64
    _write_manifest(path, document)
    with pytest.raises(ManifestError) as info:
        load_manifest(path)
    assert info.value.code == "E_MANIFEST_INTEGRITY"


@pytest.mark.parametrize(
    "mutate,code,table_id",
    [
        (lambda doc: doc["members"].pop(1), "E_MANIFEST_MEMBER_MISSING", "tb"),
        (
            lambda doc: doc["members"].append(dict(doc["members"][0])),
            "E_MANIFEST_MEMBER_DUPLICATE",
            "ta",
        ),
        (
            lambda doc: doc["members"].append(
                {"table_id": "tz", "source": "z.csv", "sha256": "a" * 64, "size": 1}
            ),
            "E_MANIFEST_MEMBER_EXTRA",
            "tz",
        ),
        (
            lambda doc: doc["members"].reverse(),
            "E_MANIFEST_MEMBER_ORDER",
            None,
        ),
        (
            lambda doc: doc["members"][0].__setitem__("source", "../escape.csv"),
            "E_MANIFEST_SOURCE_PATH",
            None,
        ),
    ],
)
def test_member_set_validation(tmp_path, mutate, code, table_id):
    contract_path = _multi(tmp_path)
    path = _create(contract_path)
    document = _read_manifest(path)
    mutate(document)
    _resign(document)
    _write_manifest(path, document)
    with pytest.raises(ManifestError) as info:
        _run(contract_path, tmp_path / "out", tmp_path / "ledger.sqlite3", path)
    assert info.value.code == code
    if table_id is not None:
        assert info.value.table_id == table_id
    assert not (tmp_path / "out").exists()
    assert Ledger(tmp_path / "ledger.sqlite3").list_runs() == []


def test_contract_change_fails_before_public_output_and_ledger(tmp_path):
    contract_path = _multi(tmp_path)
    path = _create(contract_path)
    document = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
    document["privacy"]["min_count"] = 3
    contract_path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    with pytest.raises(ManifestError) as info:
        _run(contract_path, tmp_path / "out", tmp_path / "ledger.sqlite3", path)
    assert info.value.code == "E_MANIFEST_CONTRACT_MISMATCH"
    assert not (tmp_path / "out").exists()
    assert Ledger(tmp_path / "ledger.sqlite3").list_runs() == []


def test_csv_change_names_the_mismatching_member(tmp_path):
    contract_path = _multi(tmp_path)
    path = _create(contract_path)
    (tmp_path / "b.csv").write_text(_csv(99), encoding="utf-8")
    with pytest.raises(ManifestError) as info:
        _run(contract_path, tmp_path / "out", tmp_path / "ledger.sqlite3", path)
    assert info.value.code == "E_MANIFEST_MEMBER_CHANGED"
    assert info.value.table_id == "tb"
    assert "tb" in str(info.value)
    assert not (tmp_path / "out").exists()


def test_contract_source_moved_with_identical_bytes_is_rejected(tmp_path):
    contract_path = _multi(tmp_path)
    path = _create(contract_path)
    # Same bytes, different relative source path: the normalized contract
    # hash and the per-member source binding both change.
    (tmp_path / "b-renamed.csv").write_bytes((tmp_path / "b.csv").read_bytes())
    (tmp_path / "b.csv").unlink()
    document = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
    document["tables"][1]["source"] = "b-renamed.csv"
    contract_path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    with pytest.raises(ManifestError) as info:
        _run(contract_path, tmp_path / "out", tmp_path / "ledger.sqlite3", path)
    assert info.value.code in {"E_MANIFEST_CONTRACT_MISMATCH", "E_MANIFEST_MEMBER_SOURCE"}


def test_source_moved_away_is_rejected(tmp_path):
    contract_path = _multi(tmp_path)
    path = _create(contract_path)
    (tmp_path / "a.csv").unlink()
    with pytest.raises(CellcloakError) as info:
        _run(contract_path, tmp_path / "out", tmp_path / "ledger.sqlite3", path)
    # The contract loader refuses the missing source before capture; either way
    # the failure names the affected table and no output/ledger is written.
    assert info.value.code in {"E_SOURCE_MISSING", "E_MANIFEST_MEMBER_CHANGED"}
    assert info.value.table_id == "ta"
    assert not (tmp_path / "out").exists()
    assert Ledger(tmp_path / "ledger.sqlite3").list_runs() == []


@pytest.mark.parametrize("mode", ["bytes", "snapshot"])
def test_legal_replacement_between_verification_and_capture_is_rejected(tmp_path, monkeypatch, mode):
    contract_path = _multi(tmp_path)
    path = _create(contract_path)
    original_load = pipeline_module.load_manifest

    def load_then_replace(manifest_path):
        manifest = original_load(manifest_path)
        (tmp_path / "a.csv").write_text(_csv(77), encoding="utf-8")
        return manifest

    monkeypatch.setattr(pipeline_module, "load_manifest", load_then_replace)
    with pytest.raises(ManifestError) as info:
        _run(contract_path, tmp_path / "out", tmp_path / "ledger.sqlite3", path, input_mode=mode)
    assert info.value.code == "E_MANIFEST_MEMBER_CHANGED"
    assert info.value.table_id == "ta"


@pytest.mark.parametrize("mode", ["bytes", "snapshot"])
def test_replacement_during_capture_is_caught_by_post_recheck(tmp_path, monkeypatch, mode):
    contract_path = _multi(tmp_path)
    path = _create(contract_path)
    original_recheck = manifest_module.recheck_sources

    def recheck_after_replacement(contract, manifest):
        (tmp_path / "b.csv").write_text(_csv(88), encoding="utf-8")
        original_recheck(contract, manifest)

    monkeypatch.setattr(manifest_module, "recheck_sources", recheck_after_replacement)
    with pytest.raises(ManifestError) as info:
        _run(contract_path, tmp_path / "out", tmp_path / "ledger.sqlite3", path, input_mode=mode)
    assert info.value.code in {"E_MANIFEST_SOURCE_CHANGED", "E_MANIFEST_MEMBER_CHANGED"}
    assert info.value.table_id == "tb"
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("mode", ["bytes", "snapshot"])
def test_parse_uses_captured_bytes_not_reopened_sources(tmp_path, monkeypatch, mode):
    contract_path = _multi(tmp_path)
    path = _create(contract_path)
    original_capture = pipeline_module.capture_input_set

    def capture_then_replace(contract, manifest, **kwargs):
        captured = original_capture(contract, manifest, **kwargs)
        # A legal but different matrix appears after capture. The parse must
        # still use the captured bytes.
        (tmp_path / "a.csv").write_text(_csv(1), encoding="utf-8")
        (tmp_path / "b.csv").write_text(_csv(2), encoding="utf-8")
        return captured

    def forbidden_reopen(spec):
        raise AssertionError("manifest runs must not reopen live sources")

    monkeypatch.setattr(pipeline_module, "capture_input_set", capture_then_replace)
    monkeypatch.setattr(pipeline_module, "load_table", forbidden_reopen)
    outcome = _run(
        contract_path, tmp_path / "out", tmp_path / "ledger.sqlite3", path, input_mode=mode
    )
    assert outcome.status == "ok"
    assert outcome.report["input_set"]["pinned"] is True
    assert outcome.report["input_set"]["mode"] == f"manifest:{mode}"
    detail = Ledger(tmp_path / "ledger.sqlite3").get_run_detail(outcome.run_id)
    assert detail["run"]["input_set"]["pinned"] is True
    assert detail["run"]["input_set"]["manifest_hash"] == load_manifest(path).manifest_hash
    for table in detail["tables"]:
        assert table["meta"]["source_sha256"] == table["meta"]["source_sha256"]
        member = manifest_module.load_manifest(path).member(table["table_id"])
        assert table["meta"]["source_sha256"] == member.sha256
        assert table["meta"]["source_size"] == member.size


def test_manifest_run_records_identity_in_report_and_ledger(tmp_path):
    contract_path = _multi(tmp_path)
    path = _create(contract_path)
    outcome = _run(contract_path, tmp_path / "out", tmp_path / "ledger.sqlite3", path)
    report = json.loads((tmp_path / "out" / "audit" / "report.json").read_text(encoding="utf-8"))
    loaded = load_manifest(path)
    assert report["input_set"]["pinned"] is True
    assert report["input_set"]["manifest_hash"] == loaded.manifest_hash
    assert report["input_set"]["mode"] == "manifest:snapshot"
    assert report["normalized_contract_hash"] == load_contract(contract_path).normalized_hash
    assert [member["table_id"] for member in report["input_set"]["members"]] == ["ta", "tb"]
    for member in report["input_set"]["members"]:
        assert member["matches"] is True
        assert member["parsed_sha256"] == member["sha256"]
        assert member["parsed_size"] == member["size"]
    assert all(item["input_set_pinned"] is True for item in report["inputs"])
    assert all(item["manifest_sha256"] for item in report["inputs"])

    detail = Ledger(tmp_path / "ledger.sqlite3").get_run_detail(outcome.run_id)
    assert detail["run"]["input_set"]["manifest_hash"] == loaded.manifest_hash
    assert len(detail["run"]["input_set"]["members"]) == 2
    markdown = (tmp_path / "out" / "audit" / "report.md").read_text(encoding="utf-8")
    assert loaded.manifest_hash in markdown
    html = (tmp_path / "out" / "audit" / "report.html").read_text(encoding="utf-8")
    assert loaded.manifest_hash in html


def test_snapshot_files_are_cleaned_up_after_success(tmp_path):
    contract_path = _multi(tmp_path)
    path = _create(contract_path)
    snapshot_base = tmp_path / "snapshots"
    snapshot_base.mkdir()
    _run(
        contract_path,
        tmp_path / "out",
        tmp_path / "ledger.sqlite3",
        path,
        input_mode="snapshot",
        snapshot_dir=snapshot_base,
    )
    assert list(snapshot_base.iterdir()) == []


def test_snapshot_files_are_cleaned_up_after_failure(tmp_path, monkeypatch):
    contract_path = _multi(tmp_path)
    path = _create(contract_path)
    snapshot_base = tmp_path / "snapshots"
    snapshot_base.mkdir()

    def broken_parse(spec, raw, *, expected_sha256):
        raise RuntimeError("parse exploded")

    monkeypatch.setattr(pipeline_module, "load_table_from_bytes", broken_parse)
    with pytest.raises(RuntimeError):
        _run(
            contract_path,
            tmp_path / "out",
            tmp_path / "ledger.sqlite3",
            path,
            input_mode="snapshot",
            snapshot_dir=snapshot_base,
        )
    assert list(snapshot_base.iterdir()) == []


def test_unwritable_snapshot_directory_fails_before_outputs(tmp_path):
    if os.geteuid() == 0:
        pytest.skip("permission bits do not restrict root")
    contract_path = _multi(tmp_path)
    path = _create(contract_path)
    snapshot_base = tmp_path / "snapshots"
    snapshot_base.mkdir()
    snapshot_base.chmod(0o500)
    try:
        with pytest.raises(ManifestError) as info:
            _run(
                contract_path,
                tmp_path / "out",
                tmp_path / "ledger.sqlite3",
                path,
                input_mode="snapshot",
                snapshot_dir=snapshot_base,
            )
        assert info.value.code == "E_MANIFEST_SNAPSHOT"
    finally:
        snapshot_base.chmod(0o700)
    assert not (tmp_path / "out").exists()
    assert Ledger(tmp_path / "ledger.sqlite3").list_runs() == []


def test_snapshot_mode_rejects_argument_with_bytes_mode(tmp_path):
    contract_path = _multi(tmp_path)
    path = _create(contract_path)
    snapshot_base = tmp_path / "snapshots"
    snapshot_base.mkdir()
    with pytest.raises(ManifestError) as info:
        _run(
            contract_path,
            tmp_path / "out",
            tmp_path / "ledger.sqlite3",
            path,
            input_mode="bytes",
            snapshot_dir=snapshot_base,
        )
    assert info.value.code == "E_MANIFEST_ARGUMENT"


def test_legacy_run_without_manifest_is_marked_unpinned(tmp_path):
    contract_path = _multi(tmp_path)
    outcome = _run(contract_path, tmp_path / "out", tmp_path / "ledger.sqlite3", None)
    report = json.loads((tmp_path / "out" / "audit" / "report.json").read_text(encoding="utf-8"))
    assert report["input_set"]["pinned"] is False
    assert report["input_set"]["mode"] == "legacy"
    assert report["input_set"]["manifest_hash"] is None
    assert all(item["input_set_pinned"] is False for item in report["inputs"])
    assert report["inputs"][0]["sha256"]
    detail = Ledger(tmp_path / "ledger.sqlite3").get_run_detail(outcome.run_id)
    assert detail["run"]["input_set"]["pinned"] is False
    # Historical connection fields remain present and unchanged.
    assert detail["run"]["contract_hash"] == load_contract(contract_path).contract_hash
    assert {table["table_id"] for table in detail["tables"]} == {"ta", "tb"}


def test_historical_ledger_without_input_set_column_remains_readable(tmp_path):
    db = tmp_path / "old.sqlite3"
    with sqlite3.connect(db) as connection:
        connection.execute(
            """CREATE TABLE runs (
                id TEXT PRIMARY KEY,
                dataset_name TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT NOT NULL,
                contract_hash TEXT NOT NULL,
                contract_path TEXT NOT NULL,
                output_dir TEXT NOT NULL,
                summary_json TEXT NOT NULL
            )"""
        )
        connection.execute(
            "INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "run-old",
                "legacy",
                "ok",
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:00:01Z",
                "c" * 64,
                "/tmp/contract.yml",
                "/tmp/out",
                json.dumps({"table_count": 1, "suppression_ratio": 0.0}),
            ),
        )
    ledger = Ledger(db)
    runs = ledger.list_runs()
    assert runs[0]["id"] == "run-old"
    assert runs[0]["input_set"]["pinned"] is False
    detail = ledger.get_run_detail("run-old")
    assert detail["run"]["input_set"]["mode"] == "legacy"
    assert detail["run"]["input_set"]["members"] == []


def test_bundled_demo_manifest_runs_and_matches(tmp_path):
    manifest = load_manifest(ROOT / "fixtures" / "demo" / "input-manifest.yml")
    assert manifest.contract_hash == load_contract(DEMO).normalized_hash
    outcome = execute_run(
        DEMO,
        tmp_path / "out",
        Ledger(tmp_path / "ledger.sqlite3"),
        manifest_path=ROOT / "fixtures" / "demo" / "input-manifest.yml",
    )
    assert outcome.status == "ok"
    assert outcome.report["input_set"]["manifest_hash"] == manifest.manifest_hash


def test_contract_reformatting_keeps_normalized_hash_and_manifest_valid(tmp_path):
    contract_path = _multi(tmp_path)
    path = _create(contract_path)
    document = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
    # Reorder keys and add comments; effective semantics are unchanged.
    reformatted = (
        "# reformatted contract\n"
        + yaml.safe_dump(
            {
                "tables": document["tables"],
                "privacy": document["privacy"],
                "dataset_name": document["dataset_name"],
                "version": document["version"],
            },
            sort_keys=False,
        )
    )
    contract_path.write_text(reformatted, encoding="utf-8")
    outcome = _run(contract_path, tmp_path / "out", tmp_path / "ledger.sqlite3", path)
    assert outcome.status == "ok"
    assert outcome.report["normalized_contract_hash"] == load_manifest(path).contract_hash


def test_manifest_rejects_absolute_source_path(tmp_path):
    contract_path = _multi(tmp_path)
    path = _create(contract_path)
    document = _read_manifest(path)
    document["members"][0]["source"] = "/etc/passwd"
    _resign(document)
    _write_manifest(path, document)
    with pytest.raises(ManifestError) as info:
        load_manifest(path)
    assert info.value.code == "E_MANIFEST_SOURCE_PATH"


def test_manifest_rejects_unknown_fields(tmp_path):
    contract_path = _multi(tmp_path)
    path = _create(contract_path)
    document = _read_manifest(path)
    document["extra"] = "unexpected"
    _resign(document)
    _write_manifest(path, document)
    with pytest.raises(ManifestError) as info:
        load_manifest(path)
    assert info.value.code == "E_MANIFEST_SCHEMA"


def test_create_manifest_rejects_sources_changing_between_observations(tmp_path, monkeypatch):
    contract_path = _multi(tmp_path)
    real_hash_file = manifest_module._hash_file
    calls = {"a": 0}

    def flaky_hash_file(path):
        if path.name == "a.csv":
            calls["a"] += 1
            if calls["a"] == 2:
                return "f" * 64, 1
        return real_hash_file(path)

    monkeypatch.setattr(manifest_module, "_hash_file", flaky_hash_file)
    with pytest.raises(ManifestError) as info:
        create_manifest(load_contract(contract_path), _manifest_path(tmp_path))
    assert info.value.code == "E_MANIFEST_SOURCE_CHANGED"
    assert "ta" in str(info.value)
    assert not _manifest_path(tmp_path).exists()


def test_ledger_table_meta_carries_member_identity(tmp_path):
    contract_path = _multi(tmp_path)
    path = _create(contract_path)
    outcome = _run(contract_path, tmp_path / "out", tmp_path / "ledger.sqlite3", path)
    loaded = load_manifest(path)
    detail = Ledger(tmp_path / "ledger.sqlite3").get_run_detail(outcome.run_id)
    for table in detail["tables"]:
        member = loaded.member(table["table_id"])
        assert table["meta"]["input_set_pinned"] is True
        assert table["meta"]["manifest_sha256"] == member.sha256
        assert table["meta"]["input_set_manifest_hash"] == loaded.manifest_hash
        assert table["meta"]["source_sha256"] == member.sha256
        assert table["meta"]["source_size"] == member.size


def test_cli_manifest_and_manifest_run(tmp_path, capsys):
    contract_path = _multi(tmp_path)
    manifest_path = tmp_path / "manifest.yml"
    db = str(tmp_path / "ledger.sqlite3")
    assert main([
        "manifest", "--contract", str(contract_path), "--out", str(manifest_path), "--json",
    ]) == 0
    created = json.loads(capsys.readouterr().out)
    assert len(created["manifest_hash"]) == 64
    assert [member["table_id"] for member in created["members"]] == ["ta", "tb"]

    assert main([
        "run", "--contract", str(contract_path), "--out", str(tmp_path / "out"),
        "--manifest", str(manifest_path), "--db", db, "--json",
    ]) == 0
    outcome = json.loads(capsys.readouterr().out)
    assert outcome["input_set"]["pinned"] is True
    assert outcome["input_set"]["manifest_hash"] == created["manifest_hash"]

    assert main([
        "run", "--contract", str(contract_path), "--out", str(tmp_path / "out-bytes"),
        "--manifest", str(manifest_path), "--input-mode", "bytes", "--db", db, "--json",
    ]) == 0
    bytes_outcome = json.loads(capsys.readouterr().out)
    assert bytes_outcome["input_set"]["mode"] == "manifest:bytes"


def test_cli_manifest_failure_returns_two_without_traceback(tmp_path, capsys):
    contract_path = _multi(tmp_path)
    manifest_path = tmp_path / "manifest.yml"
    assert main([
        "manifest", "--contract", str(contract_path), "--out", str(manifest_path),
    ]) == 0
    capsys.readouterr()
    (tmp_path / "a.csv").write_text(_csv(55), encoding="utf-8")
    code = main([
        "run", "--contract", str(contract_path), "--out", str(tmp_path / "out"),
        "--manifest", str(manifest_path), "--db", str(tmp_path / "ledger.sqlite3"),
    ])
    captured = capsys.readouterr()
    assert code == 2
    assert "E_MANIFEST_MEMBER_CHANGED" in captured.err
    assert "ta" in captured.err
    assert "Traceback" not in captured.err
    assert not (tmp_path / "out").exists()
