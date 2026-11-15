"""Command-line entry point for the local Cellcloak pipeline."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .config import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    default_db_path,
    default_web_output,
    require_loopback,
    validate_port,
)
from .diffing import compare_runs
from .errors import CellcloakError
from .ledger import Ledger
from .manifest import DEFAULT_INPUT_MODE, INPUT_MODES, create_manifest
from .pipeline import execute_run, reverify_run


def _ledger(args: argparse.Namespace) -> Ledger:
    return Ledger(Path(args.db).expanduser() if args.db else default_db_path())


def _print_json(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _cmd_run(args: argparse.Namespace) -> int:
    outcome = execute_run(
        args.contract,
        args.out,
        _ledger(args),
        manifest_path=args.manifest,
        input_mode=args.input_mode,
        snapshot_dir=args.snapshot_dir,
    )
    if args.json:
        payload = {
            "run_id": outcome.run_id,
            "status": outcome.status,
            "output_dir": str(outcome.output_dir),
            "summary": outcome.report["summary"],
            "artifacts": [str(artifact.path) for artifact in outcome.artifacts],
        }
        if outcome.report.get("input_set", {}).get("pinned"):
            payload["input_set"] = outcome.report["input_set"]
        _print_json(payload)
    else:
        summary = outcome.report["summary"]
        input_set = outcome.report.get("input_set", {})
        print(f"run_id: {outcome.run_id}")
        print(f"status: {outcome.status}")
        print(f"tables: {summary['table_count']}  cells: {summary['cell_count']}")
        print(
            f"suppression: {summary['suppressed_cell_count']} cells "
            f"({summary['suppression_ratio']:.2%})"
        )
        if input_set.get("pinned"):
            print(
                f"input set: pinned ({input_set.get('mode')}) "
                f"manifest={input_set.get('manifest_hash')}"
            )
        print(f"output: {outcome.output_dir}")
        for reason in summary["block_reasons"]:
            print(f"blocked: {reason['code']} ({reason['table_id']}) {reason['detail']}")
    if outcome.status == "blocked":
        return 3
    if outcome.status == "verify_failed":
        return 4
    return 0


def _cmd_manifest(args: argparse.Namespace) -> int:
    from .contract import load_contract

    contract = load_contract(args.contract)
    manifest = create_manifest(contract, args.out)
    if args.json:
        _print_json(
            {
                "manifest": str(manifest.path),
                "manifest_hash": manifest.manifest_hash,
                "contract_hash": manifest.contract_hash,
                "members": [member.as_record() for member in manifest.members],
            }
        )
    else:
        print(f"manifest: {manifest.path}")
        print(f"manifest_sha256: {manifest.manifest_hash}")
        print(f"contract_hash: {manifest.contract_hash}")
        print(f"members: {len(manifest.members)}")
        for member in manifest.members:
            print(f"  {member.table_id}: {member.source} {member.sha256} ({member.size} bytes)")
    return 0


def _cmd_runs(args: argparse.Namespace) -> int:
    runs = _ledger(args).list_runs(limit=args.limit)
    if args.json:
        _print_json(runs)
        return 0
    if not runs:
        print("no runs recorded")
        return 0
    print("RUN ID                                      STATUS        RATIO   PRIMARY  SECONDARY  DATASET")
    for run in runs:
        print(
            f"{run['id']:<43} {run['status']:<13} "
            f"{run['suppression_ratio']:>6.2%}  {run['primary_count']:>7}  "
            f"{run['secondary_count']:>9}  {run['dataset_name']}"
        )
    return 0


def _cmd_verify(args: argparse.Namespace) -> int:
    outcome = reverify_run(_ledger(args), args.run)
    if args.json:
        _print_json(
            {
                "run_id": outcome.run_id,
                "passed": outcome.passed,
                "status": outcome.status,
                "tables": outcome.tables,
            }
        )
    else:
        print(f"run_id: {outcome.run_id}")
        print(f"verify: {'passed' if outcome.passed else 'failed'}")
        print(f"status: {outcome.status}")
        for table in outcome.tables:
            print(f"  {table['table_id']}: {'pass' if table['passed'] else 'fail'}")
    return 0 if outcome.passed else 4


def _cmd_diff(args: argparse.Namespace) -> int:
    result = compare_runs(_ledger(args), args.run_a, args.run_b)
    if args.json:
        _print_json(result)
        return 0
    summary = result["summary"]
    print(f"run A: {result['run_a']['id']} ({result['run_a']['status']})")
    print(f"run B: {result['run_b']['id']} ({result['run_b']['status']})")
    print(f"status changed: {summary['status_changed']}")
    print(f"blocked changed: {summary['blocked_changed']}")
    print(f"suppression ratio delta: {summary['suppression_ratio_delta']:+.2%}")
    print(f"primary delta: {summary['primary_delta']:+d}")
    print(f"secondary delta: {summary['secondary_delta']:+d}")
    print(f"public output hash changed: {summary['public_hash_changed']}")
    for table in result["tables"]:
        if table["changed"]:
            print(
                f"  {table['table_id']}: ratio {table['ratio_a']:.2%} -> {table['ratio_b']:.2%}, "
                f"primary {table['primary_a']} -> {table['primary_b']}, "
                f"secondary {table['secondary_a']} -> {table['secondary_b']}"
            )
    return 0


def _cmd_serve(args: argparse.Namespace) -> int:
    try:
        host = require_loopback(args.host)
    except ValueError as exc:
        print(f"E_LOOPBACK_ONLY: {exc}", file=sys.stderr)
        return 2
    try:
        port = validate_port(args.port)
    except ValueError as exc:
        print(f"E_SERVE_ARGUMENT: {exc}", file=sys.stderr)
        return 2
    from .web import create_app

    app = create_app(db_path=args.db, demo_out=args.demo_out)
    print(f"serving on http://{host}:{port} (loopback only)")
    app.run(host=host, port=port, debug=False, use_reloader=False)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cellcloak", description="Local small-cell disclosure review")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="execute the full suppression pipeline")
    run.add_argument("--contract", required=True)
    run.add_argument("--out", required=True)
    run.add_argument("--manifest", help="verified input-set manifest for all tables")
    run.add_argument(
        "--input-mode",
        choices=INPUT_MODES,
        default=DEFAULT_INPUT_MODE,
        help="manifest capture strategy: snapshot (temporary files, default) or bytes (in memory)",
    )
    run.add_argument("--snapshot-dir", help="parent directory for snapshot mode temporary files")
    run.add_argument("--db")
    run.add_argument("--json", action="store_true")
    run.set_defaults(func=_cmd_run)

    manifest = sub.add_parser("manifest", help="create an input-set manifest from the current sources")
    manifest.add_argument("--contract", required=True)
    manifest.add_argument("--out", required=True)
    manifest.add_argument("--json", action="store_true")
    manifest.set_defaults(func=_cmd_manifest)

    verify = sub.add_parser("verify", help="re-verify a recorded run independently")
    verify.add_argument("--run", required=True)
    verify.add_argument("--db")
    verify.add_argument("--json", action="store_true")
    verify.set_defaults(func=_cmd_verify)

    runs = sub.add_parser("runs", help="list recent recorded runs")
    runs.add_argument("--limit", type=int, default=20)
    runs.add_argument("--db")
    runs.add_argument("--json", action="store_true")
    runs.set_defaults(func=_cmd_runs)

    diff = sub.add_parser("diff", help="compare two recorded runs")
    diff.add_argument("--run-a", required=True)
    diff.add_argument("--run-b", required=True)
    diff.add_argument("--db")
    diff.add_argument("--json", action="store_true")
    diff.set_defaults(func=_cmd_diff)

    serve = sub.add_parser("serve", help="start the loopback-only local web UI")
    serve.add_argument("--host", default=DEFAULT_HOST)
    serve.add_argument("--port", type=int, default=DEFAULT_PORT)
    serve.add_argument("--db")
    serve.add_argument("--demo-out", default=str(default_web_output()))
    serve.set_defaults(func=_cmd_serve)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except CellcloakError as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        return 2
    except (OSError, ValueError) as exc:
        print(f"ERROR E_RUNTIME: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # keep CLI output stable and stack-free
        print(f"ERROR E_INTERNAL: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
