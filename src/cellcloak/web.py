"""Loopback-only Flask UI for local run inspection."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from flask import Flask, abort, flash, redirect, render_template, request, url_for

from .config import (
    default_db_path,
    default_web_output,
    demo_contract_path,
    flask_secret,
)
from .diffing import compare_runs
from .errors import CellcloakError, LedgerError
from .ledger import Ledger
from .pipeline import execute_run


def _checks_by_table(checks: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for check in checks:
        grouped.setdefault(check.get("table_id") or "run", []).append(check)
    return grouped


def _attempt_groups(detail: dict[str, Any]) -> list[dict[str, Any]]:
    """Shape verification attempts for the run page, newest attempts last."""

    groups: list[dict[str, Any]] = []
    for attempt in detail.get("attempts", []):
        groups.append(
            {
                "attempt_id": attempt["attempt_id"],
                "sequence_no": attempt["sequence_no"],
                "source": attempt["source"],
                "status": attempt["status"],
                "result": attempt["result"],
                "snapshot_sha": attempt["snapshot_sha"],
                "started_at": attempt["started_at"],
                "finished_at": attempt["finished_at"],
                "checks_by_table": _checks_by_table(attempt["checks"]),
                "table_ids": sorted(
                    {check.get("table_id") or "run" for check in attempt["checks"]}
                ),
            }
        )
    return groups


def create_app(
    db_path: str | Path | None = None,
    demo_out: str | Path | None = None,
    *,
    testing: bool = False,
) -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config.update(
        SECRET_KEY=flask_secret(),
        TESTING=testing,
        MAX_CONTENT_LENGTH=64 * 1024,
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
    )
    ledger = Ledger(db_path or default_db_path())
    demo_output = Path(demo_out or default_web_output()).expanduser().resolve()
    contract = demo_contract_path()
    app.extensions["ledger"] = ledger

    @app.after_request
    def _headers(response: Any) -> Any:
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'none'; style-src 'self'; "
            "img-src 'self' data:; base-uri 'none'; form-action 'self'; frame-ancestors 'none'"
        )
        return response

    @app.get("/")
    def index() -> str:
        runs = ledger.list_runs(limit=20)
        return render_template(
            "index.html",
            runs=runs,
            demo_available=contract.is_file(),
            page="home",
        )

    @app.get("/runs/<run_id>")
    def run_detail(run_id: str) -> str:
        detail = ledger.get_run_detail(run_id)
        if detail is None:
            abort(404)
        grouped = _checks_by_table(detail["checks"])
        attempt_groups = _attempt_groups(detail)
        artifacts = [
            {"name": Path(item["path"]).name, "kind": item["kind"], "sha256": item["sha256"]}
            for item in detail["artifacts"]
        ]
        return render_template(
            "run.html",
            detail=detail,
            run=detail["run"],
            tables=detail["tables"],
            checks_by_table=grouped,
            attempt_groups=attempt_groups,
            artifacts=artifacts,
            page="run",
        )

    @app.get("/diff")
    def diff_page() -> str:
        recent = ledger.list_runs(limit=50)
        run_a = request.args.get("run_a", "").strip()
        run_b = request.args.get("run_b", "").strip()
        comparison: dict[str, Any] | None = None
        error: str | None = None
        if run_a and run_b:
            try:
                comparison = compare_runs(ledger, run_a, run_b)
            except LedgerError:
                error = "找不到指定的运行记录。"
        return render_template(
            "diff.html",
            runs=recent,
            comparison=comparison,
            error=error,
            run_a=run_a,
            run_b=run_b,
            page="diff",
        )

    @app.post("/demo")
    def run_demo() -> Any:
        if not contract.is_file():
            flash("内置演示契约不可用。", "error")
            return redirect(url_for("index"))
        try:
            outcome = execute_run(contract, demo_output, ledger)
        except CellcloakError as exc:
            flash(f"演示运行失败：{exc.code}", "error")
            return redirect(url_for("index"))
        flash(f"演示运行完成：{outcome.status}", "ok" if outcome.status == "ok" else "warn")
        return redirect(url_for("run_detail", run_id=outcome.run_id))

    @app.errorhandler(404)
    def not_found(_: Any) -> tuple[str, int]:
        return render_template("error.html", code=404, message="页面不存在。", page="error"), 404

    @app.errorhandler(500)
    def server_error(_: Any) -> tuple[str, int]:
        return render_template("error.html", code=500, message="本地服务处理请求时出错。", page="error"), 500

    @app.errorhandler(413)
    def too_large(_: Any) -> tuple[str, int]:
        return render_template("error.html", code=413, message="请求内容过大。", page="error"), 413

    return app
