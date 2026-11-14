"""Local runtime defaults. No remote services are ever configured here."""

from __future__ import annotations

import os
import secrets
from pathlib import Path

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5017
LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}
DEFAULT_MAX_ROUNDS = 100
MAX_ROUNDS_CEILING = 100_000


def default_db_path() -> Path:
    """Return the ledger location, overridable with CELLCLOAK_DB."""

    override = os.environ.get("CELLCLOAK_DB")
    if override:
        return Path(override).expanduser().resolve()
    return (Path.cwd() / "var" / "cellcloak.sqlite3").resolve()


def default_web_output() -> Path:
    override = os.environ.get("CELLCLOAK_WEB_OUT")
    if override:
        return Path(override).expanduser().resolve()
    return (Path.cwd() / "var" / "web-demo").resolve()


def project_root() -> Path:
    """Best-effort repository root for the bundled demo fixture."""

    return Path(__file__).resolve().parents[2]


def demo_contract_path() -> Path:
    return project_root() / "fixtures" / "demo" / "contract.yml"


def blocked_contract_path() -> Path:
    return project_root() / "fixtures" / "blocked" / "contract.yml"


def require_loopback(host: str) -> str:
    normalized = (host or "").strip().lower()
    if normalized not in LOOPBACK_HOSTS:
        raise ValueError(
            "refusing non-loopback host; use 127.0.0.1, localhost, or ::1"
        )
    return normalized


def flask_secret() -> str:
    # A random per-process value is sufficient for local flash messages.
    return os.environ.get("CELLCLOAK_SECRET") or secrets.token_hex(24)


def validate_port(port: int) -> int:
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        raise ValueError("port must be an integer between 1 and 65535")
    return port
