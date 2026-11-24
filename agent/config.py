"""Central configuration. Everything comes from environment variables so the same
code runs on the Mac (host) and inside the Airflow container."""
from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


# --- LLM -------------------------------------------------------------------
MODEL = _env("ANTHROPIC_MODEL", "claude-sonnet-4-6")
MAX_TOKENS = int(_env("AGENT_MAX_TOKENS", "1500"))
MAX_TURNS = int(_env("AGENT_MAX_TURNS", "8"))
# Cost estimate only (USD per million tokens). Override if your model's pricing differs.
PRICE_IN_PER_MTOK = float(_env("AGENT_PRICE_IN_PER_MTOK", "3.0"))
PRICE_OUT_PER_MTOK = float(_env("AGENT_PRICE_OUT_PER_MTOK", "15.0"))

# --- Postgres (dedicated least-privilege role, see agent/sql/setup.sql) -----
DB_HOST = _env("AGENT_DB_HOST", "localhost")
DB_PORT = int(_env("AGENT_DB_PORT", "5433"))  # 5433 on the Mac, 5432 inside Docker
DB_NAME = _env("AGENT_DB_NAME", _env("POSTGRES_DB", "footyflow_db"))
DB_USER = _env("AGENT_DB_USER", "footyflow_agent")
DB_PASSWORD = _env("AGENT_DB_PASSWORD")
QUERY_TIMEOUT_MS = int(_env("AGENT_QUERY_TIMEOUT_MS", "5000"))
MAX_ROWS = int(_env("AGENT_MAX_ROWS", "50"))
MAX_TOOL_CHARS = int(_env("AGENT_MAX_TOOL_CHARS", "6000"))

# --- Airflow ---------------------------------------------------------------
AIRFLOW_URL = _env("AIRFLOW_URL", "http://localhost:8081").rstrip("/")
AIRFLOW_USER = _env("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = _env("AIRFLOW_PASSWORD", "admin")  # local dev default from docker-compose
DAG_ID = "footyflow_pipeline"
ALLOWED_TASKS = ("run_producer", "run_consumer", "dbt_run", "dbt_test")

# --- Optional alerting -----------------------------------------------------
SLACK_WEBHOOK_URL = _env("SLACK_WEBHOOK_URL")


def log_dir() -> Path:
    """Airflow task logs: /opt/airflow/logs in the container, ./airflow/logs on the host."""
    override = _env("AIRFLOW_LOG_DIR")
    if override:
        return Path(override)
    for candidate in (Path("/opt/airflow/logs"), REPO_ROOT / "airflow" / "logs"):
        if candidate.exists():
            return candidate
    return REPO_ROOT / "airflow" / "logs"


def dbt_results_path() -> Path:
    override = _env("DBT_RESULTS_PATH")
    return Path(override) if override else REPO_ROOT / "transform" / "target" / "run_results.json"
