"""Postgres access for the agent. Imports psycopg2 lazily so the pure-logic modules stay testable."""
from __future__ import annotations

from . import config, guardrails


def _connect_kwargs(read_only: bool, search_path: str) -> dict:
    opts = f"-c search_path={search_path} -c statement_timeout={config.QUERY_TIMEOUT_MS}"
    if read_only:
        opts += " -c default_transaction_read_only=on"
    return dict(
        host=config.DB_HOST, port=config.DB_PORT, dbname=config.DB_NAME,
        user=config.DB_USER, password=config.DB_PASSWORD,
        connect_timeout=5, options=opts, application_name="footyflow-ops-agent",
    )


def connect_read_only():
    import psycopg2
    return psycopg2.connect(**_connect_kwargs(True, "analytics,public"))


def connect_ops():
    """Writable connection, only used for the agent_ops logging tables."""
    import psycopg2
    conn = psycopg2.connect(**_connect_kwargs(False, "agent_ops"))
    conn.autocommit = True
    return conn


def _fetch(conn, sql: str, params=None):
    conn.set_session(readonly=True, autocommit=False)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    conn.rollback()
    return cols, rows


def run_readonly_query(sql: str) -> dict:
    """Validate, wrap with a row cap and run inside a read-only transaction."""
    safe = guardrails.validate_select(sql)
    wrapped = guardrails.wrap_with_limit(safe, config.MAX_ROWS)
    conn = connect_read_only()
    try:
        cols, rows = _fetch(conn, wrapped)
    finally:
        conn.close()
    truncated = len(rows) > config.MAX_ROWS
    rows = rows[: config.MAX_ROWS]
    return {"columns": cols, "rows": [list(r) for r in rows], "row_count": len(rows), "truncated": truncated}


def run_internal_query(sql: str, params=None) -> dict:
    """For trusted, hard-coded SQL inside this package (never model-written)."""
    conn = connect_read_only()
    try:
        cols, rows = _fetch(conn, sql, params)
    finally:
        conn.close()
    return {"columns": cols, "rows": [list(r) for r in rows]}
