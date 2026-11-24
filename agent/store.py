"""Run / tool-call / incident logging. Logging must never break the agent, so every
Postgres write is best-effort. MemoryStore is used in tests and when the DB is unavailable."""
from __future__ import annotations

import sys
import uuid

from . import config


def _warn(msg: str) -> None:
    print(f"[ops-agent] {msg}", file=sys.stderr)


class MemoryStore:
    def __init__(self):
        self.runs: dict[str, dict] = {}
        self.tool_calls: list[dict] = []
        self.incidents: list[dict] = []

    def start_run(self, kind, input_text, model) -> str:
        run_id = str(uuid.uuid4())
        self.runs[run_id] = {"kind": kind, "input": input_text, "model": model, "status": "running"}
        return run_id

    def log_tool_call(self, run_id, name, tool_input, ok, output_preview, latency_ms) -> None:
        self.tool_calls.append({"run_id": run_id, "name": name, "input": tool_input, "ok": ok,
                                "output_preview": output_preview, "latency_ms": latency_ms})

    def finish_run(self, run_id, **fields) -> None:
        self.runs.setdefault(run_id, {}).update(fields)

    def save_incident(self, **fields) -> None:
        self.incidents.append(fields)


class PostgresStore:
    def start_run(self, kind, input_text, model) -> str:
        try:
            from . import db
            conn = db.connect_ops()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO agent_ops.agent_runs (kind, input, model, status) "
                        "VALUES (%s, %s, %s, 'running') RETURNING run_id",
                        (kind, input_text, model),
                    )
                    return str(cur.fetchone()[0])
            finally:
                conn.close()
        except Exception as exc:
            _warn(f"could not log run start: {exc}")
            return str(uuid.uuid4())

    def log_tool_call(self, run_id, name, tool_input, ok, output_preview, latency_ms) -> None:
        try:
            from psycopg2.extras import Json
            from . import db
            conn = db.connect_ops()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO agent_ops.tool_calls (run_id, tool_name, tool_input, ok, output_preview, latency_ms) "
                        "VALUES (%s, %s, %s, %s, %s, %s)",
                        (run_id, name, Json(tool_input), ok, output_preview, latency_ms),
                    )
            finally:
                conn.close()
        except Exception as exc:
            _warn(f"could not log tool call: {exc}")

    def finish_run(self, run_id, **f) -> None:
        try:
            from . import db
            conn = db.connect_ops()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE agent_ops.agent_runs SET status=%s, answer=%s, turns=%s, tool_call_count=%s, "
                        "input_tokens=%s, output_tokens=%s, cost_usd=%s, latency_ms=%s WHERE run_id=%s",
                        (f.get("status"), f.get("answer"), f.get("turns"), f.get("tool_call_count"),
                         f.get("input_tokens"), f.get("output_tokens"), f.get("cost_usd"),
                         f.get("latency_ms"), run_id),
                    )
            finally:
                conn.close()
        except Exception as exc:
            _warn(f"could not log run result: {exc}")

    def save_incident(self, **f) -> None:
        try:
            from psycopg2.extras import Json
            from . import db
            conn = db.connect_ops()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO agent_ops.incidents (dag_id, task_id, airflow_run_id, try_number, category, "
                        "severity, summary, root_cause, evidence, suggested_fix, safe_to_retry, confidence, model, agent_run_id) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (f.get("dag_id"), f.get("task_id"), f.get("airflow_run_id"), f.get("try_number"),
                         f.get("category"), f.get("severity"), f.get("summary"), f.get("root_cause"),
                         Json(f.get("evidence") or []), f.get("suggested_fix"), f.get("safe_to_retry"),
                         f.get("confidence"), f.get("model"), f.get("agent_run_id")),
                    )
            finally:
                conn.close()
        except Exception as exc:
            _warn(f"could not save incident: {exc}")


def get_store():
    """PostgresStore when the agent role is configured and reachable, else MemoryStore."""
    if config.DB_PASSWORD:
        try:
            from . import db
            db.connect_ops().close()
            return PostgresStore()
        except Exception as exc:
            _warn(f"Postgres logging unavailable ({exc}); using in-memory store")
    else:
        _warn("AGENT_DB_PASSWORD not set; runs will not be persisted")
    return MemoryStore()
