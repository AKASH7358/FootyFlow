"""Airflow on_failure_callback. Must never raise: a broken agent must not affect the pipeline."""
from __future__ import annotations

import os


def on_failure_diagnose(context) -> None:
    try:
        if not os.getenv("ANTHROPIC_API_KEY"):
            print("[ops-agent] ANTHROPIC_API_KEY not set; skipping diagnosis")
            return
        from . import config
        from .diagnose import diagnose, format_incident
        from .tools import attempt_from_path, find_latest_log, read_log_tail

        ti = context["task_instance"]
        log_text, attempt = "", None
        try:
            path = find_latest_log(ti.task_id, run_id=ti.run_id)
            if path:
                log_text = read_log_tail(path, 150)
                attempt = attempt_from_path(path)
        except Exception as exc:
            print(f"[ops-agent] could not read task log: {exc}")

        incident = diagnose(
            task_id=ti.task_id, log_text=log_text, dag_id=ti.dag_id, airflow_run_id=ti.run_id,
            try_number=attempt or max(int(getattr(ti, "try_number", 1) or 1) - 1, 1), exception_text=str(context.get("exception") or ""),
        )
        if "error" in incident:
            print(f"[ops-agent] diagnosis failed: {incident['error']}")
        else:
            print("[ops-agent] " + format_incident(incident))
    except Exception as exc:  # last line of defence
        print(f"[ops-agent] callback error ignored: {exc}")
