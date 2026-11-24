"""Command line entry point.  Run from the repo root:

    python -m agent.cli ask "Which match had the most goals?"
    python -m agent.cli chat
    python -m agent.cli diagnose --task dbt_run
"""
from __future__ import annotations

import argparse
import sys

from . import config


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv(config.REPO_ROOT / ".env")
    except ImportError:
        pass


def cli_approver(prompt: str) -> bool:
    answer = input(f"\n  APPROVAL NEEDED: {prompt}\n  Approve? [y/N] ").strip().lower()
    return answer in ("y", "yes")


def _footer(r) -> str:
    tools = ", ".join(f"{c['name']}{'' if c['ok'] else ' (failed)'}" for c in r.tool_calls) or "none"
    return (f"\n  tools: {tools}\n  turns: {r.turns} | tokens: {r.input_tokens} in / {r.output_tokens} out | "
            f"est. cost: ${r.cost_usd:.4f} | {r.latency_ms} ms | run: {r.run_id}")


def main(argv=None) -> int:
    _load_env()
    # Imported after .env is loaded so config picks up the variables.
    import importlib
    importlib.reload(config)
    from .agent import OpsAgent

    parser = argparse.ArgumentParser(prog="agent")
    sub = parser.add_subparsers(dest="cmd", required=True)
    ask = sub.add_parser("ask", help="ask one question")
    ask.add_argument("question")
    sub.add_parser("chat", help="interactive session")
    dx = sub.add_parser("diagnose", help="diagnose the latest log of a task")
    dx.add_argument("--task", required=True, choices=config.ALLOWED_TASKS)
    args = parser.parse_args(argv)

    if args.cmd == "diagnose":
        from .diagnose import diagnose, format_incident
        from .tools import attempt_from_path, find_latest_log, read_log_tail
        path = find_latest_log(args.task)
        if not path:
            print(f"No log found for {args.task} under {config.log_dir()}")
            return 1
        incident = diagnose(task_id=args.task, log_text=read_log_tail(path, 150),
                            try_number=attempt_from_path(path) or 0, notify=False)
        print(format_incident(incident) if "error" not in incident else f"Diagnosis failed: {incident['error']}")
        return 0

    agent = OpsAgent(approver=cli_approver)
    if args.cmd == "ask":
        r = agent.run(args.question)
        print(r.answer + _footer(r))
        return 0 if r.status == "ok" else 1

    print("FootyFlow ops agent. Type 'exit' to quit.")
    history: list = []
    while True:
        try:
            q = input("\nyou> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if q.lower() in ("exit", "quit"):
            return 0
        if not q:
            continue
        r = agent.run(q, history=history, kind="chat")
        history = r.messages[-20:]
        print("\n" + r.answer + _footer(r))


if __name__ == "__main__":
    sys.exit(main())
