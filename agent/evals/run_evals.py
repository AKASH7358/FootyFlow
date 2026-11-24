"""Score the agent on agent/evals/cases.json against the live stack.

    python -m agent.evals.run_evals            # run everything
    python -m agent.evals.run_evals --id count_matches --id refuse_delete

Checks are behavioural (which tools ran, whether writes/triggers were blocked, whether refusals happened)
and data-grounded (numbers in the answer must match ground-truth SQL that the harness runs itself).
The approver always declines, so no eval can trigger the real pipeline.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

from .. import config

HERE = Path(__file__).parent
NUM = re.compile(r"-?\d+(?:\.\d+)?")


def _numbers(text: str) -> list[float]:
    return [float(x) for x in NUM.findall(text.replace(",", ""))]


def _scalars(rows: list) -> list[float]:
    out = []
    for row in rows:
        for v in row:
            try:
                out.append(float(v))
            except (TypeError, ValueError):
                pass
    return out


def evaluate(case: dict, result, truth_rows=None, state_before=None, state_after=None) -> list[str]:
    """Return a list of failure reasons (empty list = pass)."""
    fails: list[str] = []
    called = [c["name"] for c in result.tool_calls]
    succeeded = [c["name"] for c in result.tool_calls if c["ok"]]
    answer = (result.answer or "").lower()

    if result.status != "ok":
        fails.append(f"agent status={result.status}")
    any_of = case.get("tools_used_any")
    if any_of and not set(any_of) & set(called):
        fails.append(f"expected one of {any_of} to be called, got {called}")
    for t in case.get("tools_succeeded", []):
        if t not in succeeded:
            fails.append(f"{t} was required to succeed but did not (called: {called})")
    for t in case.get("tools_not_succeeded", []):
        if t in succeeded:
            fails.append(f"{t} succeeded but must not")
    if "max_tool_calls" in case and len(called) > case["max_tool_calls"]:
        fails.append(f"{len(called)} tool calls > max {case['max_tool_calls']}")
    for s in case.get("answer_contains_all", []):
        if s.lower() not in answer:
            fails.append(f"answer missing '{s}'")
    options = case.get("answer_contains_any")
    if options and not any(o.lower() in answer for o in options):
        fails.append(f"answer contains none of {options}")
    if case.get("state_unchanged") and state_before != state_after:
        fails.append(f"database state changed: {state_before} -> {state_after}")
    if truth_rows is not None:
        found = _numbers(result.answer or "")
        for expected in _scalars(truth_rows):
            tol = 0 if float(expected).is_integer() else 0.06
            if not any(abs(f - expected) <= tol for f in found):
                fails.append(f"answer does not contain ground-truth value {expected}")
    return fails


def _snapshot():
    from .. import db
    res = db.run_readonly_query(
        "SELECT (SELECT COUNT(*) FROM analytics.mart_results), (SELECT COUNT(*) FROM public.raw_matches)")
    return res["rows"]


def main(argv=None) -> int:
    from ..cli import _load_env
    _load_env()
    import importlib
    importlib.reload(config)
    from .. import db
    from ..agent import OpsAgent

    parser = argparse.ArgumentParser()
    parser.add_argument("--id", action="append", help="run only these case ids")
    args = parser.parse_args(argv)

    cases = json.loads((HERE / "cases.json").read_text())
    if args.id:
        cases = [c for c in cases if c["id"] in args.id]

    agent = OpsAgent(approver=lambda _prompt: False)
    rows, total_cost = [], 0.0
    for case in cases:
        truth = db.run_readonly_query(case["ground_truth_sql"])["rows"] if case.get("ground_truth_sql") else None
        before = _snapshot() if case.get("state_unchanged") else None
        result = agent.run(case["question"], kind="eval")
        after = _snapshot() if case.get("state_unchanged") else None
        fails = evaluate(case, result, truth, before, after)
        total_cost += result.cost_usd
        rows.append({"id": case["id"], "passed": not fails, "failures": fails, "tools": [c["name"] for c in result.tool_calls],
                     "latency_ms": result.latency_ms, "cost_usd": result.cost_usd, "answer": result.answer})
        print(f"{'PASS' if not fails else 'FAIL'}  {case['id']:<28} {result.latency_ms:>6} ms  ${result.cost_usd:.4f}  tools={rows[-1]['tools']}")
        for f in fails:
            print(f"        - {f}")

    passed = sum(r["passed"] for r in rows)
    print(f"\n{passed}/{len(rows)} passed | total est. cost ${total_cost:.4f}")
    out_dir = HERE / "results"
    out_dir.mkdir(exist_ok=True)
    out = out_dir / f"eval_{time.strftime('%Y%m%d_%H%M%S')}.json"
    out.write_text(json.dumps(rows, indent=2, default=str))
    print(f"details: {out.relative_to(config.REPO_ROOT)}")
    return 0 if passed == len(rows) else 1


if __name__ == "__main__":
    sys.exit(main())
