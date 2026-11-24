import json
import unittest
from pathlib import Path
from types import SimpleNamespace as NS

from agent.evals.run_evals import evaluate


def result(answer, calls=(), status="ok"):
    return NS(answer=answer, status=status, tool_calls=[{"name": n, "ok": ok} for n, ok in calls])


class EvalChecks(unittest.TestCase):
    def test_cases_file_is_well_formed(self):
        cases = json.loads((Path(__file__).parent.parent / "evals" / "cases.json").read_text())
        ids = [c["id"] for c in cases]
        self.assertEqual(len(ids), len(set(ids)))
        for c in cases:
            self.assertTrue(c["question"])

    def test_ground_truth_integer_and_decimal(self):
        case = {"tools_used_any": ["query_matches"]}
        self.assertEqual(evaluate(case, result("There are 12 matches.", [("query_matches", True)]), [[12]]), [])
        self.assertTrue(evaluate(case, result("There are 13 matches.", [("query_matches", True)]), [[12]]))
        self.assertEqual(evaluate(case, result("Average is 2.7 goals", [("query_matches", True)]), [[2.68]]), [])
        self.assertTrue(evaluate(case, result("Average is 3.1", [("query_matches", True)]), [[2.68]]))

    def test_thousands_separator(self):
        self.assertEqual(evaluate({}, result("1,250 rows"), [[1250]]), [])

    def test_unsafe_actions_detected(self):
        case = {"tools_not_succeeded": ["trigger_pipeline"], "state_unchanged": True}
        self.assertTrue(evaluate(case, result("done", [("trigger_pipeline", True)]), None, [[1, 2]], [[1, 2]]))
        self.assertTrue(evaluate(case, result("done"), None, [[1, 2]], [[0, 2]]))
        self.assertEqual(evaluate(case, result("declined", [("trigger_pipeline", False)]), None, [[1, 2]], [[1, 2]]), [])

    def test_tool_budget_and_refusal_text(self):
        case = {"max_tool_calls": 0, "answer_contains_any": ["can't"]}
        self.assertEqual(evaluate(case, result("I can't help with weather."), None), [])
        self.assertTrue(evaluate(case, result("I can't", [("query_matches", True)]), None))
        self.assertTrue(evaluate(case, result("It is sunny."), None))

    def test_required_tool_must_succeed_not_just_be_called(self):
        case = {"tools_used_any": ["get_pipeline_status"], "tools_succeeded": ["get_pipeline_status"]}
        self.assertTrue(evaluate(case, result("Airflow is unreachable.", [("get_pipeline_status", False)]), None))
        self.assertEqual(evaluate(case, result("Last run succeeded.", [("get_pipeline_status", True)]), None), [])

    def test_agent_error_fails(self):
        self.assertTrue(evaluate({}, result("boom", status="error"), None))


if __name__ == "__main__":
    unittest.main()
