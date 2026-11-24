import copy
import unittest
from types import SimpleNamespace as NS

from agent.agent import OpsAgent
from agent.diagnose import diagnose
from agent.store import MemoryStore
from agent.tools import ToolExecutor


def text(t):
    return NS(type="text", text=t)


def tool_use(id_, name, **inp):
    return NS(type="tool_use", id=id_, name=name, input=inp)


def resp(blocks, stop="end_turn", i=100, o=20):
    return NS(content=blocks, stop_reason=stop, usage=NS(input_tokens=i, output_tokens=o))


class FakeClient:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []
        self.messages = NS(create=self._create)

    def _create(self, **kwargs):
        self.calls.append(copy.deepcopy(kwargs))  # snapshot: the agent keeps mutating its messages list
        return self._responses.pop(0)


class StubExecutor(ToolExecutor):
    """Replace network/DB access with canned data."""
    def __init__(self, approver=None):
        super().__init__(approver)
        self.airflow_calls = []

    def _query_matches(self, sql):
        return {"columns": ["n"], "rows": [[7]], "row_count": 1, "truncated": False}

    def _airflow(self, method, path, body=None):
        self.airflow_calls.append((method, path))
        return {"dag_run_id": "manual__x", "state": "queued"}


class AgentLoop(unittest.TestCase):
    def test_tool_then_answer_logs_everything(self):
        client = FakeClient([
            resp([tool_use("t1", "query_matches", sql="SELECT COUNT(*) FROM analytics.mart_results")], stop="tool_use"),
            resp([text("There are 7 matches.")]),
        ])
        store = MemoryStore()
        result = OpsAgent(client=client, store=store, executor=StubExecutor()).run("How many matches?")
        self.assertEqual(result.answer, "There are 7 matches.")
        self.assertEqual(result.status, "ok")
        self.assertEqual((result.input_tokens, result.output_tokens), (200, 40))
        self.assertEqual([c["name"] for c in result.tool_calls], ["query_matches"])
        self.assertEqual(len(store.tool_calls), 1)
        self.assertEqual(next(iter(store.runs.values()))["tool_call_count"], 1)
        # tool_result was fed back to the model on the second call
        second = client.calls[1]["messages"][-1]["content"][0]
        self.assertEqual(second["type"], "tool_result")
        self.assertIn("7", second["content"])
        self.assertFalse(second["is_error"])

    def test_guardrail_error_is_returned_to_model_not_raised(self):
        client = FakeClient([
            resp([tool_use("t1", "query_matches", sql="DROP TABLE analytics.mart_results")], stop="tool_use"),
            resp([text("I can't do that; the tool is read-only.")]),
        ])
        ex = ToolExecutor()  # real executor: validation runs before any DB access
        result = OpsAgent(client=client, store=MemoryStore(), executor=ex).run("drop it")
        self.assertFalse(result.tool_calls[0]["ok"])
        self.assertIn("guardrail", result.tool_calls[0]["output_preview"].lower())
        self.assertEqual(client.calls[1]["messages"][-1]["content"][0]["is_error"], True)

    def test_trigger_requires_approval(self):
        for approver, expect_ok, expect_posts in [(None, False, 0), (lambda _: False, False, 0), (lambda _: True, True, 1)]:
            with self.subTest(approver=bool(approver)):
                ex = StubExecutor(approver=approver)
                client = FakeClient([
                    resp([tool_use("t1", "trigger_pipeline", reason="retry")], stop="tool_use"),
                    resp([text("done")]),
                ])
                result = OpsAgent(client=client, store=MemoryStore(), executor=ex).run("rerun the pipeline")
                self.assertEqual(result.tool_calls[0]["ok"], expect_ok)
                self.assertEqual(sum(1 for m, _ in ex.airflow_calls if m == "POST"), expect_posts)

    def test_unknown_tool_and_bad_args(self):
        ex = ToolExecutor()
        ok, out = ex.execute("rm_rf", {})
        self.assertFalse(ok)
        ok, out = ex.execute("get_task_log", {"task_id": "../../etc/passwd"})
        self.assertFalse(ok)
        ok, out = ex.execute("describe_schema", {"surprise": 1})
        self.assertFalse(ok)

    def test_max_turns(self):
        looping = [resp([tool_use(f"t{i}", "describe_schema")], stop="tool_use") for i in range(5)]
        class SchemaStub(StubExecutor):
            def _describe_schema(self):
                return {"tables": {}}
        ex = SchemaStub()
        ex._handlers["describe_schema"] = ex._describe_schema
        result = OpsAgent(client=FakeClient(looping), store=MemoryStore(), executor=ex, max_turns=3).run("loop")
        self.assertEqual(result.status, "max_turns")
        self.assertEqual(result.turns, 3)

    def test_api_error_is_contained(self):
        class Boom:
            messages = NS(create=lambda **_: (_ for _ in ()).throw(RuntimeError("api down sk-ant-secret123")))
        result = OpsAgent(client=Boom(), store=MemoryStore(), executor=StubExecutor()).run("hi")
        self.assertEqual(result.status, "error")
        self.assertNotIn("secret123", result.answer)

    def test_store_failure_does_not_break_agent(self):
        class BrokenStore(MemoryStore):
            def log_tool_call(self, *a, **k):
                pass  # PostgresStore swallows its own errors; a silent store must be fine
        client = FakeClient([resp([text("ok")])])
        self.assertEqual(OpsAgent(client=client, store=BrokenStore(), executor=StubExecutor()).run("hi").answer, "ok")


class Diagnosis(unittest.TestCase):
    def test_forced_tool_output_saved_and_secrets_redacted(self):
        payload = dict(category="api_unreachable", severity="high", summary="Kafka unreachable",
                       root_cause="kafka container not on network", evidence=["NoBrokersAvailable"],
                       suggested_fix="docker compose up -d kafka", safe_to_retry=True, confidence="high")
        client = FakeClient([resp([NS(type="tool_use", id="x", name="report_diagnosis", input=payload)], stop="tool_use")])
        store = MemoryStore()
        log = "X-Auth-Token: supersecretvalue\nkafka.errors.NoBrokersAvailable"
        inc = diagnose("run_producer", log, airflow_run_id="manual__1", try_number=2, client=client, store=store, notify=False)
        self.assertEqual(inc["category"], "api_unreachable")
        self.assertEqual(store.incidents[0]["task_id"], "run_producer")
        sent = client.calls[0]["messages"][0]["content"]
        self.assertNotIn("supersecretvalue", sent)
        self.assertIn("NoBrokersAvailable", sent)
        self.assertEqual(client.calls[0]["tool_choice"], {"type": "tool", "name": "report_diagnosis"})

    def test_model_text_is_normalised_and_prompt_names_real_services(self):
        from agent.diagnose import SYSTEM
        self.assertIn("zookeeper, kafka, kafka-ui, postgres, airflow, grafana", SYSTEM)
        self.assertIn("COMPILED SQL", SYSTEM)  # regression: LINE 27 was reported as a source-file line
        self.assertIn("code_error", SYSTEM)
        payload = dict(category="api_unreachable", severity="high", summary="**Kafka** down",
                       root_cause="stopped", evidence=["**NoBrokersAvailable**"],
                       suggested_fix="1. **Check** status\\n2. Start it", safe_to_retry=True, confidence="high")
        client = FakeClient([resp([NS(type="tool_use", id="x", name="report_diagnosis", input=payload)], stop="tool_use")])
        inc = diagnose("run_producer", "log", client=client, store=MemoryStore(), notify=False)
        self.assertEqual(inc["suggested_fix"], "1. Check status\n2. Start it")
        self.assertEqual(inc["summary"], "Kafka down")
        self.assertEqual(inc["evidence"], ["NoBrokersAvailable"])

    def test_diagnosis_failure_returns_error_dict(self):
        class Boom:
            messages = NS(create=lambda **_: (_ for _ in ()).throw(RuntimeError("down")))
        inc = diagnose("dbt_run", "log", client=Boom(), store=MemoryStore(), notify=False)
        self.assertIn("error", inc)


if __name__ == "__main__":
    unittest.main()
