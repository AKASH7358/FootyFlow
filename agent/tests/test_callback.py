import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace as NS
from unittest import mock

from agent import airflow_callback, tools

RUN_ID = "manual__2026-07-02T01:51:44.036156+00:00"  # real run_id format from this pipeline's logs


class CallbackAndLogs(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        d = Path(self.tmp.name) / "dag_id=footyflow_pipeline" / f"run_id={RUN_ID}" / "task_id=run_producer"
        d.mkdir(parents=True)
        (d / "attempt=1.log").write_text("first attempt\n")
        (d / "attempt=2.log").write_text("line1\nkafka.errors.NoBrokersAvailable\n")
        os.utime(d / "attempt=1.log", (1, 1))
        self.env = mock.patch.dict(os.environ, {"AIRFLOW_LOG_DIR": self.tmp.name, "ANTHROPIC_API_KEY": "test"})
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_finds_newest_attempt_for_run_with_colons_and_plus(self):
        path = tools.find_latest_log("run_producer", run_id=RUN_ID)
        self.assertTrue(path.endswith("attempt=2.log"))
        self.assertIn("NoBrokersAvailable", tools.read_log_tail(path, 10))

    def test_attempt_from_path(self):
        self.assertEqual(tools.attempt_from_path("/x/task_id=a/attempt=2.log"), 2)
        self.assertIsNone(tools.attempt_from_path("/x/other.log"))

    def test_unknown_task_rejected(self):
        with self.assertRaises(tools.ToolError):
            tools.find_latest_log("../../etc")

    def test_callback_passes_log_to_diagnosis(self):
        ti = NS(task_id="run_producer", dag_id="footyflow_pipeline", run_id=RUN_ID, try_number=3)  # Airflow 2.x reports +1 in callbacks
        with mock.patch("agent.diagnose.diagnose", return_value={"error": "stub"}) as dx:
            airflow_callback.on_failure_diagnose({"task_instance": ti, "exception": RuntimeError("boom")})
        kwargs = dx.call_args.kwargs
        self.assertEqual(kwargs["task_id"], "run_producer")
        self.assertEqual(kwargs["try_number"], 2)  # from attempt=2.log, not ti.try_number
        self.assertIn("NoBrokersAvailable", kwargs["log_text"])
        self.assertIn("boom", kwargs["exception_text"])

    def test_callback_never_raises(self):
        airflow_callback.on_failure_diagnose({})  # malformed context
        with mock.patch.dict(os.environ, {"ANTHROPIC_API_KEY": ""}):
            airflow_callback.on_failure_diagnose({"task_instance": NS()})


if __name__ == "__main__":
    unittest.main()
