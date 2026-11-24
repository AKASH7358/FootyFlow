import unittest

from agent.guardrails import GuardrailError, redact_secrets, validate_select, wrap_with_limit


class ValidSql(unittest.TestCase):
    def test_accepts_read_only_queries(self):
        good = [
            "SELECT COUNT(*) FROM analytics.mart_results",
            "select home_team, away_team from analytics.mart_results where result_type = 'Draw' order by total_goals desc limit 5;",
            "WITH t AS (SELECT * FROM analytics.mart_results) SELECT winner, COUNT(*) FROM t GROUP BY winner",
            "SELECT EXTRACT(year FROM consumed_at) FROM public.raw_matches",
            "SELECT * FROM analytics.mart_results WHERE winner = 'Delete United'",
            "SELECT * FROM analytics.mart_results m JOIN analytics.stg_matches s ON m.match_id = s.match_id",
            "SELECT * FROM analytics.mart_results WHERE winner = 'It''s a draw'",
            "SELECT * FROM analytics.mart_results WHERE winner = 'e'",
        ]
        for sql in good:
            with self.subTest(sql=sql):
                self.assertTrue(validate_select(sql))

    def test_strips_trailing_semicolon(self):
        self.assertEqual(validate_select("SELECT 1 FROM analytics.mart_results;"), "SELECT 1 FROM analytics.mart_results")


class InvalidSql(unittest.TestCase):
    def test_rejects_unsafe_queries(self):
        bad = {
            "delete": "DELETE FROM public.raw_matches",
            "drop": "DROP TABLE analytics.mart_results",
            "update": "UPDATE public.raw_matches SET score = '9-9'",
            "insert": "INSERT INTO public.raw_matches (match_id) VALUES (1)",
            "stacked": "SELECT 1 FROM analytics.mart_results; DROP TABLE analytics.mart_results",
            "line comment": "SELECT * FROM analytics.mart_results -- sneaky",
            "block comment": "SELECT /* x */ * FROM analytics.mart_results",
            "pg_sleep": "SELECT pg_sleep(30)",
            "catalog": "SELECT * FROM pg_shadow",
            "info schema": "SELECT * FROM information_schema.tables",
            "select into": "SELECT * INTO newtable FROM analytics.mart_results",
            "other table": "SELECT * FROM users",
            "other schema": "SELECT * FROM agent_ops.agent_runs",
            "e-string": "SELECT E'\\x41' FROM analytics.mart_results",
            "dollar quote": "SELECT $$x$$ FROM analytics.mart_results",
            "double quote": 'SELECT "pg_sleep"(5)',
            "explain": "EXPLAIN ANALYZE SELECT 1",
            "set": "SET ROLE postgres",
            "copy": "COPY analytics.mart_results TO '/tmp/x'",
            "empty": "   ",
            "unterminated": "SELECT * FROM analytics.mart_results WHERE winner = 'abc",
            "too long": "SELECT " + "1," * 3000 + "1 FROM analytics.mart_results",
        }
        for name, sql in bad.items():
            with self.subTest(name=name):
                with self.assertRaises(GuardrailError):
                    validate_select(sql)


class Helpers(unittest.TestCase):
    def test_wrap_with_limit(self):
        self.assertEqual(wrap_with_limit("SELECT 1", 50), "SELECT * FROM (SELECT 1) AS _q LIMIT 51")

    def test_redaction(self):
        text = (
            "X-Auth-Token: abc123secret\npassword=hunter2 key " + "sk-ant-" + "api03-XYZ_123 "
            + "AKIA" + "ABCDEFGHIJKLMNOP postgresql://user:pw123@host/db Bearer eyJ.abc.def"
        )
        out = redact_secrets(text)
        for leaked in ("abc123secret", "hunter2", "api03-XYZ", "ABCDEFGHIJKLMNOP", "pw123", "eyJ.abc.def"):
            self.assertNotIn(leaked, out)

    def test_redaction_keeps_diagnostic_text(self):
        msg = 'FATAL: password authentication failed for user "footyflow"'
        self.assertEqual(redact_secrets(msg), msg)


if __name__ == "__main__":
    unittest.main()
