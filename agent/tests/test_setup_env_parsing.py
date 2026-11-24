import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "setup_db.sh"


def parse(env_bytes: bytes, key: str = "AGENT_DB_PASSWORD") -> str:
    """Run the real env_get() from setup_db.sh against a temporary .env."""
    with tempfile.TemporaryDirectory() as tmp:
        (Path(tmp) / ".env").write_bytes(env_bytes)
        out = subprocess.run(
            ["bash", "-c", f'set -euo pipefail; eval "$(sed -n \'/^env_get()/,/^}}/p\' "{SCRIPT}")"; cd "{tmp}"; env_get {key}'],
            capture_output=True, text=True, check=True,
        )
        return out.stdout.rstrip("\n")


class EnvParsing(unittest.TestCase):
    def test_plain(self):
        self.assertEqual(parse(b"AGENT_DB_PASSWORD=abc123\n"), "abc123")

    def test_surrounding_whitespace_is_stripped(self):
        self.assertEqual(parse(b"AGENT_DB_PASSWORD=  abc123  \n"), "abc123")

    def test_crlf(self):
        self.assertEqual(parse(b"A=1\r\nAGENT_DB_PASSWORD=abc123\r\n"), "abc123")

    def test_quotes(self):
        self.assertEqual(parse(b'AGENT_DB_PASSWORD="abc123"\n'), "abc123")
        self.assertEqual(parse(b"AGENT_DB_PASSWORD='abc123'\n"), "abc123")

    def test_last_occurrence_wins(self):
        self.assertEqual(parse(b"AGENT_DB_PASSWORD=old\nAGENT_DB_PASSWORD=new\n"), "new")

    def test_special_characters_survive(self):
        self.assertEqual(parse(b"AGENT_DB_PASSWORD=p@ss.w-rd_9\n"), "p@ss.w-rd_9")

    def test_missing_key_is_empty_not_an_error(self):
        self.assertEqual(parse(b"OTHER=1\n"), "")

    def test_does_not_match_key_prefix(self):
        self.assertEqual(parse(b"XAGENT_DB_PASSWORD=nope\n"), "")


if __name__ == "__main__":
    unittest.main()
