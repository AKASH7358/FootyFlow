"""Safety layer between the LLM and the database / logs.

Defence in depth for SQL:
  1. this validator (single read-only SELECT, allow-listed relations)
  2. a Postgres role that only has SELECT on three relations (agent/sql/setup.sql)
  3. a read-only transaction with a statement timeout and a row cap (agent/db.py)
"""
from __future__ import annotations

import re

MAX_SQL_CHARS = 4000

ALLOWED_RELATIONS = {
    "analytics.mart_results", "analytics.stg_matches", "public.raw_matches",
    "mart_results", "stg_matches", "raw_matches",
}

_FORBIDDEN_KEYWORDS = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|copy|call|do|execute|"
    r"prepare|deallocate|vacuum|analyze|refresh|reindex|cluster|comment|lock|listen|notify|"
    r"set|reset|show|begin|commit|rollback|savepoint|merge|into)\b"
)
_FORBIDDEN_NAMES = re.compile(
    r"\b(pg_\w*|lo_\w+|dblink\w*|set_config|current_setting|information_schema|"
    r"query_to_xml\w*|xpath\w*|txid_\w+)\b"
)


class GuardrailError(ValueError):
    """A request broke a safety rule. The message is safe to show to the model."""


def _scan(sql: str) -> str:
    """Replace string literals with '' and reject comments / escape-string tricks."""
    out: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if c == "'":
            prev = sql[i - 1] if i >= 1 else ""
            prev2 = sql[i - 2] if i >= 2 else ""
            if prev in "eE" and prev and not (prev2.isalnum() or prev2 == "_"):
                raise GuardrailError("escape strings (E'...') are not allowed")
            if prev == "&" and prev2 in "uU" and prev2:
                raise GuardrailError("unicode escape strings (U&'...') are not allowed")
            j = i + 1
            while True:
                if j >= n:
                    raise GuardrailError("unterminated string literal")
                if sql[j] == "'":
                    if j + 1 < n and sql[j + 1] == "'":
                        j += 2
                        continue
                    break
                j += 1
            out.append("''")
            i = j + 1
            continue
        if sql.startswith("--", i) or sql.startswith("/*", i):
            raise GuardrailError("SQL comments are not allowed")
        out.append(c)
        i += 1
    return "".join(out)


def validate_select(sql: str) -> str:
    """Return a cleaned single SELECT statement or raise GuardrailError."""
    if not isinstance(sql, str) or not sql.strip():
        raise GuardrailError("sql must be a non-empty string")
    if len(sql) > MAX_SQL_CHARS:
        raise GuardrailError(f"query too long (max {MAX_SQL_CHARS} characters)")
    if '"' in sql or "$" in sql or "\x00" in sql:
        raise GuardrailError("double quotes, '$' and null bytes are not allowed; use plain lowercase identifiers")

    scrubbed = _scan(sql).strip()
    while scrubbed.endswith(";"):
        scrubbed = scrubbed[:-1].rstrip()
    if ";" in scrubbed:
        raise GuardrailError("only a single statement is allowed")

    lowered = scrubbed.lower()
    if not re.match(r"(select|with)\b", lowered):
        raise GuardrailError("only SELECT queries are allowed")
    m = _FORBIDDEN_KEYWORDS.search(lowered)
    if m:
        raise GuardrailError(f"keyword not allowed: {m.group(1).upper()} (this tool is read-only)")
    m = _FORBIDDEN_NAMES.search(lowered)
    if m:
        raise GuardrailError(f"name not allowed: {m.group(1)}")

    ctes = set(re.findall(r"(?:\bwith\s+(?:recursive\s+)?|,\s*)([a-z_][a-z0-9_]*)\s+as\s*\(", lowered))
    for_scan = re.sub(r"\bextract\s*\(\s*\w+\s+from\b", "extract(x ", lowered)
    for rel in re.findall(r"\b(?:from|join)\s+([a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)?)", for_scan):
        if rel in ALLOWED_RELATIONS or rel in ctes:
            continue
        raise GuardrailError(
            f"relation not allowed: {rel}. Allowed: analytics.mart_results, analytics.stg_matches, public.raw_matches"
        )
    return sql.strip().rstrip(";").strip()


def wrap_with_limit(sql: str, max_rows: int) -> str:
    """Wrap a validated query so we can fetch max_rows+1 and detect truncation."""
    return f"SELECT * FROM ({sql}) AS _q LIMIT {int(max_rows) + 1}"


_REDACTIONS = [
    (re.compile(r"sk-ant-[A-Za-z0-9_\-]+"), "[REDACTED]"),
    (re.compile(r"AKIA[0-9A-Z]{16}"), "[REDACTED]"),
    (re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._\-]+"), "Bearer [REDACTED]"),
    (re.compile(r"(?i)(postgres(?:ql)?://[^:\s/]+:)[^@\s]+@"), r"\1[REDACTED]@"),
    (
        re.compile(r"(?i)\b(x-auth-token|authorization|api[_-]?key|password|passwd|secret|token)\b(\s*[:=]\s*)([\"']?)([^\s\"',;]+)"),
        r"\1\2\3[REDACTED]",
    ),
]


def redact_secrets(text: str) -> str:
    """Strip credentials before text is sent to an LLM, logged or posted to Slack."""
    for pattern, repl in _REDACTIONS:
        text = pattern.sub(repl, text)
    return text
