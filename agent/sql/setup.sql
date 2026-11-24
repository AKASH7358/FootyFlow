-- Least-privilege setup for the ops agent. Run via agent/setup_db.sh as the Postgres admin user.
-- psql variable expected: agent_pw

SELECT format('CREATE ROLE footyflow_agent LOGIN PASSWORD %L', :'agent_pw')
 WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'footyflow_agent') \gexec
SELECT format('ALTER ROLE footyflow_agent PASSWORD %L', :'agent_pw') \gexec
ALTER ROLE footyflow_agent CONNECTION LIMIT 10;
ALTER ROLE footyflow_agent SET statement_timeout = '15s';

SELECT format('GRANT CONNECT ON DATABASE %I TO footyflow_agent', current_database()) \gexec

-- Read-only access to exactly the relations the query tool allow-lists.
CREATE SCHEMA IF NOT EXISTS analytics;
GRANT USAGE ON SCHEMA analytics TO footyflow_agent;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO footyflow_agent;
-- dbt drops and recreates tables on every run, which wipes grants. Default privileges re-apply
-- SELECT to anything this admin role creates in analytics later (dbt connects as the same role).
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO footyflow_agent;

GRANT USAGE ON SCHEMA public TO footyflow_agent;
REVOKE CREATE ON SCHEMA public FROM footyflow_agent;
SELECT 'GRANT SELECT ON public.raw_matches TO footyflow_agent' WHERE to_regclass('public.raw_matches') IS NOT NULL \gexec

-- Operational log: the only place the agent can write.
CREATE SCHEMA IF NOT EXISTS agent_ops;

CREATE TABLE IF NOT EXISTS agent_ops.agent_runs (
    run_id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at      timestamptz NOT NULL DEFAULT now(),
    kind            text,          -- ask | chat | diagnose | eval
    input           text,
    answer          text,
    model           text,
    status          text,          -- running | ok | error | max_turns
    turns           int,
    tool_call_count int,
    input_tokens    int,
    output_tokens   int,
    cost_usd        numeric(10,6),
    latency_ms      int
);

CREATE TABLE IF NOT EXISTS agent_ops.tool_calls (
    id             bigserial PRIMARY KEY,
    run_id         uuid REFERENCES agent_ops.agent_runs (run_id),
    created_at     timestamptz NOT NULL DEFAULT now(),
    tool_name      text,
    tool_input     jsonb,
    ok             boolean,
    output_preview text,
    latency_ms     int
);

CREATE TABLE IF NOT EXISTS agent_ops.incidents (
    id             bigserial PRIMARY KEY,
    created_at     timestamptz NOT NULL DEFAULT now(),
    dag_id         text,
    task_id        text,
    airflow_run_id text,
    try_number     int,
    category       text,
    severity       text,
    summary        text,
    root_cause     text,
    evidence       jsonb,
    suggested_fix  text,
    safe_to_retry  boolean,
    confidence     text,
    model          text,
    agent_run_id   uuid
);

GRANT USAGE ON SCHEMA agent_ops TO footyflow_agent;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA agent_ops TO footyflow_agent;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA agent_ops TO footyflow_agent;
