# FootyFlow 

A real-time football data pipeline built with Apache Kafka, Airflow, dbt, PostgreSQL, and Grafana — all running locally via Docker.

## Architecture

```
football-data.org API
        >>
Apache Kafka (message broker)
        >>
Python Consumer
        >>
PostgreSQL (raw_matches)
        >>
dbt (stg_matches → mart_results)
        >>
Apache Airflow (orchestration @hourly)
        >>
Grafana (live dashboard)
```

## Tech Stack

| Layer | Tool |
|---|---|
| Streaming | Apache Kafka + Zookeeper |
| Storage | PostgreSQL 15 |
| Transformation | dbt (data build tool) |
| Orchestration | Apache Airflow 2.9 |
| Visualisation | Grafana |
| Infrastructure | Docker + Docker Compose |
| Language | Python 3.12 |

## Project Structure

```
footyflow/
├── producer/           # Kafka producer — fetches from football API
├── consumer/           # Kafka consumer — writes to PostgreSQL
├── transform/          # dbt project — staging + mart models
│   └── models/
│       ├── staging/    # stg_matches — clean and type-cast raw data
│       └── marts/      # mart_results — winner, goals, result type
├── airflow/
│   └── dags/           # Airflow DAG — orchestrates full pipeline
├── Dockerfile.airflow  # Custom image with all dependencies
└── docker-compose.yml  # Full stack — one command setup
```

## Quick Start

**Prerequisites:** Docker Desktop, Python 3.9+, football-data.org API key (free at football-data.org)

```bash
# Cloning repo
git clone https://github.com/AKASH7358/FootyFlow.git
cd FootyFlow

# Add your API key generated from https://www.football-data.org/client/register 
echo "FOOTBALL_API_KEY=your_key_here" >> .env
echo "POSTGRES_USER=footyflow" >> .env
echo "POSTGRES_PASSWORD=footyflow123" >> .env
echo "POSTGRES_DB=footyflow_db" >> .env

# Start everything
docker compose up -d

# Trigger the pipeline
open http://localhost:8081   # Airflow UI (admin/admin)
open http://localhost:3000   # Grafana dashboard (admin/footyflow123)
open http://localhost:8080   # Kafka UI
```

## Pipeline

The Airflow DAG runs `@hourly` and executes 4 tasks in sequence:

```
run_producer → run_consumer → dbt_run → dbt_test
```

1. **run_producer** — fetches live/recent matches from football-data.org and publishes to Kafka topic `matches`
2. **run_consumer** — reads from Kafka and upserts into `public.raw_matches` in PostgreSQL
3. **dbt_run** — transforms raw data into `analytics.stg_matches` (view) and `analytics.mart_results` (table)
4. **dbt_test** — runs 9 data quality tests (uniqueness, not null, accepted values)

## dbt Models

**stg_matches** — cleans raw data:
- Splits `score` string into `home_goals` and `away_goals` integers
- Standardises column names

**mart_results** — business-ready analytics:
- Adds `winner`, `result_type` (Home Win / Away Win / Draw)
- Calculates `total_goals` and `goal_difference`

## Data Quality

9 automated dbt tests run on every pipeline execution:
- `unique` — no duplicate match IDs
- `not_null` — all key fields populated
- `accepted_values` — result_type is always Home Win, Away Win, or Draw

## Dashboard

Grafana dashboard at `localhost:3000` shows:
- Match results table with goals and winner
- Goals comparison bar chart per match
- Result type breakdown pie chart
## AI Operations Agent

A tool-calling Claude agent that sits on top of the pipeline and does two jobs for whoever operates it:

1. **Answers questions** about the match data and pipeline health in plain English, grounded in live tool results.
2. **Diagnoses failures.** When an Airflow task fails after its retries, a callback sends the redacted log to Claude and writes a structured incident (root cause, evidence, suggested fix, safe-to-retry) to Postgres and, optionally, Slack.

```
operator question ──> Claude ──┬─> describe_schema / query_matches ──> Postgres (read-only role)
                               ├─> get_pipeline_status ─────────────> Airflow REST API
                               ├─> get_dbt_test_results ────────────> transform/target/run_results.json
                               ├─> get_task_log ────────────────────> Airflow task logs (redacted)
                               └─> trigger_pipeline ──[human approves]──> Airflow REST API

Airflow task fails ──> on_failure_callback ──> Claude (forced structured output) ──> agent_ops.incidents (+ Slack)
every run / tool call / token / cost ──> agent_ops.agent_runs, agent_ops.tool_calls
```

### Guardrails

| Risk | Control |
|---|---|
| Model writes or destroys data | Validator allows one `SELECT`/`WITH` on three allow-listed relations; blocks comments, `$`-quoting, escape strings, `pg_*`, `information_schema`. Postgres role `footyflow_agent` has `SELECT` on those relations only. Queries run in a read-only transaction with a 5s timeout and a 50-row cap. |
| Model takes an unsafe action | `trigger_pipeline` needs explicit human approval on every call; with no approver (e.g. inside Airflow) it is refused. |
| Prompt injection via logs or data | Logs and query results are labelled untrusted in the prompt; the model has no write tools to abuse. |
| Secrets sent to an LLM | API keys, bearer tokens, passwords and DB URLs are redacted before any log text leaves the machine. |
| Agent breaks the pipeline | Callback is wrapped so it can never raise; the DAG runs unchanged if the agent or API key is missing. |

### Setup

```bash
cp .env.example .env            # then fill in ANTHROPIC_API_KEY and AGENT_DB_PASSWORD (keep .env out of git)
pip install -r agent/requirements.txt
docker compose up -d --build    # rebuild so the Airflow image includes the anthropic SDK
./agent/setup_db.sh             # creates the least-privilege role and agent_ops tables (idempotent)
```

```bash
python -m agent.cli ask "Which match had the most goals?"
python -m agent.cli chat
python -m agent.cli diagnose --task dbt_run     # diagnose the latest log of a task
python -m agent.evals.run_evals                 # 14 eval cases against the live stack
python -m unittest discover -s agent/tests -t . # unit tests, no network or DB needed
```

### Evals

`agent/evals/cases.json` holds 14 cases: data questions whose numbers are checked against ground-truth SQL the harness runs itself, pipeline-status and dbt questions, and safety cases (delete request, DROP-plus-trigger injection, unsafe SQL, out-of-scope question, a question with no answer in the data). Every run reports pass/fail, latency and estimated cost.

### Runbook and known limitations

- `dbt` recreates tables on each run, which wipes grants; `setup.sql` uses default privileges so the agent keeps access.
- `run_results.json` reflects only the latest dbt command (`run` or `test`), so the agent reports which one it saw.
- Airflow credentials default to the local `admin/admin` from `docker-compose.yml`. Change them and set `AIRFLOW_USER` / `AIRFLOW_PASSWORD` before exposing anything beyond localhost.
- Data is a small slice from the football-data.org free tier. This demonstrates the engineering, not predictive accuracy.
- Cost figures are estimates from token counts and configurable prices (`AGENT_PRICE_*`).
