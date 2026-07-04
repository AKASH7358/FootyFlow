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