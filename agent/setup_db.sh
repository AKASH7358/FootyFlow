#!/usr/bin/env bash
# One-time (idempotent) setup of the agent's Postgres role and logging tables.
# Usage: AGENT_DB_PASSWORD=choose-a-password ./agent/setup_db.sh   (or put it in .env)
set -euo pipefail
cd "$(dirname "$0")/.."

# Read a KEY from .env without sourcing it (avoids shell-parsing surprises).
env_get() {
  [ -f .env ] || return 0
  # Same rules as python-dotenv: last occurrence wins; strip CR, surrounding whitespace and one pair of quotes.
  { grep -E "^$1=" .env || true; } | tail -1 | cut -d= -f2- | tr -d '\r' \
    | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e 's/^"\(.*\)"$/\1/' -e "s/^'\\(.*\\)'\$/\\1/"
}

PGUSER="${POSTGRES_USER:-$(env_get POSTGRES_USER)}"
PGDB="${POSTGRES_DB:-$(env_get POSTGRES_DB)}"
AGENT_DB_PASSWORD="${AGENT_DB_PASSWORD:-$(env_get AGENT_DB_PASSWORD)}"

[ -n "$PGUSER" ] && [ -n "$PGDB" ] || { echo "POSTGRES_USER / POSTGRES_DB not found in env or .env"; exit 1; }
[ -n "$AGENT_DB_PASSWORD" ] || { echo "Set AGENT_DB_PASSWORD (env var or .env) first"; exit 1; }

docker exec -i postgres psql -v ON_ERROR_STOP=1 -U "$PGUSER" -d "$PGDB" -v agent_pw="$AGENT_DB_PASSWORD" -f - < agent/sql/setup.sql
echo "Agent role and agent_ops tables are ready."
