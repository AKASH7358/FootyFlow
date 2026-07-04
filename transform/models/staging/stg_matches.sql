-- models/staging/stg_matches.sql

-- What this model does:
-- 1. Reads from raw_matches (written by your Python consumer)
-- 2. Splits the "2-1" score string into two separate integers
-- 3. Renames columns to be cleaner
-- 4. Adds a derived column: who won?

WITH source AS (
    -- ref() is how dbt models reference tables or each other
    -- for raw tables that dbt didn't create, we use source() instead
    -- but since raw_matches is in a different schema (public), 
    -- we reference it directly for now
    SELECT * FROM public.raw_matches
),

cleaned AS (
    SELECT offset_id, match_id, home_team, away_team,

    SPLIT_PART(score, '-', 1)::INT AS home_goals,
    SPLIT_PART(score, '-', 2)::INT AS away_goals,

    minute,
    consumed_at

    FROM source
)

SELECT * FROM cleaned