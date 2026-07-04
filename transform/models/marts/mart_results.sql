-- models/marts/mart_results.sql

-- What this model does:
-- Builds on stg_matches and adds business-level columns:
-- total goals, result (home win / away win / draw), goal difference

WITH matches AS (
    -- ref() is the key dbt concept here
    -- instead of writing the full table name, you reference the model name
    -- dbt figures out the dependency automatically and runs stg_matches first

    SELECT * FROM {{ref('stg_matches')}}
),

results AS (
    SELECT match_id, home_team, away_team, home_goals, away_goals, home_goals + away_goals AS total_goals,
    ABS(home_goals - away_goals) AS goal_difference, minute as final_minute,

    CASE 
        WHEN home_goals > away_goals THEN home_team
        WHEN away_goals > home_goals THEN away_team
        ELSE 'Draw'
    END AS winner,

    CASE
        WHEN home_goals > away_goals THEN 'Home Win'
        WHEN away_goals > home_goals THEN 'Away Win'
        ELSE 'Draw'
    END AS result_type,

    consumed_at

    FROM matches
)

SELECT * FROM results