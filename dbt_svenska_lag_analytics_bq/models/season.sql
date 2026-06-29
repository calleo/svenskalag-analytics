WITH seeded_seasons AS (
    SELECT
        SEASON_ID,
        SEASON_NAME,
        YEAR,
        DIFFICULTY
    FROM {{ ref('seed_seasons') }}
), missing_seasons AS (
    SELECT DISTINCT
        stg_activity.SEASON_ID,
        CONCAT('Unknown season ', CAST(stg_activity.SEASON_ID AS STRING)) AS SEASON_NAME,
        EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', stg_activity.SCHEDULE_DATE)) AS YEAR,
        'Unknown' AS DIFFICULTY
    FROM {{ ref('stg_activity') }}
    LEFT JOIN seeded_seasons USING (SEASON_ID)
    WHERE seeded_seasons.SEASON_ID IS NULL
      AND stg_activity.SEASON_ID IS NOT NULL
)
SELECT * FROM seeded_seasons
UNION ALL
SELECT * FROM missing_seasons