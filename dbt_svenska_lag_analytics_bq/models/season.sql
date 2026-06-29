SELECT
    SEASON_ID,
    SEASON_NAME,
    YEAR,
    DIFFICULTY
FROM {{ ref('seed_seasons') }}