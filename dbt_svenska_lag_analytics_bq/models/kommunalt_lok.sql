-- https://www.taby.se/fritid-och-kultur/foreningsliv/bidrag-stod-och-foreningsjubileer/

WITH activities AS (
    SELECT
        CAST(START_AT AS DATE) AS DATE_DAY,
        ACTIVITY_ID,
        MEMBER_ID,
        DATE_DIFF(CAST(START_AT AS DATE), DATE_OF_BIRTH, YEAR) AS AGE_AT_ACTIVITY_START,
        PLAYERS_ATTENDED,
        DURATION_MINUTES,
    FROM {{ ref('attended_activity') }}
    LEFT JOIN {{ ref('activity') }} USING(ACTIVITY_ID)
    LEFT JOIN {{ ref('member') }} USING(MEMBER_ID)
), contribution_per_member_and_day AS (
    SELECT
        DATE_DAY,
        MEMBER_ID,
        MAX(
            CASE
                -- Activity has to be at least 60 min
                WHEN DURATION_MINUTES < 60 THEN 0
                -- Activity has to have at least 5 players attending
                WHEN PLAYERS_ATTENDED < 5 THEN 0
                WHEN AGE_AT_ACTIVITY_START > 20 THEN 0
                WHEN AGE_AT_ACTIVITY_START > 12 THEN 11
                WHEN AGE_AT_ACTIVITY_START > 6 THEN 8.5
                ELSE 0
            END
        ) AS CONTRIBUTION_MUNICIPALITY,
        -- Double-up if more than 15
        IF(PLAYERS_ATTENDED > 15, 2, 1) AS MULTIPLIER
    FROM activities
    GROUP BY ALL
)
SELECT
    *
    --DATE_DAY,
    --SUM(CONTRIBUTION_MUNICIPALITY * MULTIPLIER) AS CONTRIBUTION_MUNICIPALITY
FROM contribution_per_member_and_day
GROUP BY ALL
