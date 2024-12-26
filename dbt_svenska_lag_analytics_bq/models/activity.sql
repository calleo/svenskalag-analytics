WITH activities AS (
    SELECT
        SCHEDULE_ID AS ACTIVITY_ID,
        ACTIVITY_TYPE_ID,
        activity_type.NAME AS ACTIVITY,
        TIMESTAMP(CONCAT(SCHEDULE_DATE, ' ', START_TIME, ':00'), 'Europe/Stockholm') AS START_AT,
        TIMESTAMP(CONCAT(SCHEDULE_DATE, ' ', END_TIME, ':00'), 'Europe/Stockholm') AS END_AT,
        PLACE,
        IS_HOME_FACILITY,
        OPPONENT_CLUB_ID,
        OPPONENT,
        RESULT_TYPE_ID,
        result_type.NAME AS RESULT,
        GOALS_FOR,
        GOALS_AGAINST
    FROM {{ ref('stg_activity') }}
    LEFT JOIN {{ ref('activity_type') }} USING(ACTIVITY_TYPE_ID)
    LEFT JOIN {{ ref('result_type') }} USING(RESULT_TYPE_ID)
), corrected_end_at AS (
    SELECT
        * EXCEPT(END_AT),
        CASE
            WHEN EXTRACT(HOUR FROM END_AT) = 0 AND EXTRACT(MINUTE FROM END_AT) = 0 THEN TIMESTAMP_ADD(END_AT, INTERVAL 1 DAY)
            ELSE END_AT
        END AS END_AT
    FROM activities
)
SELECT
    *,
    TIMESTAMP_DIFF(END_AT, START_AT, MINUTE) AS DURATION_MINUTES,
    TIMESTAMP_DIFF(END_AT, START_AT, MINUTE)/60 AS DURATION_HOURS,
FROM corrected_end_at
