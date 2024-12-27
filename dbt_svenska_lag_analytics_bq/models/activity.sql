WITH activities AS (
    SELECT
        SCHEDULE_ID AS ACTIVITY_ID,
        ACTIVITY_TYPE_ID,
        activity_type.NAME AS ACTIVITY,
        TIMESTAMP(CONCAT(SCHEDULE_DATE, ' ', START_TIME, ':00'), 'Europe/Stockholm') AS START_AT,
        TIMESTAMP(CONCAT(SCHEDULE_DATE, ' ', END_TIME, ':00'), 'Europe/Stockholm') AS END_AT,
        IF(END_TIME = '00:00', 1, 0) AS ADD_DAYS, -- Svenska Lag start/end logic
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
        * EXCEPT(END_AT, ADD_DAYS),
        TIMESTAMP_ADD(END_AT, INTERVAL ADD_DAYS DAY) AS END_AT
    FROM activities
)
SELECT
    *,
    TIMESTAMP_DIFF(END_AT, START_AT, MINUTE) AS DURATION_MINUTES,
    TIMESTAMP_DIFF(END_AT, START_AT, MINUTE)/60 AS DURATION_HOURS,
FROM corrected_end_at
