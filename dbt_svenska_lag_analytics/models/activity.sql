WITH activities AS (
    SELECT
        SCHEDULE_ID AS ACTIVITY_ID,
        EVENT_TYPE_ID,
        CASE
            WHEN EVENT_TYPE_ID = 1 THEN 'Training'
            WHEN EVENT_TYPE_ID = 2 THEN 'Meeting'
            WHEN EVENT_TYPE_ID = 4 THEN 'Other'
            WHEN EVENT_TYPE_ID = 5 THEN 'Competition'
            WHEN EVENT_TYPE_ID = 6 THEN 'Cup'
            WHEN EVENT_TYPE_ID = 7 THEN 'Match'
            WHEN EVENT_TYPE_ID = 9 THEN 'Work Shift'
            WHEN EVENT_TYPE_ID = 10 THEN 'Camp'
        END AS EVENT_TYPE_LABEL,
        CONCAT(SCHEDULE_DATE, ' ', START_TIME, ':00 Europe/Stockholm')::TIMESTAMPTZ AS START_AT,
        CONCAT(SCHEDULE_DATE, ' ', END_TIME, ':00 Europe/Stockholm')::TIMESTAMPTZ AS END_AT,
        PLACE,
        IS_HOME_FACILITY,
        OPPONENT_CLUB_ID,
        OPPONENT,
        RESULT_ID,
        CASE
            WHEN RESULT_ID = 1 THEN 'Loss'
            WHEN RESULT_ID = 2 THEN 'Draw'
            WHEN RESULT_ID = 3 THEN 'Win'
        END AS RESULT_LABEL,
        GOALS_FOR,
        GOALS_AGAINST
    FROM {{ ref('stg_activity') }}
), corrected_end_at AS (
    SELECT
        * EXCLUDE (END_AT),
        CASE
            WHEN HOUR(END_AT) = 0 AND MINUTE(END_AT) = 0 THEN END_AT + INTERVAL 1 DAY
            ELSE END_AT
        END AS END_AT
    FROM activities
)
SELECT
    *,
    DATE_DIFF('minute', START_AT, END_AT) AS DURATION_MINUTES
FROM corrected_end_at
