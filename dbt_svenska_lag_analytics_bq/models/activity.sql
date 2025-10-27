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
        GOALS_AGAINST,
        SEASON_ID,
        CUP_NR_OF_GAMES,
    FROM {{ ref('stg_activity') }}
    LEFT JOIN {{ ref('activity_type') }} USING(ACTIVITY_TYPE_ID)
    LEFT JOIN {{ ref('result_type') }} USING(RESULT_TYPE_ID)
), corrected_end_at AS (
    SELECT
        * EXCEPT(END_AT, ADD_DAYS),
        TIMESTAMP_ADD(END_AT, INTERVAL ADD_DAYS DAY) AS END_AT
    FROM activities
), participants AS (
    SELECT
        SCHEDULE_ID AS ACTIVITY_ID,
        SUM(IF(member.MEMBER_TYPE = 'Player', 1, 0)) AS PLAYERS_ATTENDED,
        SUM(IF(member.MEMBER_TYPE = 'Leader', 1, 0)) AS LEADER_ATTENDED,
        SUM(IF(member.MEMBER_TYPE = 'Parent', 1, 0)) AS PARENTS_ATTENDED,
    FROM {{ ref('stg_presence') }}
    LEFT JOIN {{ ref('member') }} ON member.MEMBER_ID = stg_presence.USER_ID
    GROUP BY ACTIVITY_ID
)
SELECT
    corrected_end_at.*,
    TIMESTAMP_DIFF(END_AT, START_AT, MINUTE) AS DURATION_MINUTES,
    TIMESTAMP_DIFF(END_AT, START_AT, MINUTE)/60 AS DURATION_HOURS,
    PLAYERS_ATTENDED,
    LEADER_ATTENDED,
    PARENTS_ATTENDED
FROM corrected_end_at
LEFT JOIN participants USING (ACTIVITY_ID)
