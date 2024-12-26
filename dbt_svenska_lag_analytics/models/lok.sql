WITH eligiable_activities AS (
    SELECT
        ACTIVITY_ID,
        SUM(
            CASE WHEN member.MEMBER_TYPE_LABEL = 'Player' AND DATE_DIFF('year', DATE_OF_BIRTH, START_AT) BETWEEN 7 AND 25 THEN 1 ELSE 0 END
        ) AS PLAYERS_ATTENDED,
        SUM(
            CASE WHEN member.MEMBER_TYPE_LABEL = 'Leader' THEN 1 ELSE 0 END
        ) AS LEADERS_ATTENDED
    FROM {{ ref('attended_activity') }}
    LEFT JOIN {{ ref('member') }} USING(member_id)
    LEFT JOIN {{ ref('activity') }} USING(ACTIVITY_ID)
    GROUP BY ALL
    HAVING PLAYERS_ATTENDED >= 3 AND LEADERS_ATTENDED >= 1
), add_acitivity_count AS (
    SELECT
        ACTIVITY_ID,
        MEMBER_ID,
        MEMBER_TYPE_LABEL,
        START_AT,
        END_AT,
        DATE_TRUNC('week', START_AT)::DATE AS WEEK,
        DATE_DIFF('year', DATE_OF_BIRTH, START_AT) AS MEMBER_AGE,
        ROW_NUMBER() OVER(PARTITION BY MEMBER_ID, START_AT::DATE ORDER BY START_AT ASC) AS ACC_DAILY_ACTIVITIES,
        ROW_NUMBER() OVER(PARTITION BY MEMBER_ID, WEEK ORDER BY START_AT ASC) AS ACC_WEEKLY_ACTIVITIES
    FROM {{ ref('attended_activity') }}
    LEFT JOIN {{ ref('activity') }} USING(ACTIVITY_ID)
    LEFT JOIN {{ ref('member') }} USING(member_id)
    WHERE ACTIVITY_ID IN (SELECT ACTIVITY_ID FROM eligiable_activities)
), activity_summary AS (
    SELECT
        ACTIVITY_ID,
        START_AT,
        ACC_DAILY_ACTIVITIES,
        ACC_WEEKLY_ACTIVITIES,
        -- https://www.rf.se/bidrag-och-stod/lok-stod
        CASE
            WHEN ACC_DAILY_ACTIVITIES > 1 THEN 0
            WHEN MEMBER_AGE BETWEEN 7 AND 9 AND ACC_WEEKLY_ACTIVITIES IN (1, 2) THEN 7.5
            WHEN MEMBER_AGE BETWEEN 7 AND 9 AND ACC_WEEKLY_ACTIVITIES = 3 THEN 5
            WHEN MEMBER_AGE BETWEEN 10 AND 25 AND ACC_WEEKLY_ACTIVITIES IN (1, 2) THEN 7.5
            WHEN MEMBER_AGE BETWEEN 10 AND 25 AND ACC_WEEKLY_ACTIVITIES = 3 THEN 5
            WHEN MEMBER_AGE BETWEEN 10 AND 25 AND ACC_WEEKLY_ACTIVITIES IN (4, 5) THEN 2.5
            ELSE 0
        END AS LOK_AMOUNT_PLAYER
    FROM add_acitivity_count
)
SELECT
    ACTIVITY_ID,
    START_AT,
    SUM(LOK_AMOUNT_PLAYER) AS LOK_AMOUNT_PLAYERS,
    MAX(
        CASE
            WHEN eligiable_activities.LEADERS_ATTENDED = 1 THEN 20
            WHEN eligiable_activities.LEADERS_ATTENDED > 1 THEN 25
            ELSE 0
        END
    ) AS LOK_AMOUNT_LEADERS
FROM activity_summary
LEFT JOIN eligiable_activities USING(ACTIVITY_ID)
GROUP BY ALL
