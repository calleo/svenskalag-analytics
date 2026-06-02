WITH training_activities AS (
    SELECT
        ACTIVITY_ID,
        CAST(START_AT AS DATE) AS ACTIVITY_DATE
    FROM {{ ref('activity') }}
    WHERE ACTIVITY_TYPE_ID = 1
      AND START_AT < CURRENT_TIMESTAMP()
), available_trainings AS (
    SELECT
        COUNTIF(ACTIVITY_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK)) AS AVAILABLE_4W,
        COUNTIF(ACTIVITY_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)) AS AVAILABLE_12W,
        COUNTIF(ACTIVITY_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 WEEK)) AS AVAILABLE_24W
    FROM training_activities
), member_training_attendance AS (
    SELECT
        attended_activity.MEMBER_ID,
        training_activities.ACTIVITY_DATE
    FROM {{ ref('attended_activity') }} AS attended_activity
    INNER JOIN training_activities USING (ACTIVITY_ID)
), attended_trainings AS (
    SELECT
        MEMBER_ID,
        COUNTIF(ACTIVITY_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK)) AS TRAININGS_4W,
        COUNTIF(ACTIVITY_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)) AS TRAININGS_12W,
        COUNTIF(ACTIVITY_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 WEEK)) AS TRAININGS_24W
    FROM member_training_attendance
    GROUP BY MEMBER_ID
)
SELECT
    member.MEMBER_ID,
    member.FULL_NAME,
    member.MEMBER_TYPE,
    COALESCE(attended_trainings.TRAININGS_4W, 0) AS TRAININGS_4W,
    ROUND(SAFE_DIVIDE(COALESCE(attended_trainings.TRAININGS_4W, 0), available_trainings.AVAILABLE_4W) * 100, 1) AS RATE_4W,
    COALESCE(attended_trainings.TRAININGS_12W, 0) AS TRAININGS_12W,
    ROUND(SAFE_DIVIDE(COALESCE(attended_trainings.TRAININGS_12W, 0), available_trainings.AVAILABLE_12W) * 100, 1) AS RATE_12W,
    COALESCE(attended_trainings.TRAININGS_24W, 0) AS TRAININGS_24W,
    ROUND(SAFE_DIVIDE(COALESCE(attended_trainings.TRAININGS_24W, 0), available_trainings.AVAILABLE_24W) * 100, 1) AS RATE_24W
FROM {{ ref('member') }} AS member
LEFT JOIN attended_trainings USING (MEMBER_ID)
CROSS JOIN available_trainings
WHERE member.MEMBER_TYPE = 'Player'
