SELECT
    MD5(CONCAT(IFNULL(CAST(SCHEDULE_ID AS STRING), ''), IFNULL(CAST(USER_ID AS STRING), ''))) AS ATTENDANCE_ID,
    SCHEDULE_ID AS ACTIVITY_ID,
    USER_ID AS MEMBER_ID
FROM {{ ref('stg_presence') }}