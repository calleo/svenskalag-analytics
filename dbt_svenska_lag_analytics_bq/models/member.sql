WITH members AS (
    SELECT
        USER_ID AS MEMBER_ID,
        LEFT(REPLACE(TO_JSON_STRING(MD5(CAST(USER_ID AS STRING))), '"', ''), 6) AS MEMBER_HASH,
        NULLIF(FIRST_NAME, '') AS FIRST_NAME,
        NULLIF(LAST_NAME, '') AS LAST_NAME,
        TEAM_NAME,
        INACTIVATED_AT,
        DATE_OF_BIRTH,
        IF(EXTRACT(YEAR FROM DATE_OF_BIRTH) = 2014, TRUE, FALSE) AS IS_F2014,
        MEMBER_TYPE_ID,
        member_type.NAME AS MEMBER_TYPE,
    FROM {{ ref('stg_presence') }}
    LEFT JOIN {{ ref('member_type') }} USING(MEMBER_TYPE_ID)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY SCHEDULE_ID DESC) = 1
), deleted_names AS (
    SELECT
        MEMBER_ID,
        TEAM_NAME,
        INACTIVATED_AT,
        DATE_OF_BIRTH,
        IS_F2014,
        MEMBER_TYPE_ID,
        MEMBER_TYPE,
        COALESCE(FIRST_NAME, 'FN-' || MEMBER_HASH) AS FIRST_NAME,
        COALESCE(LAST_NAME, 'EN-' || MEMBER_HASH) AS LAST_NAME,
    FROM members
)
SELECT
    *,
    FIRST_NAME || ' ' || LAST_NAME AS FULL_NAME,
FROM deleted_names
