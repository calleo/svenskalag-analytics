WITH leaders AS (
    SELECT
        scheduleId AS SCHEDULE_ID,
        leader AS LEADER
    FROM {{ source('dwh', 'raw_presence') }},
    UNNEST(teamPresenceGroups[OFFSET(0)].leaders) AS leader
    WHERE LOAD_ID = (SELECT LOAD_ID FROM {{ source('dwh', 'raw_presence') }} ORDER BY LOAD_TIMESTAMP DESC LIMIT 1)
),
players AS (
    SELECT
        scheduleId AS SCHEDULE_ID,
        player AS PLAYER
    FROM {{ source('dwh', 'raw_presence') }},
    UNNEST(teamPresenceGroups[OFFSET(0)].participants) AS player
    WHERE LOAD_ID = (SELECT LOAD_ID FROM {{ source('dwh', 'raw_presence') }} ORDER BY LOAD_TIMESTAMP DESC LIMIT 1)
)
SELECT
    LEADER.ID,
    SCHEDULE_ID,
    LEADER.firstName AS FIRST_NAME,
    LEADER.lastName AS LAST_NAME,
    LEADER.userId AS USER_ID,
    LEADER.teamName AS TEAM_NAME,
    LEADER.inactivatedDate AS INACTIVATED_AT,
    LEADER.stats.star AS STATS_STAR,
    DATE(LEADER.birthDate) AS DATE_OF_BIRTH,
    LEADER.memberTypeId AS MEMBER_TYPE_ID
FROM leaders
UNION ALL
SELECT
    PLAYER.ID,
    SCHEDULE_ID,
    PLAYER.firstName AS FIRST_NAME,
    PLAYER.lastName AS LAST_NAME,
    PLAYER.userId AS USER_ID,
    PLAYER.teamName AS TEAM_NAME,
    PLAYER.inactivatedDate AS INACTIVATED_AT,
    PLAYER.stats.star AS STATS_STAR,
    DATE(PLAYER.birthDate) AS DATE_OF_BIRTH,
    PLAYER.memberTypeId AS MEMBER_TYPE_ID
FROM players
