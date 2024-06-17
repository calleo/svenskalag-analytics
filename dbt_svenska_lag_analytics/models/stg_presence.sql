WITH leaders AS (
    SELECT
        scheduleId AS SCHEDULE_ID,
        unnest(teamPresenceGroups[1].leaders) AS LEADER
    FROM {{ source('svenska_lag', 'raw_presence') }}
), players AS (
    SELECT
        scheduleId AS SCHEDULE_ID,
        unnest(teamPresenceGroups[1].participants) AS PLAYER
    FROM {{ source('svenska_lag', 'raw_presence') }}
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
    LEADER.birthDate::DATE AS DATE_OF_BIRTH,
    LEADER.isLeader AS IS_LEADER,
    LEADER.isPlayer AS IS_PLAYER
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
    PLAYER.birthDate::DATE AS DATE_OF_BIRTH,
    PLAYER.isLeader AS IS_LEADER,
    PLAYER.isPlayer AS IS_PLAYER
FROM players
