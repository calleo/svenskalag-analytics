SELECT
    teamId AS TEAM_ID,
    scheduleId AS SCHEDULE_ID,
    username AS USERNAME,
    scheduleDate AS SCHEDULE_DATE,
    startTime as START_TIME,
    endTime AS END_TIME,
    eventTypeId AS EVENT_TYPE_ID,
    place AS PLACE,
    isHomeFacility AS IS_HOME_FACILITY,
    opponentClubId AS OPPONENT_CLUB_ID,
    opponent AS OPPONENT,
    resultId AS RESULT_ID,
    goalsFor AS GOALS_FOR,
    goalsAgainst AS GOALS_AGAINST
FROM {{ source('svenska_lag', 'raw_activity') }}
