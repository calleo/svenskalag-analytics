SELECT
    *
FROM {{ ref('attended_activity') }}
LEFT JOIN {{ ref('member') }} USING(member_id)
LEFT JOIN {{ ref('activity') }} USING(activity_id)
