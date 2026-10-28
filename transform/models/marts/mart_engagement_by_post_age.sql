-- How fast engagement arrives: the likes, reposts and replies received each day, by how many
-- hours old the post was at the time. The age comes from the post's key, so it works for posts
-- created before collection began too. Hour 48 holds everything two days old or more, and -1
-- posts whose key doesn't give a believable creation time.
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    partitioned_by=['month(activity_date)'],
    pre_hook="{{ clear_window('activity_date') }}"
) }}
{% set w = build_window() %}
WITH aged AS (
    SELECT
        activity_date,
        action,
        {{ seconds_between(tid_time("regexp_extract(target_post_uri, '([^/]+)$', 1)"), 'event_time') }} AS age_seconds
    FROM {{ ref('stg_commits') }}
    WHERE event_time >= {{ utc_timestamp(w.start) }}
      AND event_time < {{ utc_timestamp(w.end) }}
      AND target_post_uri IS NOT NULL
      AND target_did <> did
      AND action IN ('like', 'repost', 'reply')
),
bucketed AS (
    SELECT
        activity_date,
        action,
        CASE
            WHEN age_seconds IS NULL OR age_seconds < 0 THEN -1
            WHEN age_seconds >= 48 * 3600 THEN 48
            ELSE CAST(floor(age_seconds / 3600) AS INTEGER)
        END AS age_hours
    FROM aged
)
SELECT
    activity_date,
    age_hours,
    count_if(action = 'like') AS likes,
    count_if(action = 'repost') AS reposts,
    count_if(action = 'reply') AS replies,
    count(*) AS engagement
FROM bucketed
GROUP BY activity_date, age_hours
