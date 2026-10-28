-- One row per post per day it received likes, reposts or replies from accounts other than its author.
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    partitioned_by=['day(activity_date)'],
    pre_hook="{{ clear_window('activity_date') }}"
) }}
{% set w = build_window() %}
SELECT
    activity_date,
    {{ post_key('target_post_uri') }} AS post_key,
    {{ account_key('target_did') }} AS author_key,
    count_if(action = 'like') AS likes,
    count_if(action = 'repost') AS reposts,
    count_if(action = 'reply') AS replies
FROM {{ ref('stg_commits') }}
WHERE event_time >= {{ utc_timestamp(w.start) }}
  AND event_time < {{ utc_timestamp(w.end) }}
  AND target_post_uri IS NOT NULL
  AND target_did <> did
  AND action IN ('like', 'repost', 'reply')
GROUP BY activity_date, {{ post_key('target_post_uri') }}, {{ account_key('target_did') }}
