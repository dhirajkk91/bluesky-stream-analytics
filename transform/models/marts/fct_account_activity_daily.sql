-- One row per account per day it wrote anything: what it did that day.
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    partitioned_by=['day(activity_date)'],
    pre_hook="{{ clear_window('activity_date') }}"
) }}
{% set w = build_window() %}
SELECT
    activity_date,
    account_key,
    count(*) AS writes,
    count_if(action = 'post') AS posts,
    count_if(action = 'reply') AS replies,
    count_if(action = 'like') AS likes,
    count_if(action = 'repost') AS reposts,
    count_if(action = 'follow') AS follows,
    count_if(action = 'block') AS blocks,
    count_if(action = 'edit') AS edits,
    count_if(action = 'delete') AS deletes,
    count_if(action IN ('profile', 'other')) AS other_writes,
    min(event_time) AS first_write_at,
    min(CASE WHEN action = 'profile' AND operation = 'create' THEN event_time END) AS profile_created_at
FROM {{ ref('stg_commits') }}
WHERE event_time >= {{ utc_timestamp(w.start) }}
  AND event_time < {{ utc_timestamp(w.end) }}
GROUP BY activity_date, account_key
