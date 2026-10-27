-- One row per account per day it received engagement from another account.
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    partitioned_by=['day(activity_date)'],
    pre_hook="{{ clear_window('activity_date') }}"
) }}
{% set w = build_window() %}
SELECT
    activity_date,
    {{ account_key('target_did') }} AS account_key,
    count_if(action = 'like') AS likes_received,
    count_if(action = 'repost') AS reposts_received,
    count_if(action = 'reply') AS replies_received,
    count_if(action = 'follow') AS follows_received,
    count_if(action IN ('like', 'repost', 'reply')) AS engagement_received
FROM {{ ref('stg_commits') }}
WHERE event_time >= {{ utc_timestamp(w.start) }}
  AND event_time < {{ utc_timestamp(w.end) }}
  AND target_did IS NOT NULL
  AND target_did <> did
  AND action IN ('like', 'repost', 'reply', 'follow')
GROUP BY activity_date, {{ account_key('target_did') }}
