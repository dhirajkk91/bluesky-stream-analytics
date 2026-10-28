-- Posts and replies made each day, by the first language their author tagged. Untagged posts
-- count under 'none'.
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    partitioned_by=['month(date_day)'],
    pre_hook="{{ clear_window('date_day') }}"
) }}
{% set w = build_window() %}
SELECT
    activity_date AS date_day,
    coalesce(language, 'none') AS language,
    count(*) AS posts
FROM {{ ref('stg_commits') }}
WHERE event_time >= {{ utc_timestamp(w.start) }}
  AND event_time < {{ utc_timestamp(w.end) }}
  AND action IN ('post', 'reply')
GROUP BY activity_date, coalesce(language, 'none')
