-- One row per day about posts: how many were made, how many replies answered a post never seen,
-- how many posts got any engagement, and how much of the engagement went to posts under a day old.
-- Whether a parent was seen is as of the night the day was built.
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    partitioned_by=['month(date_day)'],
    pre_hook="{{ clear_window('date_day') }}"
) }}
{% set w = build_window() %}
WITH days AS (
    SELECT date_day FROM {{ ref('dim_date') }}
    WHERE date_day >= DATE '{{ w.start }}' AND date_day < DATE '{{ w.end }}'
),
created AS (
    SELECT
        c.activity_date AS date_day,
        count_if(c.action = 'post') AS posts,
        count_if(c.action = 'reply') AS replies,
        count_if(c.action = 'reply' AND p.is_placeholder) AS replies_to_unseen_posts
    FROM {{ ref('stg_commits') }} c
    LEFT JOIN {{ ref('dim_post') }} p ON p.post_key = {{ post_key('c.target_post_uri') }}
    WHERE c.event_time >= {{ utc_timestamp(w.start) }}
      AND c.event_time < {{ utc_timestamp(w.end) }}
      AND c.action IN ('post', 'reply')
    GROUP BY c.activity_date
),
engaged AS (
    SELECT
        activity_date AS date_day,
        count(*) AS posts_engaged,
        sum(likes + reposts + replies) AS engagement
    FROM {{ ref('fct_post_engagement_daily') }}
    WHERE activity_date >= DATE '{{ w.start }}' AND activity_date < DATE '{{ w.end }}'
    GROUP BY activity_date
),
fresh AS (
    SELECT
        activity_date AS date_day,
        CAST(sum(CASE WHEN age_hours BETWEEN 0 AND 23 THEN engagement ELSE 0 END) AS DOUBLE) / sum(engagement)
            AS share_to_posts_under_a_day
    FROM {{ ref('mart_engagement_by_post_age') }}
    WHERE activity_date >= DATE '{{ w.start }}' AND activity_date < DATE '{{ w.end }}'
    GROUP BY activity_date
)
SELECT
    d.date_day,
    coalesce(c.posts, 0) AS posts,
    coalesce(c.replies, 0) AS replies,
    coalesce(c.replies_to_unseen_posts, 0) AS replies_to_unseen_posts,
    coalesce(e.posts_engaged, 0) AS posts_engaged,
    coalesce(e.engagement, 0) AS engagement,
    f.share_to_posts_under_a_day
FROM days d
LEFT JOIN created c ON c.date_day = d.date_day
LEFT JOIN engaged e ON e.date_day = d.date_day
LEFT JOIN fresh f ON f.date_day = d.date_day
