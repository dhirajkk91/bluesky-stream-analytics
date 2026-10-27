-- One row per day: active accounts, new accounts, what they did, and how concentrated
-- the engagement they received was. "Active" means wrote something that day; the feed
-- doesn't show accounts that only read.
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
activity AS (
    SELECT activity_date, account_key
    FROM {{ ref('fct_account_activity_daily') }}
    WHERE activity_date >= {{ dbt.dateadd('day', -29, "DATE '" ~ w.start ~ "'") }}
      AND activity_date < DATE '{{ w.end }}'
),
active AS (
    SELECT
        d.date_day,
        count(DISTINCT CASE WHEN a.activity_date = d.date_day THEN a.account_key END) AS daily_active,
        count(DISTINCT CASE WHEN a.activity_date > {{ dbt.dateadd('day', -7, 'd.date_day') }} THEN a.account_key END) AS weekly_active,
        count(DISTINCT a.account_key) AS monthly_active
    FROM days d
    JOIN activity a
      ON a.activity_date > {{ dbt.dateadd('day', -30, 'd.date_day') }}
     AND a.activity_date <= d.date_day
    GROUP BY d.date_day
),
volumes AS (
    SELECT
        activity_date AS date_day,
        sum(posts) AS posts,
        sum(replies) AS replies,
        sum(likes) AS likes,
        sum(reposts) AS reposts,
        sum(follows) AS follows,
        sum(blocks) AS blocks,
        sum(deletes) AS deletes
    FROM {{ ref('fct_account_activity_daily') }}
    WHERE activity_date >= DATE '{{ w.start }}' AND activity_date < DATE '{{ w.end }}'
    GROUP BY activity_date
),
signups AS (
    SELECT signup_date AS date_day, count(*) AS new_accounts
    FROM {{ ref('int_account_firsts') }}
    WHERE signup_date >= DATE '{{ w.start }}' AND signup_date < DATE '{{ w.end }}'
    GROUP BY signup_date
),
ranked AS (
    SELECT
        activity_date AS date_day,
        engagement_received AS engagement,
        row_number() OVER (PARTITION BY activity_date ORDER BY engagement_received DESC) AS rank_desc,
        row_number() OVER (PARTITION BY activity_date ORDER BY engagement_received ASC) AS rank_asc,
        count(*) OVER (PARTITION BY activity_date) AS creators
    FROM {{ ref('fct_engagement_received_daily') }}
    WHERE activity_date >= DATE '{{ w.start }}' AND activity_date < DATE '{{ w.end }}'
      AND engagement_received > 0
),
concentration AS (
    -- The top 1% is rounded up, so a day with any engagement has at least one top creator.
    -- Gini: 0 when every creator received the same, close to 1 when one received nearly all.
    SELECT
        date_day,
        max(creators) AS creators_engaged,
        sum(engagement) AS engagement,
        CAST(sum(CASE WHEN rank_desc <= ceil(creators / 100.0) THEN engagement ELSE 0 END) AS DOUBLE)
            / sum(engagement) AS top_1pct_share,
        CAST(2 * sum(rank_asc * engagement) AS DOUBLE) / (max(creators) * sum(engagement))
            - CAST(max(creators) + 1 AS DOUBLE) / max(creators) AS gini
    FROM ranked
    GROUP BY date_day
)
SELECT
    d.date_day,
    coalesce(a.daily_active, 0) AS daily_active,
    coalesce(a.weekly_active, 0) AS weekly_active,
    coalesce(a.monthly_active, 0) AS monthly_active,
    -- The rolling windows only cover full weeks and months once collection is that old.
    d.date_day >= {{ dbt.dateadd('day', 6, "DATE '" ~ var('collection_start') ~ "'") }} AS weekly_complete,
    d.date_day >= {{ dbt.dateadd('day', 29, "DATE '" ~ var('collection_start') ~ "'") }} AS monthly_complete,
    coalesce(s.new_accounts, 0) AS new_accounts,
    coalesce(v.posts, 0) AS posts,
    coalesce(v.replies, 0) AS replies,
    coalesce(v.likes, 0) AS likes,
    coalesce(v.reposts, 0) AS reposts,
    coalesce(v.follows, 0) AS follows,
    coalesce(v.blocks, 0) AS blocks,
    coalesce(v.deletes, 0) AS deletes,
    coalesce(c.creators_engaged, 0) AS creators_engaged,
    c.top_1pct_share,
    c.gini
FROM days d
LEFT JOIN active a ON a.date_day = d.date_day
LEFT JOIN volumes v ON v.date_day = d.date_day
LEFT JOIN signups s ON s.date_day = d.date_day
LEFT JOIN concentration c ON c.date_day = d.date_day
