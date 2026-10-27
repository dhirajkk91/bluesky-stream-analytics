-- Day-N retention of new accounts: of the accounts that signed up on a day, the share that
-- wrote something exactly 1, 7 and 30 days later. Only days that have already happened count.
{% set w = build_window() %}
WITH cohorts AS (
    SELECT account_key, signup_date
    FROM {{ ref('int_account_firsts') }}
    WHERE signup_date IS NOT NULL
),
offsets AS (
    SELECT 1 AS day_n UNION ALL SELECT 7 UNION ALL SELECT 30
),
due AS (
    SELECT c.account_key, c.signup_date, o.day_n, {{ dbt.dateadd('day', 'o.day_n', 'c.signup_date') }} AS check_date
    FROM cohorts c
    CROSS JOIN offsets o
),
activity AS (
    SELECT DISTINCT activity_date, account_key FROM {{ ref('fct_account_activity_daily') }}
)
SELECT
    d.signup_date AS cohort_date,
    d.day_n,
    count(*) AS cohort_size,
    count(a.account_key) AS retained,
    CAST(count(a.account_key) AS DOUBLE) / count(*) AS retention_rate
FROM due d
LEFT JOIN activity a ON a.account_key = d.account_key AND a.activity_date = d.check_date
WHERE d.check_date < DATE '{{ w.end }}'
GROUP BY d.signup_date, d.day_n
