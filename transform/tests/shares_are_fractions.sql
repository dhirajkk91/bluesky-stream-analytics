-- Shares, rates and the Gini coefficient all lie between 0 and 1.
SELECT 'mart_daily_metrics' AS model, CAST(date_day AS VARCHAR) AS day FROM {{ ref('mart_daily_metrics') }}
WHERE top_1pct_share < 0 OR top_1pct_share > 1 OR gini < 0 OR gini > 1
UNION ALL
SELECT 'mart_retention', CAST(cohort_date AS VARCHAR) FROM {{ ref('mart_retention') }}
WHERE retention_rate < 0 OR retention_rate > 1
