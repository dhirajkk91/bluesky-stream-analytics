-- Anyone active today was active this week, and anyone active this week was active this month.
SELECT date_day, daily_active, weekly_active, monthly_active
FROM {{ ref('mart_daily_metrics') }}
WHERE daily_active > weekly_active OR weekly_active > monthly_active
