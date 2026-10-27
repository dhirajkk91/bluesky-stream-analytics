-- One row per day from the start of collection to the last complete day.
{% set w = build_window() %}
WITH days AS ({{ days_between(var('collection_start'), w.end) }})
SELECT
    date_day,
    {{ iso_day_of_week('date_day') }} AS iso_day_of_week,
    {{ iso_day_of_week('date_day') }} IN (6, 7) AS is_weekend,
    CAST(date_trunc('week', date_day) AS DATE) AS week_start,
    CAST(date_trunc('month', date_day) AS DATE) AS month_start
FROM days
