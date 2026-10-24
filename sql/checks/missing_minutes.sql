-- Minutes with no events between the first and last event collected.
WITH per_minute AS (
    SELECT date_trunc('minute', event_time) AS minute, count(*) AS events
    FROM events
    GROUP BY 1
),
bounds AS (
    SELECT min(minute) AS first_minute, max(minute) AS last_minute
    FROM per_minute
),
all_minutes AS (
    SELECT minute
    FROM bounds
    CROSS JOIN UNNEST(sequence(first_minute, last_minute, INTERVAL '1' MINUTE)) AS t (minute)
)
SELECT a.minute
FROM all_minutes AS a
LEFT JOIN per_minute AS p ON a.minute = p.minute
WHERE p.minute IS NULL
ORDER BY a.minute
