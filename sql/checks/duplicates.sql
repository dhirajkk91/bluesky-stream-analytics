-- Rows stored versus distinct events, per hour.
-- An event is identified by its account, record and revision (commits) or its sequence number (identity and account events).
WITH keyed AS (
    SELECT
        dt,
        hr,
        concat_ws('|', did, kind, coalesce(collection, ''), coalesce(rkey, ''), coalesce(rev, ''), coalesce(operation, ''), coalesce(seq, '')) AS event_key
    FROM events
)
SELECT
    dt,
    hr,
    count(*) AS rows_stored,
    count(DISTINCT event_key) AS distinct_events,
    count(*) - count(DISTINCT event_key) AS duplicates
FROM keyed
GROUP BY dt, hr
ORDER BY dt, hr
