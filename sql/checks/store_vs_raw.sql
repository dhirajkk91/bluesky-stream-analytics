-- Distinct raw events against rows in the event store, per hour, for yesterday (UTC).
-- Every row should show 0 missing and 0 extra. Reads one day of raw files, about 5 GB.
WITH bounds AS (
    SELECT
        CAST(current_date - INTERVAL '1' DAY AS timestamp) AS day_start,
        CAST(current_date AS timestamp) AS day_end
),
raw AS (
    SELECT
        date_trunc('hour', event_time) AS hour,
        count(DISTINCT concat_ws('|', did, kind, coalesce(collection, ''), coalesce(rkey, ''), coalesce(rev, ''), coalesce(operation, ''), coalesce(seq, ''))) AS raw_events
    FROM events, bounds
    -- A rewind just after midnight can file the day's last seconds in the next day's first hour.
    WHERE (dt = CAST(current_date - INTERVAL '1' DAY AS varchar) OR (dt = CAST(current_date AS varchar) AND hr = '00'))
      AND event_time >= day_start
      AND event_time < day_end
    GROUP BY 1
),
store AS (
    -- The table stores event_time with its UTC zone; drop the zone to match the view.
    SELECT
        CAST(date_trunc('hour', event_time) AS timestamp) AS hour,
        count(*) AS store_rows
    FROM bluesky_store.events, bounds
    WHERE event_time >= day_start
      AND event_time < day_end
    GROUP BY 1
)
SELECT
    coalesce(raw.hour, store.hour) AS hour,
    coalesce(raw_events, 0) AS raw_events,
    coalesce(store_rows, 0) AS store_rows,
    greatest(coalesce(raw_events, 0) - coalesce(store_rows, 0), 0) AS missing_from_store,
    greatest(coalesce(store_rows, 0) - coalesce(raw_events, 0), 0) AS extra_in_store
FROM raw
FULL OUTER JOIN store ON raw.hour = store.hour
ORDER BY 1
