-- The projections against a recount straight from the event store, per record type.
-- expected_current applies current_records' rule to the events themselves: a record exists if its
-- highest-rev create or edit is newer than any delete of it. difference should be 0 for every type,
-- apart from events loaded in the last few minutes, before the hourly fold has caught up.
-- Reads the commit columns of the whole store.
WITH store AS (
    SELECT
        collection,
        did,
        rkey,
        max(rev) FILTER (WHERE operation IN ('create', 'update')) AS live_rev,
        max(rev) FILTER (WHERE operation = 'delete') AS delete_rev
    FROM bluesky_store.events
    WHERE kind = 'commit'
      AND collection IN (
          'app.bsky.feed.post', 'app.bsky.feed.like', 'app.bsky.feed.repost',
          'app.bsky.graph.follow', 'app.bsky.graph.block'
      )
    GROUP BY collection, did, rkey
),
expected AS (
    SELECT
        collection,
        count(*) FILTER (WHERE live_rev IS NOT NULL AND (delete_rev IS NULL OR delete_rev < live_rev)) AS expected_current,
        count(*) FILTER (WHERE live_rev IS NULL) AS deletes_of_records_never_seen
    FROM store
    GROUP BY collection
),
projected AS (
    SELECT
        collection,
        count(*) AS projected_current,
        count(*) FILTER (WHERE record_time IS NULL) AS without_record_time
    FROM bluesky_store.current_records
    GROUP BY collection
)
SELECT
    e.collection,
    e.expected_current,
    coalesce(p.projected_current, 0) AS projected_current,
    coalesce(p.projected_current, 0) - e.expected_current AS difference,
    e.deletes_of_records_never_seen,
    coalesce(p.without_record_time, 0) AS without_record_time
FROM expected e
LEFT JOIN projected p ON p.collection = e.collection
ORDER BY e.collection
