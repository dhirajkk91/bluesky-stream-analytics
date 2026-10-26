-- What exists right now: one row per like, post, repost, follow and block, at its latest version,
-- with deleted records left out. Built from the projection tables the hourly job appends to.
-- A record's latest version is its highest rev. It's deleted if a delete marker has a higher rev,
-- so the result is the same however often, or in whatever order, events were appended.
CREATE OR REPLACE VIEW bluesky_store.current_records AS
SELECT
    did,
    collection,
    rkey,
    rev,
    record_time,
    event_time AS updated_at,
    subject_did,
    subject_collection,
    subject_rkey,
    reply_parent_uri,
    reply_root_uri,
    langs
FROM (
    SELECT
        r.*,
        row_number() OVER (PARTITION BY r.did, r.collection, r.rkey ORDER BY r.rev DESC) AS version_rank
    FROM bluesky_store.records r
) latest
WHERE version_rank = 1
  AND NOT EXISTS (
      SELECT 1
      FROM bluesky_store.record_deletes d
      WHERE d.did = latest.did
        AND d.collection = latest.collection
        AND d.rkey = latest.rkey
        AND d.rev > latest.rev
  )
