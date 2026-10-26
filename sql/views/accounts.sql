-- Every account seen in an identity or account event, with its current handle and status.
-- Identity and account events are a small share of the store, so this is computed from it directly.
-- Each kind of event has its own sequence number, and the highest one is the latest.
CREATE OR REPLACE VIEW bluesky_store.accounts AS
SELECT
    did,
    max_by(handle, seq) FILTER (WHERE kind = 'identity') AS handle,
    max(event_time) FILTER (WHERE kind = 'identity') AS identity_updated_at,
    max_by(account_active, seq) FILTER (WHERE kind = 'account') AS active,
    max_by(account_status, seq) FILTER (WHERE kind = 'account') AS status,
    max(event_time) FILTER (WHERE kind = 'account') AS status_updated_at
FROM bluesky_store.events
WHERE kind IN ('identity', 'account')
GROUP BY did
