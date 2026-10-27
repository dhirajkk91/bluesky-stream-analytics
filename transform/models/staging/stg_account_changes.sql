-- Handle changes (identity events) and status changes (account events), in the order they happened.
SELECT
    did,
    {{ account_key('did') }} AS account_key,
    event_time,
    seq,
    kind,
    CASE WHEN kind = 'identity' THEN handle END AS handle,
    CASE
        WHEN kind <> 'account' THEN NULL
        WHEN account_active THEN 'active'
        ELSE coalesce(account_status, 'inactive')
    END AS status
FROM {{ source('bluesky_store', 'events') }}
WHERE kind IN ('identity', 'account')
