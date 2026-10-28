-- Changes to accounts, in the order they happened: status from account events, and handles
-- from the handle lookups. Jetstream's identity events say that a handle changed but almost
-- never what to, so the handle job looks each one up; lookups that found nothing are left out.
-- Both carry the relay's sequence number, so the two kinds of change order correctly together.
SELECT
    {{ account_key('did') }} AS account_key,
    event_time,
    seq,
    CAST(NULL AS VARCHAR) AS handle,
    CASE WHEN account_active THEN 'active' ELSE coalesce(account_status, 'inactive') END AS status
FROM {{ source('bluesky_store', 'events') }}
WHERE kind = 'account'

UNION ALL

SELECT
    {{ account_key('did') }} AS account_key,
    identity_time AS event_time,
    identity_seq AS seq,
    handle,
    CAST(NULL AS VARCHAR) AS status
FROM {{ source('bluesky_store', 'account_handles') }}
WHERE found
