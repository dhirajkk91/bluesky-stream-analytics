-- Account history, slowly changing dimension type 2: a new version whenever the handle or
-- the account's status changes. Accounts never seen in an identity or account event get one
-- version with an unknown handle and status, from when they were first seen writing.
WITH changes AS (
    SELECT
        account_key,
        event_time,
        seq,
        handle,
        status,
        -- count() skips nulls, so these number the runs that share one known value.
        count(handle) OVER (PARTITION BY account_key ORDER BY event_time, seq ROWS UNBOUNDED PRECEDING) AS handle_run,
        count(status) OVER (PARTITION BY account_key ORDER BY event_time, seq ROWS UNBOUNDED PRECEDING) AS status_run
    FROM {{ ref('stg_account_changes') }}
),
filled AS (
    SELECT
        account_key,
        event_time,
        seq,
        max(handle) OVER (PARTITION BY account_key, handle_run) AS handle,
        max(status) OVER (PARTITION BY account_key, status_run) AS status
    FROM changes
),
compared AS (
    SELECT
        *,
        lag(handle) OVER (PARTITION BY account_key ORDER BY event_time, seq) AS previous_handle,
        lag(status) OVER (PARTITION BY account_key ORDER BY event_time, seq) AS previous_status,
        row_number() OVER (PARTITION BY account_key ORDER BY event_time, seq) AS change_number
    FROM filled
),
version_starts AS (
    SELECT account_key, event_time AS valid_from, handle, status
    FROM compared
    WHERE change_number = 1
       OR handle IS DISTINCT FROM previous_handle
       OR status IS DISTINCT FROM previous_status
),
never_changed AS (
    SELECT f.account_key, f.first_seen_at AS valid_from, CAST(NULL AS VARCHAR) AS handle, CAST(NULL AS VARCHAR) AS status
    FROM {{ ref('int_account_firsts') }} f
    WHERE NOT EXISTS (SELECT 1 FROM version_starts v WHERE v.account_key = f.account_key)
),
versions AS (
    SELECT * FROM version_starts
    UNION ALL
    SELECT * FROM never_changed
)
SELECT
    account_key,
    row_number() OVER (PARTITION BY account_key ORDER BY valid_from) AS version,
    handle,
    coalesce(status, 'unknown') AS status,
    valid_from,
    lead(valid_from) OVER (PARTITION BY account_key ORDER BY valid_from) AS valid_to,
    lead(valid_from) OVER (PARTITION BY account_key ORDER BY valid_from) IS NULL AS is_current
FROM versions
