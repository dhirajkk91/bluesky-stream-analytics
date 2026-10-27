-- When each account was first seen writing, and whether that looks like a new account.
-- A signup is a profile record created with nothing seen from the account more than an hour
-- before it: an account that existed before collection began would have written earlier.
SELECT
    account_key,
    min(activity_date) AS first_activity_date,
    min(first_write_at) AS first_seen_at,
    min(profile_created_at) AS profile_created_at,
    CASE
        WHEN min(profile_created_at) IS NOT NULL
         AND min(first_write_at) >= min(profile_created_at) - INTERVAL '1' HOUR
        THEN {{ utc_date('min(profile_created_at)') }}
    END AS signup_date
FROM {{ ref('fct_account_activity_daily') }}
GROUP BY account_key
