-- Every account has exactly one current version, and versions don't overlap.
SELECT account_key
FROM {{ ref('dim_account') }}
GROUP BY account_key
HAVING count_if(is_current) <> 1
UNION ALL
SELECT account_key
FROM {{ ref('dim_account') }}
WHERE valid_to IS NOT NULL AND valid_to < valid_from
