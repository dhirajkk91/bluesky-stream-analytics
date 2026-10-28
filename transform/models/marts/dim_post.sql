-- One row per post that exists now, plus a placeholder for every post a reply points at that
-- hasn't been seen: one created before collection began, or one whose create hasn't arrived yet.
-- That's Kimball's inferred member: a reply's parent always has a row to join to, and the
-- placeholder gives way to the real post once its create is seen. A placeholder still knows its
-- author and, from the key, when it was created. Built in full from the projections each night,
-- so deleted posts drop out.
WITH live AS (
    SELECT
        'at://' || did || '/app.bsky.feed.post/' || rkey AS uri,
        did,
        record_time,
        CASE WHEN reply_parent_uri LIKE 'at://%/app.bsky.feed.post/%' THEN reply_parent_uri END AS parent_uri,
        CASE WHEN reply_root_uri LIKE 'at://%/app.bsky.feed.post/%' THEN reply_root_uri END AS root_uri,
        reply_parent_uri IS NOT NULL AS is_reply,
        langs
    FROM {{ source('bluesky_store', 'current_records') }}
    WHERE collection = 'app.bsky.feed.post'
),
referenced AS (
    SELECT parent_uri AS uri FROM live WHERE parent_uri IS NOT NULL
    UNION
    SELECT root_uri FROM live WHERE root_uri IS NOT NULL
),
placeholders AS (
    SELECT uri FROM referenced r
    WHERE NOT EXISTS (SELECT 1 FROM live l WHERE l.uri = r.uri)
)
SELECT
    {{ post_key('uri') }} AS post_key,
    {{ account_key('did') }} AS author_key,
    record_time AS created_at,
    FALSE AS is_placeholder,
    is_reply,
    {{ post_key('parent_uri') }} AS parent_post_key,
    {{ post_key('root_uri') }} AS root_post_key,
    {{ first_language('langs') }} AS language
FROM live

UNION ALL

SELECT
    {{ post_key('uri') }} AS post_key,
    {{ account_key("regexp_extract(uri, '^at://([^/]+)/', 1)") }} AS author_key,
    {{ tid_time("regexp_extract(uri, '([^/]+)$', 1)") }} AS created_at,
    TRUE AS is_placeholder,
    CAST(NULL AS BOOLEAN) AS is_reply,
    CAST(NULL AS VARCHAR) AS parent_post_key,
    CAST(NULL AS VARCHAR) AS root_post_key,
    CAST(NULL AS VARCHAR) AS language
FROM placeholders
