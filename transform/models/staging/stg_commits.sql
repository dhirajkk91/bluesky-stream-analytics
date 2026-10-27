-- Every write to a Bluesky record type, classified into the actions the metrics count.
-- target_did is the account an action is aimed at: the author of a liked, reposted or
-- replied-to post, or the followed or blocked account. Redaction blanks it for records
-- that were later deleted, so those stop counting as engagement received.
SELECT
    did,
    {{ account_key('did') }} AS account_key,
    event_time,
    {{ utc_date('event_time') }} AS activity_date,
    collection,
    operation,
    CASE
        WHEN operation = 'delete' THEN 'delete'
        WHEN operation = 'update' THEN 'edit'
        WHEN collection = 'app.bsky.feed.post' AND reply_parent_uri IS NOT NULL THEN 'reply'
        WHEN collection = 'app.bsky.feed.post' THEN 'post'
        WHEN collection = 'app.bsky.feed.like' THEN 'like'
        WHEN collection = 'app.bsky.feed.repost' THEN 'repost'
        WHEN collection = 'app.bsky.graph.follow' THEN 'follow'
        WHEN collection = 'app.bsky.graph.block' THEN 'block'
        WHEN collection = 'app.bsky.actor.profile' THEN 'profile'
        ELSE 'other'
    END AS action,
    CASE
        WHEN operation <> 'create' THEN NULL
        WHEN collection IN ('app.bsky.feed.like', 'app.bsky.feed.repost', 'app.bsky.graph.follow', 'app.bsky.graph.block')
            THEN subject_did
        WHEN collection = 'app.bsky.feed.post'
            THEN nullif(regexp_extract(reply_parent_uri, '^at://([^/]+)/', 1), '')
    END AS target_did
FROM {{ source('bluesky_store', 'events') }}
WHERE kind = 'commit'
  AND collection LIKE 'app.bsky.%'
