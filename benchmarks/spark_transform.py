"""
The event store's transform written for Spark, kept as the reference the DuckDB job is compared against.

The pipeline ran on this until the comparison in compare_engines.py showed
DuckDB doing the same work about 7 times faster on this data. It produces the
same columns and the same rows as bluesky_stream_analytics.event_store.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

EVENT_KEY = ["did", "kind", "collection", "rkey", "rev", "operation", "seq"]

# A follow's subject is an account DID; a like's or repost's is an object with
# the post's URI. Declared as a string, Spark keeps the object as JSON text.
EVENT_SCHEMA = """
    did string,
    time_us bigint,
    kind string,
    commit struct<
        rev string, operation string, collection string, rkey string,
        record struct<subject string, langs array<string>, reply struct<parent struct<uri string>, root struct<uri string>>>
    >,
    identity struct<seq bigint, handle string>,
    account struct<seq bigint, active boolean, status string>
"""

FOLLOW_LIKE = ("app.bsky.graph.follow", "app.bsky.graph.block")
POINTS_AT_RECORD = ("app.bsky.feed.like", "app.bsky.feed.repost")
URI_PATTERN = r"^at://([^/]+)/([^/]+)/([^/]+)$"


def parse_events(lines: DataFrame) -> DataFrame:
    """Typed event rows from a `value` column of raw JSON lines, dropping lines that aren't events."""
    e = F.from_json("value", EVENT_SCHEMA)
    parsed = lines.select(e.alias("e")).where(
        F.col("e.did").isNotNull() & F.col("e.time_us").isNotNull() & F.col("e.kind").isNotNull()
    )
    collection = F.col("e.commit.collection")
    subject = F.col("e.commit.record.subject")
    uri = F.when(collection.isin(*POINTS_AT_RECORD), F.get_json_object(subject, "$.uri"))

    return parsed.select(
        F.col("e.did").alias("did"),
        F.timestamp_micros("e.time_us").alias("event_time"),
        F.col("e.kind").alias("kind"),
        collection.alias("collection"),
        F.col("e.commit.operation").alias("operation"),
        F.col("e.commit.rkey").alias("rkey"),
        F.col("e.commit.rev").alias("rev"),
        F.coalesce("e.identity.seq", "e.account.seq").alias("seq"),
        F.when(collection.isin(*FOLLOW_LIKE), subject).otherwise(_uri_part(uri, 1)).alias("subject_did"),
        _uri_part(uri, 2).alias("subject_collection"),
        _uri_part(uri, 3).alias("subject_rkey"),
        F.col("e.commit.record.reply.parent.uri").alias("reply_parent_uri"),
        F.col("e.commit.record.reply.root.uri").alias("reply_root_uri"),
        F.when(collection == "app.bsky.feed.post", F.col("e.commit.record.langs")).alias("langs"),
        F.col("e.identity.handle").alias("handle"),
        F.col("e.account.active").alias("account_active"),
        F.col("e.account.status").alias("account_status"),
    )


def _uri_part(uri: Column, n: int) -> Column:
    return F.nullif(F.regexp_extract(uri, URI_PATTERN, n), F.lit(""))


def distinct_events(events: DataFrame) -> DataFrame:
    """One row per event key."""
    return events.dropDuplicates(EVENT_KEY)
