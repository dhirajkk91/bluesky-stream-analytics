"""
Turn raw Jetstream lines into typed rows for the event store.

One row per distinct event. Commits are identified by account, record type,
record key, revision and operation; identity and account events by their
sequence number. Post text and other record content are not kept.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

EVENT_KEY = ["did", "kind", "collection", "rkey", "rev", "operation", "seq"]

# Only the fields the table keeps. A follow's subject is an account DID; a
# like's or repost's is an object with the post's URI. Declared as a string,
# Spark keeps the object as JSON text.
EVENT_SCHEMA = """
    did string,
    time_us bigint,
    kind string,
    commit struct<
        rev string, operation string, collection string, rkey string, cid string,
        record struct<subject string, langs array<string>, reply struct<parent struct<uri string>, root struct<uri string>>>
    >,
    identity struct<seq bigint, handle string>,
    account struct<seq bigint, active boolean, status string>
"""

LIKE = "app.bsky.feed.like"
REPOST = "app.bsky.feed.repost"
FOLLOW = "app.bsky.graph.follow"
BLOCK = "app.bsky.graph.block"
POST = "app.bsky.feed.post"

TABLE_COLUMNS = """
    did string NOT NULL,
    time_us bigint NOT NULL,
    event_time timestamp NOT NULL,
    kind string NOT NULL,
    collection string,
    operation string,
    rkey string,
    rev string,
    cid string,
    seq bigint,
    subject_did string,
    subject_uri string,
    reply_parent_uri string,
    reply_root_uri string,
    langs array<string>,
    handle string,
    account_active boolean,
    account_status string,
    source_file string NOT NULL
"""


def parse_events(lines: DataFrame) -> DataFrame:
    """Parse raw lines into typed event rows, dropping lines that aren't valid events."""
    return readable(parse_lines(lines))


def parse_lines(lines: DataFrame) -> DataFrame:
    """
    Parse raw lines into typed rows, one per line.

    Expects a `value` column with one JSON event per row and a `source_file`
    column. A line that isn't a valid event comes out with did, time_us or kind
    null; `readable` drops those.
    """
    parsed = lines.select(F.from_json("value", EVENT_SCHEMA).alias("e"), "source_file")
    collection = F.col("e.commit.collection")
    subject = F.col("e.commit.record.subject")
    subject_uri = F.when(collection.isin(LIKE, REPOST), F.get_json_object(subject, "$.uri"))

    return parsed.select(
        F.col("e.did").alias("did"),
        F.col("e.time_us").alias("time_us"),
        F.timestamp_micros("e.time_us").alias("event_time"),
        F.col("e.kind").alias("kind"),
        collection.alias("collection"),
        F.col("e.commit.operation").alias("operation"),
        F.col("e.commit.rkey").alias("rkey"),
        F.col("e.commit.rev").alias("rev"),
        F.col("e.commit.cid").alias("cid"),
        F.coalesce("e.identity.seq", "e.account.seq").alias("seq"),
        F.when(collection.isin(FOLLOW, BLOCK), subject)
        .otherwise(_did_of(subject_uri))
        .alias("subject_did"),
        subject_uri.alias("subject_uri"),
        F.col("e.commit.record.reply.parent.uri").alias("reply_parent_uri"),
        F.col("e.commit.record.reply.root.uri").alias("reply_root_uri"),
        F.when(collection == POST, F.col("e.commit.record.langs")).alias("langs"),
        F.col("e.identity.handle").alias("handle"),
        F.col("e.account.active").alias("account_active"),
        F.col("e.account.status").alias("account_status"),
        "source_file",
    )


def readable(rows: DataFrame) -> DataFrame:
    """Keep rows that parsed into an event: they have an account, a time and a kind."""
    return rows.where(F.col("did").isNotNull() & F.col("time_us").isNotNull() & F.col("kind").isNotNull())


def _did_of(at_uri: Column) -> Column:
    """The account part of an at:// URI, e.g. did:plc:abc from at://did:plc:abc/app.bsky.feed.post/xyz."""
    return F.nullif(F.regexp_extract(at_uri, r"^at://([^/]+)/", 1), F.lit(""))


def distinct_events(events: DataFrame) -> DataFrame:
    """Keep one row per event key. Copies of an event are identical apart from source_file."""
    return events.dropDuplicates(EVENT_KEY)


def key_condition(target: str, source: str) -> str:
    """SQL condition matching rows with the same event key. Null-safe, since commits have no seq and vice versa."""
    return " AND ".join(f"{target}.{c} <=> {source}.{c}" for c in EVENT_KEY)
