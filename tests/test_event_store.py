import pytest

from sample_events import account, delete, follow, identity, like, post, repost, time_us

from bluesky_stream_analytics.event_store import distinct_events, key_condition, parse_events


def parse(spark, lines, source="events/date=2026-10-24/hour=10/a.jsonl.zst"):
    df = spark.createDataFrame([(line, source) for line in lines], "value string, source_file string")
    return parse_events(df)


def one(spark, line):
    rows = parse(spark, [line]).collect()
    assert len(rows) == 1
    return rows[0].asDict()


def test_like_points_at_the_post_and_its_author(spark):
    row = one(spark, like("did:plc:fan", "3like", time_us(10), post_author="did:plc:writer", post_rkey="3abc"))
    assert row["collection"] == "app.bsky.feed.like"
    assert row["operation"] == "create"
    assert row["subject_uri"] == "at://did:plc:writer/app.bsky.feed.post/3abc"
    assert row["subject_did"] == "did:plc:writer"
    assert row["cid"] == "bafyrei3like"


def test_repost_points_at_the_post_and_its_author(spark):
    row = one(spark, repost("did:plc:fan", "3rp", time_us(10), post_author="did:plc:writer"))
    assert row["subject_did"] == "did:plc:writer"
    assert row["subject_uri"].startswith("at://did:plc:writer/")


def test_follow_subject_is_the_followed_account(spark):
    row = one(spark, follow("did:plc:a", "3f", time_us(10), subject="did:plc:b"))
    assert row["subject_did"] == "did:plc:b"
    assert row["subject_uri"] is None


def test_post_keeps_languages_and_reply_but_not_text(spark):
    parent = "at://did:plc:op/app.bsky.feed.post/3parent"
    row = one(spark, post("did:plc:a", "3p", time_us(10), text="secret words", langs=["en", "ja"], reply_to=parent))
    assert row["langs"] == ["en", "ja"]
    assert row["reply_parent_uri"] == parent
    assert row["reply_root_uri"] == parent
    assert row["subject_did"] is None
    assert "secret words" not in str(row)


def test_delete_has_no_record_fields(spark):
    row = one(spark, delete("did:plc:a", "app.bsky.feed.like", "3like", time_us(10)))
    assert row["operation"] == "delete"
    assert row["rkey"] == "3like"
    assert row["cid"] is None and row["subject_did"] is None


def test_identity_and_account_events(spark):
    ident = one(spark, identity("did:plc:a", 101, time_us(10), handle="new.bsky.social"))
    assert (ident["kind"], ident["seq"], ident["handle"], ident["collection"]) == ("identity", 101, "new.bsky.social", None)

    acct = one(spark, account("did:plc:a", 102, time_us(10), active=False, status="deactivated"))
    assert (acct["kind"], acct["seq"], acct["account_active"], acct["account_status"]) == ("account", 102, False, "deactivated")


def test_event_time_is_utc_from_time_us(spark):
    # Formatted inside Spark, which runs in UTC; collecting a timestamp converts it to the machine's local zone.
    df = parse(spark, [like("did:plc:a", "3l", time_us(10, 30, 15, 250))])
    row = df.selectExpr("date_format(event_time, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS t", "source_file").first()
    assert row.t == "2026-10-24 10:30:15.000250"
    assert row.source_file == "events/date=2026-10-24/hour=10/a.jsonl.zst"


def test_unreadable_lines_are_dropped(spark):
    lines = ["not json", '{"time_us": 1, "kind": "commit"}', "", like("did:plc:a", "3l", time_us(10))]
    assert parse(spark, lines).count() == 1


def test_distinct_events_drops_repeats_only(spark):
    ts = time_us(10)
    lines = [
        like("did:plc:a", "3l", ts),
        like("did:plc:a", "3l", ts),  # replayed after a reconnect
        like("did:plc:a", "3l2", ts),  # same account and time, different record
        delete("did:plc:a", "app.bsky.feed.like", "3l", ts + 1),  # same record, later revision
        identity("did:plc:a", 1, ts),
        identity("did:plc:a", 1, ts),
        identity("did:plc:a", 2, ts),
    ]
    assert distinct_events(parse(spark, lines)).count() == 5


@pytest.mark.parametrize("column", ["did", "kind", "collection", "rkey", "rev", "operation", "seq"])
def test_key_condition_is_null_safe_on_every_key_column(column):
    assert f"t.{column} <=> s.{column}" in key_condition("t", "s")
