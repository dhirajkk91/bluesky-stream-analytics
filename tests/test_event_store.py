import duckdb
import pytest

from sample_events import account, delete, follow, identity, like, post, repost, time_us, write_hour_file

from bluesky_stream_analytics.event_store import COLUMNS, EVENT_KEY, append_new, key_match, stage_batch

COLUMN_NAMES = [line.split()[0] for line in COLUMNS.strip().splitlines()]


@pytest.fixture
def con():
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"CREATE TABLE events ({COLUMNS})")
    yield con
    con.close()


def stage(con, tmp_path, lines, hour=10, name="a"):
    path = write_hour_file(tmp_path, hour, name, lines)
    return stage_batch(con, [str(path)])


def rows(con):
    """Rows of the staged batch as dicts, timestamps formatted in UTC."""
    cur = con.execute("SELECT * EXCLUDE (event_time), strftime(event_time, '%Y-%m-%d %H:%M:%S.%f') AS t FROM batch ORDER BY event_time")
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


def one(con, tmp_path, line):
    stage(con, tmp_path, [line])
    [row] = rows(con)
    return row


def test_columns_match_the_table(con, tmp_path):
    stage(con, tmp_path, [like("did:plc:a", "3l", time_us(10))])
    staged = [d[0] for d in con.execute("SELECT * FROM batch LIMIT 0").description]
    assert staged == COLUMN_NAMES


def test_like_points_at_the_post_and_its_author(con, tmp_path):
    row = one(con, tmp_path, like("did:plc:fan", "3like", time_us(10), post_author="did:plc:writer", post_rkey="3abc"))
    assert (row["collection"], row["operation"]) == ("app.bsky.feed.like", "create")
    assert (row["subject_did"], row["subject_collection"], row["subject_rkey"]) == (
        "did:plc:writer",
        "app.bsky.feed.post",
        "3abc",
    )


def test_repost_points_at_the_post_and_its_author(con, tmp_path):
    row = one(con, tmp_path, repost("did:plc:fan", "3rp", time_us(10), post_author="did:plc:writer", post_rkey="3xyz"))
    assert (row["subject_did"], row["subject_rkey"]) == ("did:plc:writer", "3xyz")


def test_follow_subject_is_the_followed_account(con, tmp_path):
    row = one(con, tmp_path, follow("did:plc:a", "3f", time_us(10), subject="did:plc:b"))
    assert row["subject_did"] == "did:plc:b"
    assert row["subject_collection"] is None and row["subject_rkey"] is None


def test_post_keeps_languages_and_reply_but_not_text(con, tmp_path):
    parent = "at://did:plc:op/app.bsky.feed.post/3parent"
    row = one(con, tmp_path, post("did:plc:a", "3p", time_us(10), text="secret words", langs=["en", "ja"], reply_to=parent))
    assert row["langs"] == ["en", "ja"]
    assert row["reply_parent_uri"] == parent and row["reply_root_uri"] == parent
    assert row["subject_did"] is None
    assert "secret words" not in str(row)


def test_delete_has_no_subject(con, tmp_path):
    row = one(con, tmp_path, delete("did:plc:a", "app.bsky.feed.like", "3like", time_us(10)))
    assert (row["operation"], row["rkey"], row["subject_did"]) == ("delete", "3like", None)


def test_identity_and_account_events(con, tmp_path):
    stage(con, tmp_path, [identity("did:plc:a", 101, time_us(10), handle="new.bsky.social"),
                          account("did:plc:a", 102, time_us(10, 1), active=False, status="deactivated")])
    ident, acct = rows(con)
    assert (ident["kind"], ident["seq"], ident["handle"], ident["collection"]) == ("identity", 101, "new.bsky.social", None)
    assert (acct["kind"], acct["seq"], acct["account_active"], acct["account_status"]) == ("account", 102, False, "deactivated")


def test_event_time_is_utc_to_the_microsecond_and_source_file_is_set(con, tmp_path):
    row = one(con, tmp_path, like("did:plc:a", "3l", time_us(10, 30, 15, 250)))
    assert row["t"] == "2026-10-24 10:30:15.000250"
    assert row["source_file"] == "events/date=2026-10-24/hour=10/a.jsonl.zst"


def test_unreadable_lines_are_not_counted(con, tmp_path):
    readable, distinct = stage(con, tmp_path, ["not json", '{"time_us": 1, "kind": "commit"}', like("did:plc:a", "3l", time_us(10))])
    assert (readable, distinct) == (1, 1)


def test_only_true_repeats_are_dropped(con, tmp_path):
    ts = time_us(10)
    readable, distinct = stage(con, tmp_path, [
        like("did:plc:a", "3l", ts),
        like("did:plc:a", "3l", ts),  # replayed after a reconnect
        like("did:plc:a", "3l2", ts),  # same account and time, different record
        delete("did:plc:a", "app.bsky.feed.like", "3l", ts + 1),  # same record, later revision
        identity("did:plc:a", 1, ts),
        identity("did:plc:a", 1, ts),
        identity("did:plc:a", 2, ts),
    ])
    assert (readable, distinct) == (7, 5)


def test_append_adds_only_new_events(con, tmp_path):
    stage(con, tmp_path, [like(f"did:plc:{i}", "3l", time_us(10, i)) for i in range(5)])
    assert append_new(con, "events") == 5

    stage(con, tmp_path, [like(f"did:plc:{i}", "3l", time_us(10, i)) for i in range(7)], name="b")
    assert append_new(con, "events") == 2
    assert con.execute("SELECT count(*) FROM events").fetchone()[0] == 7


def test_append_of_an_empty_batch_adds_nothing(con, tmp_path):
    stage(con, tmp_path, ["not json"])
    assert append_new(con, "events") == 0


def test_key_match_is_null_safe_on_every_key_column():
    condition = key_match("b", "t")
    for column in EVENT_KEY:
        assert f"b.{column} IS NOT DISTINCT FROM t.{column}" in condition
