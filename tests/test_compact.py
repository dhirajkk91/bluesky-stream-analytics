"""Tests of compaction: which tier runs when, what each reaches, and the SQL itself, run against local tables in DuckDB."""

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from sample_events import at_us, delete, edit_post, like, post, tid, write_hour_file

from bluesky_stream_analytics import compact
from bluesky_stream_analytics.compact import (
    INTERACTIONS,
    POSTS,
    Athena,
    Scope,
    Step,
    plan,
    redact_sql,
    remove_superseded_sql,
    run_steps,
    scopes,
    tier_for,
)
from bluesky_stream_analytics.event_store import COLUMNS, stage_batch
from bluesky_stream_analytics.process import connect
from bluesky_stream_analytics.projections import create_tables, fold_files

SCHEMA = "bluesky_store"
VIEWS = Path(__file__).parents[1] / "sql" / "views"
LIKE, POST = "app.bsky.feed.like", "app.bsky.feed.post"
TODAY = date(2026, 10, 27)
NOW = datetime(2026, 10, 26, 12, tzinfo=UTC)
OLD = datetime(2026, 10, 10, 12, tzinfo=UTC)

# Keys, and when each record was created.
DELETED_LIKE = tid(NOW)
KEPT_LIKE = tid(NOW, 1)
OLD_DELETED_LIKE = tid(OLD)
DELETED_POST = tid(NOW, 2)
EDITED_POST = tid(NOW, 3)


def duckdb_timestamp(moment: datetime) -> str:
    return f"TIMESTAMPTZ '{moment:%Y-%m-%d %H:%M:%S}+00'"


def at(moment: datetime, minutes: int = 0) -> int:
    return at_us(moment) + minutes * 60_000_000


@pytest.fixture
def con(tmp_path):
    """Local tables holding a store and its projections: each record created, then some deleted or edited."""
    con = connect(threads=2, memory_limit="1GB", extension_dir=None)
    con.execute(f"CREATE SCHEMA {SCHEMA}")
    con.execute(f"CREATE TABLE {SCHEMA}.events ({COLUMNS})")
    create_tables(con, SCHEMA)
    con.execute((VIEWS / "current_records.sql").read_text())

    lines = [
        like("did:a", DELETED_LIKE, at(NOW), rev="3a1"),
        delete("did:a", LIKE, DELETED_LIKE, at(NOW, 5), rev="3a2"),
        like("did:a", KEPT_LIKE, at(NOW), post_author="did:plc:kept", rev="3a3"),
        like("did:b", OLD_DELETED_LIKE, at(OLD), rev="3b1"),
        delete("did:b", LIKE, OLD_DELETED_LIKE, at(NOW, 1), rev="3b2"),
        post("did:c", DELETED_POST, at(NOW), langs=("en",), reply_to="at://did:x/app.bsky.feed.post/3p", rev="3c1"),
        edit_post("did:c", DELETED_POST, at(NOW, 1), langs=("fr",), rev="3c2"),
        delete("did:c", POST, DELETED_POST, at(NOW, 2), rev="3c3"),
        post("did:d", EDITED_POST, at(NOW), langs=("en",), rev="3d1"),
        edit_post("did:d", EDITED_POST, at(NOW, 1), langs=("de",), rev="3d2"),
    ]
    raw = write_hour_file(tmp_path / "raw", 10, "x", lines)
    stage_batch(con, [str(raw)])
    con.execute(f"INSERT INTO {SCHEMA}.events SELECT * FROM batch")
    con.execute(f"COPY batch TO '{tmp_path}/store.parquet' (FORMAT parquet)")
    fold_files(con, SCHEMA, [str(tmp_path / "store.parquet")], 0, 1)
    yield con
    con.close()


def compact_locally(con, tier: str) -> None:
    for scope in scopes(tier, TODAY):
        con.execute(redact_sql(scope, duckdb_timestamp))
        con.execute(remove_superseded_sql(scope, duckdb_timestamp))


def records(con):
    return con.execute(f"SELECT rkey, rev FROM {SCHEMA}.records ORDER BY rkey, rev").fetchall()


def current(con):
    return con.execute(f"SELECT rkey, rev, langs FROM {SCHEMA}.current_records ORDER BY rkey").fetchall()


def all_events(con):
    return con.execute(
        f"""
        SELECT did, epoch_us(event_time), operation, rkey, rev, subject_did, reply_parent_uri, langs
        FROM {SCHEMA}.events ORDER BY ALL
        """
    ).fetchall()


def event_details(con, rkey):
    return con.execute(
        f"""
        SELECT operation, subject_did, reply_parent_uri, langs FROM {SCHEMA}.events
        WHERE rkey = ? ORDER BY rev
        """,
        [rkey],
    ).fetchall()


@pytest.mark.parametrize(
    ("day", "tier"),
    [(date(2026, 11, 1), "monthly"), (date(2026, 11, 8), "weekly"), (date(2026, 11, 9), "daily"), (date(2026, 11, 7), "daily")],
)
def test_the_first_of_the_month_is_monthly_and_sundays_weekly(day, tier):
    assert tier_for(day) == tier


def test_each_tier_reaches_back_from_midnight():
    midnight = datetime(2026, 10, 27, tzinfo=UTC)
    assert scopes("daily", TODAY) == [Scope(POSTS, datetime(2026, 10, 24, tzinfo=UTC)), Scope(INTERACTIONS, datetime(2026, 10, 24, tzinfo=UTC))]
    assert scopes("weekly", TODAY) == [Scope(POSTS, None), Scope(INTERACTIONS, datetime(2026, 10, 19, tzinfo=UTC))]
    assert scopes("monthly", TODAY) == [Scope(POSTS, None), Scope(INTERACTIONS, None)]
    assert all(s.since is None or (midnight - s.since).seconds == 0 for t in compact.TIERS for s in scopes(t, TODAY))


def test_a_run_redacts_before_removing_and_ends_with_the_markers():
    names = [step.name for step in plan("daily", TODAY)]
    assert names == [
        "redact the event store", "redact the event store",
        "remove superseded rows", "remove superseded rows",
        "optimize records", "optimize records",
        "optimize events", "optimize record_deletes",
    ]
    assert plan("daily", TODAY)[6].sql.endswith("WHERE event_time >= TIMESTAMP '2026-10-23 00:00:00 UTC'")
    assert plan("weekly", TODAY)[6].sql.endswith("USING BIN_PACK")


def test_daily_compaction_removes_deleted_and_old_versions_in_reach(con):
    before = current(con)

    compact_locally(con, "daily")

    assert current(con) == before == sorted([(EDITED_POST, "3d2", ["de"]), (KEPT_LIKE, "3a3", None)])
    # The like created 16 days ago is out of the daily run's reach: still there, still hidden.
    assert records(con) == sorted([(OLD_DELETED_LIKE, "3b1"), (EDITED_POST, "3d2"), (KEPT_LIKE, "3a3")])


def test_daily_compaction_redacts_the_events_of_deleted_records_only(con):
    compact_locally(con, "daily")

    assert event_details(con, DELETED_LIKE) == [("create", None, None, None), ("delete", None, None, None)]
    assert event_details(con, DELETED_POST) == [
        ("create", None, None, None),
        ("update", None, None, None),
        ("delete", None, None, None),
    ]
    assert event_details(con, KEPT_LIKE) == [("create", "did:plc:kept", None, None)]
    assert event_details(con, EDITED_POST) == [("create", None, None, ["en"]), ("update", None, None, ["de"])]
    assert event_details(con, OLD_DELETED_LIKE)[0][1] == "did:plc:author"  # out of reach until the monthly run


def test_the_monthly_run_reaches_everything(con):
    compact_locally(con, "monthly")

    assert records(con) == sorted([(EDITED_POST, "3d2"), (KEPT_LIKE, "3a3")])
    assert event_details(con, OLD_DELETED_LIKE) == [("create", None, None, None), ("delete", None, None, None)]


def test_compacting_again_changes_nothing(con):
    compact_locally(con, "monthly")
    rows = records(con)
    events = all_events(con)

    compact_locally(con, "monthly")

    assert records(con) == rows
    assert all_events(con) == events


def test_the_event_store_keeps_every_event_and_only_loses_details(con):
    columns = "did, kind, collection, operation, rkey, rev, seq, epoch_us(event_time), source_file"
    before = con.execute(f"SELECT {columns} FROM {SCHEMA}.events ORDER BY ALL").fetchall()

    compact_locally(con, "monthly")

    assert con.execute(f"SELECT {columns} FROM {SCHEMA}.events ORDER BY ALL").fetchall() == before


class FakeAthena:
    def __init__(self, fail_on: str | None = None):
        self.fail_on = fail_on
        self.statements: list[str] = []

    def start_query_execution(self, QueryString, WorkGroup, QueryExecutionContext):
        self.statements.append(QueryString)
        return {"QueryExecutionId": str(len(self.statements))}

    def get_query_execution(self, QueryExecutionId):
        sql = self.statements[int(QueryExecutionId) - 1]
        failed = self.fail_on is not None and self.fail_on in sql
        status = {"State": "FAILED", "StateChangeReason": "boom"} if failed else {"State": "SUCCEEDED"}
        return {"QueryExecution": {"Status": status, "Statistics": {"DataScannedInBytes": 1_000_000}}}


def test_every_step_is_logged_in_one_statement():
    client = FakeAthena()
    steps = [Step("one", "a", "SELECT 1"), Step("two", "b", "SELECT 2")]

    run_steps(Athena(client, poll_seconds=0), "daily", steps, datetime(2026, 10, 27, 3, 45, tzinfo=UTC))

    assert client.statements[:2] == ["SELECT 1", "SELECT 2"]
    [insert] = client.statements[2:]
    assert "INSERT INTO bluesky_store.compaction_runs" in insert
    assert "(TIMESTAMP '2026-10-27 03:45:00', 'daily', 'one', 'a', 0.0, 1000000, true)" in insert
    assert "'two', 'b'" in insert


def test_a_failed_step_stops_the_run_and_is_logged():
    client = FakeAthena(fail_on="SELECT 2")
    steps = [Step("one", "a", "SELECT 1"), Step("two", "b", "SELECT 2"), Step("three", "c", "SELECT 3")]

    with pytest.raises(RuntimeError, match="two \\(b\\) failed: boom"):
        run_steps(Athena(client, poll_seconds=0), "daily", steps, datetime(2026, 10, 27, tzinfo=UTC))

    assert "SELECT 3" not in client.statements
    assert "'two', 'b', 0.0, 0, false" in client.statements[-1]


def test_redacted_events_folded_again_are_hidden_and_removed_next_time(con, tmp_path):
    # The projections skip compaction's own commits, but if one were folded again, nothing would change.
    compact_locally(con, "daily")
    con.execute(
        f"COPY (SELECT * FROM {SCHEMA}.events WHERE rkey = ?) TO '{tmp_path}/redacted.parquet' (FORMAT parquet)",
        [DELETED_LIKE],
    )
    fold_files(con, SCHEMA, [str(tmp_path / "redacted.parquet")], 1, 2)
    assert (DELETED_LIKE, "3a1") in records(con)
    before = current(con)

    compact_locally(con, "daily")

    assert current(con) == before
    assert (DELETED_LIKE, "3a1") not in records(con)
