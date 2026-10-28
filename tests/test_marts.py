"""The dbt marts, built with DuckDB on a small hand-made store, checked against numbers worked out by hand."""

import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from sample_events import account, at_us, follow, identity, like, post, profile, tid, write_hour_file

from bluesky_stream_analytics import projections
from bluesky_stream_analytics.event_store import COLUMNS, stage_batch
from bluesky_stream_analytics.handles import HANDLE_COLUMNS

PROJECT = Path(__file__).parents[1] / "transform"
POST = "app.bsky.feed.post"


def moment(day: int, hour: int = 12) -> datetime:
    return datetime(2026, 10, day, hour, tzinfo=UTC)


def at(day: int, hour: int = 12) -> int:
    return at_us(moment(day, hour))


def uri(did: str, rkey: str) -> str:
    return f"at://{did}/{POST}/{rkey}"


# Post keys are TIDs, so a post's age can be read from its key.
B_POST = tid(moment(24, 9))
C_POST = tid(moment(24, 10))
A_POST = tid(moment(25, 11))
A_REPLY = tid(moment(26, 9))
C_REPLY = tid(moment(25, 15))
# A post c replies to that was made before collection began, so it's never seen.
UNSEEN = tid(datetime(2026, 10, 1, 12, tzinfo=UTC))


# Four accounts over three days:
#   a  signs up on the 25th (profile first), likes b's post, is back on the 26th, changes handle, deactivates
#   b  was around before collection began; posts on the 24th, likes its own post on the 25th
#   c  wrote on the 24th, creates a profile on the 25th: not new; replies to a post from before collection
#   d  signs up on the 26th and follows b
EVENTS = [
    post("did:b", B_POST, at(24, 9), rev="3b01"),
    post("did:c", C_POST, at(24, 10), langs=("ja",), rev="3c01"),
    identity("did:a", 10, at(25, 10), handle=None),
    profile("did:a", at(25, 10), rev="3a01"),
    post("did:a", A_POST, at(25, 11), rev="3a02"),
    like("did:a", "3a2", at(25, 12), post_author="did:b", post_rkey=B_POST, rev="3a03"),
    like("did:b", "3b2", at(25, 13), post_author="did:b", post_rkey=B_POST, rev="3b02"),
    profile("did:c", at(25, 14), rev="3c02"),
    post("did:c", C_REPLY, at(25, 15), reply_to=uri("did:x", UNSEEN), rev="3c03"),
    identity("did:a", 20, at(26, 8), handle=None),
    post("did:a", A_REPLY, at(26, 9), reply_to=uri("did:b", B_POST), rev="3a04"),
    account("did:a", 21, at(26, 12), active=False, status="deactivated"),
    profile("did:d", at(26, 15), rev="3d01"),
    follow("did:d", "3d2", at(26, 16), subject="did:b", rev="3d02"),
]


# Jetstream's identity events carry no handle, so the handle job looked them up.
HANDLES = [
    ("did:a", at(25, 10), 10, "a1.bsky.social", True),
    ("did:a", at(26, 8), 20, "a2.bsky.social", True),
]


def write_store(path: Path, tmp: Path, lines: list[str]) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA IF NOT EXISTS bluesky_store")
    con.execute(f"CREATE TABLE IF NOT EXISTS bluesky_store.events ({COLUMNS})")
    if not con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'account_handles'").fetchone()[0]:
        con.execute(f"CREATE TABLE bluesky_store.account_handles ({HANDLE_COLUMNS})")
        for did, time_us, seq, handle, found in HANDLES:
            con.execute(
                "INSERT INTO bluesky_store.account_handles VALUES (?, make_timestamptz(?), ?, now(), ?, ?)",
                [did, time_us, seq, handle, found],
            )
    batch_number = len(list(tmp.rglob("*.zst"))) + 1
    raw = write_hour_file(tmp, 10, f"f{batch_number}", lines)
    stage_batch(con, [str(raw)])
    con.execute("INSERT INTO bluesky_store.events SELECT * FROM batch")
    # The projections are folded from the same events, as on the server.
    parquet = tmp / f"store-{batch_number}.parquet"
    con.execute(f"COPY batch TO '{parquet}' (FORMAT parquet)")
    projections.create_tables(con, "bluesky_store")
    projections.fold_files(con, "bluesky_store", [str(parquet)], batch_number - 1, batch_number)
    con.execute((Path(__file__).parents[1] / "sql" / "views" / "current_records.sql").read_text())
    con.close()


def build(path: Path, tmp: Path, end_date: str, full_refresh: bool = False) -> None:
    """Run `dbt build` in its own process, as the server does, so it lets go of the database file after."""
    args = [
        str(Path(sys.executable).with_name("dbt")),
        "build",
        "--project-dir", str(PROJECT),
        "--profiles-dir", str(PROJECT),
        "--target", "ci",
        "--target-path", str(tmp / "target"),
        "--log-path", str(tmp / "logs"),
        "--vars", json.dumps({"collection_start": "2026-10-24", "end_date": end_date}),
    ]
    if full_refresh:
        args.append("--full-refresh")
    env = {**os.environ, "DBT_DUCKDB_PATH": str(path), "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    result = subprocess.run(args, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout[-4000:]


def query(path: Path, sql: str):
    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def build_sample(tmp: Path) -> Path:
    path = tmp / "ci.duckdb"
    write_store(path, tmp / "raw", EVENTS)
    build(path, tmp, end_date="2026-10-27", full_refresh=True)
    return path


@pytest.fixture(scope="module")
def marts(tmp_path_factory):
    """Built once and only read by the tests that use it."""
    return build_sample(tmp_path_factory.mktemp("marts"))


def test_daily_metrics(marts):
    rows = query(
        marts,
        """
        SELECT CAST(date_day AS VARCHAR), daily_active, weekly_active, monthly_active, new_accounts,
               posts, replies, likes, follows, creators_engaged, top_1pct_share, gini
        FROM main.mart_daily_metrics ORDER BY date_day
        """,
    )
    assert rows == [
        ("2026-10-24", 2, 2, 2, 0, 2, 0, 0, 0, 0, None, None),
        ("2026-10-25", 3, 3, 3, 1, 1, 1, 2, 0, 2, 0.5, 0.0),
        ("2026-10-26", 2, 4, 4, 1, 0, 1, 0, 1, 1, 1.0, 0.0),
    ]


def test_engagement_received_leaves_out_self_engagement(marts):
    rows = query(
        marts,
        """
        SELECT CAST(activity_date AS VARCHAR), likes_received, replies_received, follows_received, engagement_received
        FROM main.fct_engagement_received_daily
        WHERE account_key = sha256('did:b') ORDER BY activity_date
        """,
    )
    assert rows == [("2026-10-25", 1, 0, 0, 1), ("2026-10-26", 0, 1, 1, 1)]


def test_only_real_signups_are_new_accounts(marts):
    rows = query(marts, "SELECT account_key, CAST(signup_date AS VARCHAR) FROM main.int_account_firsts ORDER BY 2, 1")
    signups = {key: day for key, day in rows if day}
    keys = dict(query(marts, "SELECT sha256(d), d FROM (VALUES ('did:a'), ('did:b'), ('did:c'), ('did:d')) t(d)"))
    assert {keys[k]: day for k, day in signups.items()} == {"did:a": "2026-10-25", "did:d": "2026-10-26"}


def test_retention_counts_only_days_that_have_happened(marts):
    rows = query(marts, "SELECT CAST(cohort_date AS VARCHAR), day_n, cohort_size, retained, retention_rate FROM main.mart_retention ORDER BY 1, 2")
    assert rows == [("2026-10-25", 1, 1, 1, 1.0)]


def test_account_history_has_a_version_per_change(marts):
    rows = query(
        marts,
        """
        SELECT version, handle, status, is_current FROM main.dim_account
        WHERE account_key = sha256('did:a') ORDER BY version
        """,
    )
    assert rows == [
        (1, "a1.bsky.social", "unknown", False),
        (2, "a2.bsky.social", "unknown", False),
        (3, "a2.bsky.social", "deactivated", True),
    ]
    assert query(marts, "SELECT count(*), count_if(handle IS NULL) FROM main.dim_account") == [(6, 3)]


def test_an_incremental_build_redoes_recent_days_without_duplicating_them(tmp_path):
    marts = build_sample(tmp_path)
    write_store(marts, tmp_path / "raw", [post("did:b", "3b9", at(27, 9), rev="3b09"), like("did:c", "3c9", at(26, 20), post_author="did:a", post_rkey="3a1", rev="3c09")])

    build(marts, tmp_path, end_date="2026-10-28")

    rows = query(marts, "SELECT CAST(date_day AS VARCHAR), daily_active, likes FROM main.mart_daily_metrics ORDER BY date_day")
    # The 26th is redone with the late like, the 27th is added, and the 24th and 25th are untouched.
    assert rows == [("2026-10-24", 2, 0), ("2026-10-25", 3, 2), ("2026-10-26", 3, 1), ("2026-10-27", 1, 0)]
    assert query(marts, "SELECT count(*), count(DISTINCT (activity_date, account_key)) FROM main.fct_account_activity_daily") == [(9, 9)]


def placeholder_key(marts) -> str:
    return query(marts, f"SELECT sha256('{uri('did:x', UNSEEN)}')")[0][0]


def test_every_reply_parent_has_a_post_row_real_or_placeholder(marts):
    rows = query(marts, "SELECT is_placeholder, count(*) FROM main.dim_post GROUP BY 1 ORDER BY 1")
    assert rows == [(False, 5), (True, 1)]
    placeholder = query(
        marts,
        "SELECT post_key, author_key = sha256('did:x'), epoch_us(created_at), is_reply FROM main.dim_post WHERE is_placeholder",
    )
    # A placeholder still knows its author, and its creation time from its key.
    assert placeholder == [(placeholder_key(marts), True, at_us(datetime(2026, 10, 1, 12, tzinfo=UTC)), None)]


def test_post_engagement_by_day_and_age(marts):
    engagement = query(
        marts,
        """
        SELECT CAST(activity_date AS VARCHAR), likes, reposts, replies
        FROM main.fct_post_engagement_daily ORDER BY activity_date, likes DESC
        """,
    )
    assert engagement == [("2026-10-25", 1, 0, 0), ("2026-10-25", 0, 0, 1), ("2026-10-26", 0, 0, 1)]
    by_age = query(
        marts,
        "SELECT CAST(activity_date AS VARCHAR), age_hours, likes, replies FROM main.mart_engagement_by_post_age ORDER BY 1, 2",
    )
    # The like came 27 hours after b's post; the replies reached posts 48 hours and 24 days old.
    assert by_age == [("2026-10-25", 27, 1, 0), ("2026-10-25", 48, 0, 1), ("2026-10-26", 48, 0, 1)]


def test_post_metrics_and_languages(marts):
    metrics = query(
        marts,
        """
        SELECT CAST(date_day AS VARCHAR), posts, replies, replies_to_unseen_posts, posts_engaged, engagement, share_to_posts_under_a_day
        FROM main.mart_post_metrics_daily ORDER BY date_day
        """,
    )
    assert metrics == [
        ("2026-10-24", 2, 0, 0, 0, 0, None),
        ("2026-10-25", 1, 1, 1, 2, 2, 0.0),
        ("2026-10-26", 0, 1, 0, 1, 1, 0.0),
    ]
    languages = query(marts, "SELECT CAST(date_day AS VARCHAR), language, posts FROM main.mart_posts_by_language_daily ORDER BY 1, 2")
    assert languages == [("2026-10-24", "en", 1), ("2026-10-24", "ja", 1), ("2026-10-25", "en", 2), ("2026-10-26", "en", 1)]


def test_a_placeholder_gives_way_to_the_real_post_when_it_arrives(tmp_path):
    marts = build_sample(tmp_path)
    write_store(marts, tmp_path / "raw", [post("did:x", UNSEEN, at(27, 9), rev="3x01")])

    build(marts, tmp_path, end_date="2026-10-28")

    rows = query(marts, f"SELECT is_placeholder, epoch_us(created_at) FROM main.dim_post WHERE post_key = sha256('{uri('did:x', UNSEEN)}')")
    assert rows == [(False, at_us(datetime(2026, 10, 1, 12, tzinfo=UTC)))]
    assert query(marts, "SELECT count(*), count_if(is_placeholder) FROM main.dim_post") == [(6, 0)]
