"""The dbt marts, built with DuckDB on a small hand-made store, checked against numbers worked out by hand."""

import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from sample_events import account, at_us, follow, identity, like, post, profile, write_hour_file

from bluesky_stream_analytics.event_store import COLUMNS, stage_batch

PROJECT = Path(__file__).parents[1] / "transform"
POST = "app.bsky.feed.post"


def at(day: int, hour: int = 12) -> int:
    return at_us(datetime(2026, 10, day, hour, tzinfo=UTC))


# Four accounts over three days:
#   a  signs up on the 25th (profile first), likes b's post, is back on the 26th, changes handle, deactivates
#   b  was around before collection began; posts on the 24th, likes its own post on the 25th
#   c  wrote on the 24th, creates a profile on the 25th: not new
#   d  signs up on the 26th and follows b
EVENTS = [
    post("did:b", "3b1", at(24, 9), rev="3b01"),
    post("did:c", "3c1", at(24, 10), rev="3c01"),
    identity("did:a", 10, at(25, 10), handle="a1.bsky.social"),
    profile("did:a", at(25, 10), rev="3a01"),
    post("did:a", "3a1", at(25, 11), rev="3a02"),
    like("did:a", "3a2", at(25, 12), post_author="did:b", post_rkey="3b1", rev="3a03"),
    like("did:b", "3b2", at(25, 13), post_author="did:b", post_rkey="3b1", rev="3b02"),
    profile("did:c", at(25, 14), rev="3c02"),
    identity("did:a", 20, at(26, 8), handle="a2.bsky.social"),
    post("did:a", "3a3", at(26, 9), reply_to="at://did:b/app.bsky.feed.post/3b1", rev="3a04"),
    account("did:a", 21, at(26, 12), active=False, status="deactivated"),
    profile("did:d", at(26, 15), rev="3d01"),
    follow("did:d", "3d2", at(26, 16), subject="did:b", rev="3d02"),
]


def write_store(path: Path, tmp: Path, lines: list[str]) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA IF NOT EXISTS bluesky_store")
    con.execute(f"CREATE TABLE IF NOT EXISTS bluesky_store.events ({COLUMNS})")
    raw = write_hour_file(tmp, 10, f"f{len(list(tmp.rglob('*.zst')))}", lines)
    stage_batch(con, [str(raw)])
    con.execute("INSERT INTO bluesky_store.events SELECT * FROM batch")
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
        ("2026-10-25", 3, 3, 3, 1, 1, 0, 2, 0, 1, 1.0, 0.0),
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
