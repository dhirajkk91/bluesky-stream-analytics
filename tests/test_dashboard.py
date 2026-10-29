import json
from datetime import UTC, datetime

import duckdb
import pytest

from bluesky_stream_analytics import dashboard
from test_marts import build_sample


@pytest.fixture(scope="module")
def marts(tmp_path_factory):
    return build_sample(tmp_path_factory.mktemp("dashboard"))


def duckdb_query(path):
    def run(sql: str) -> list[dict]:
        con = duckdb.connect(str(path), read_only=True)
        try:
            cursor = con.execute(sql)
            names = [c[0] for c in cursor.description]
            return [dict(zip(names, row)) for row in cursor.fetchall()]
        finally:
            con.close()

    return run


def test_the_export_from_the_sample_marts(marts):
    data = dashboard.build(duckdb_query(marts), "main", "bluesky_store", "2026-10-24", now=datetime(2026, 10, 27, 5, tzinfo=UTC))
    json.dumps(data)  # it has to be plain JSON

    assert data["generated_at"] == "2026-10-27T05:00:00+00:00"
    assert [(d["date"], d["partial"], d["daily_active"], d["likes"]) for d in data["days"]] == [
        ("2026-10-24", True, 2, 0),
        ("2026-10-25", False, 3, 2),
        ("2026-10-26", False, 2, 0),
    ]
    day = data["days"][1]
    assert (day["top_1pct_share"], day["posts_engaged"], day["replies_to_unseen_posts"]) == (0.5, 2, 1)

    # The whole days are the 25th and 26th; the 24th began partway through.
    assert data["recent"] == {"start": "2026-10-25", "end": "2026-10-26", "days": 2}
    assert data["post_age"] == [{"age_hours": 27, "engagement": 1}, {"age_hours": 48, "engagement": 2}]
    # One cohort of one account, and three English posts: both too small to show.
    assert data["retention"] == []
    assert data["languages"] == [{"language": "other", "posts": 3}]
    # The sample has no deletes, so no projection run saw one.
    assert data["deletes"] is None


def test_small_cohorts_are_left_out():
    rows = [
        {"cohort_date": "2026-10-25", "day_n": 1, "cohort_size": 19_196, "retained": 4_127},
        {"cohort_date": "2026-10-26", "day_n": 1, "cohort_size": 99, "retained": 20},
    ]
    assert dashboard.retention(rows) == [{**rows[0], "rate": 4_127 / 19_196}]


def test_small_languages_fold_into_other():
    rows = [
        {"language": "en", "posts": 14_000_000},
        {"language": "ja", "posts": 2_500_000},
        {"language": "is", "posts": 650},
        {"language": "fo", "posts": 40},
    ]
    assert dashboard.languages(rows, days=7) == [
        {"language": "en", "posts": 14_000_000},
        {"language": "ja", "posts": 2_500_000},
        {"language": "other", "posts": 690},
    ]


def test_how_long_deletes_stayed_visible():
    rows = [{"visible_seconds": s} for s in (3_600, 5_400, 8_460, None)]
    assert dashboard.delete_visibility(rows) == {"runs": 3, "average_minutes": 97, "longest_minutes": 141}
    assert dashboard.delete_visibility([]) is None


class FakeAthena:
    """Answers one query with two pages of results, the way Athena does."""

    def __init__(self, state="SUCCEEDED"):
        self.state = state

    def start_query_execution(self, QueryString, WorkGroup):
        return {"QueryExecutionId": "q1"}

    def get_query_execution(self, QueryExecutionId):
        return {"QueryExecution": {"Status": {"State": self.state, "StateChangeReason": "SYNTAX_ERROR"}}}

    def get_paginator(self, name):
        columns = [{"Name": "date", "Type": "varchar"}, {"Name": "n", "Type": "bigint"},
                   {"Name": "share", "Type": "double"}, {"Name": "done", "Type": "boolean"}]
        meta = {"ColumnInfo": columns}

        def row(*values):
            return {"Data": [{} if v is None else {"VarCharValue": v} for v in values]}

        pages = [
            {"ResultSet": {"ResultSetMetadata": meta, "Rows": [row("date", "n", "share", "done"), row("2026-10-25", "3", "0.5", "true")]}},
            {"ResultSet": {"ResultSetMetadata": meta, "Rows": [row("2026-10-26", "2", None, "false")]}},
        ]

        class Paginator:
            def paginate(self, QueryExecutionId):
                return pages

        return Paginator()


def test_athena_rows_come_back_typed_across_pages():
    rows = dashboard.Athena(FakeAthena())("SELECT 1")
    assert rows == [
        {"date": "2026-10-25", "n": 3, "share": 0.5, "done": True},
        {"date": "2026-10-26", "n": 2, "share": None, "done": False},
    ]


def test_a_failed_athena_query_raises():
    with pytest.raises(RuntimeError, match="SYNTAX_ERROR"):
        dashboard.Athena(FakeAthena("FAILED"))("SELECT 1")


def test_catch_up_runs_dont_count_for_how_long_deletes_stayed_visible():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA store")
    con.execute("CREATE TABLE store.projection_runs (run_at TIMESTAMPTZ, oldest_delete_time TIMESTAMPTZ)")
    # A rebuild folds two days of backlog back to back, then the hourly runs take over.
    con.execute(
        """
        INSERT INTO store.projection_runs VALUES
            ('2026-10-27 01:34:00+00', '2026-10-24 22:14:00+00'),
            ('2026-10-27 01:35:00+00', '2026-10-25 00:00:00+00'),
            ('2026-10-27 01:48:00+00', '2026-10-27 00:00:00+00'),
            ('2026-10-27 02:20:00+00', '2026-10-27 01:00:00+00'),
            ('2026-10-27 03:20:00+00', '2026-10-27 02:00:00+00'),
            ('2026-10-27 05:20:00+00', '2026-10-27 02:59:00+00'),
            ('2026-10-27 06:20:00+00', NULL)
        """
    )

    def query(sql):
        cursor = con.execute(sql)
        names = [c[0] for c in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]

    sql = dashboard.DELETES_SQL.format(store="store", runs=dashboard.DELETE_RUNS, catch_up=dashboard.CATCH_UP_MINUTES)
    assert dashboard.delete_visibility(query(sql)) == {"runs": 3, "average_minutes": 100, "longest_minutes": 141}
