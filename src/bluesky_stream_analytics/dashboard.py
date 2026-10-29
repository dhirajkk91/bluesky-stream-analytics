"""
Export what the public dashboard shows, as one small JSON file.

Runs after the nightly marts build. It reads totals from the marts (daily
metrics, retention, engagement by post age, languages) and from the projection
runs (how long deletes stayed visible), and writes them to
s3://<bucket>/dashboard/dashboard.json. A GitHub Actions workflow copies that
file into the static page on GitHub Pages.

Only totals leave: nothing about an account or a post. Groups smaller than
MIN_GROUP (a cohort, a language) are left out or folded into "other".
"""

import argparse
import json
import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import boto3

from bluesky_stream_analytics.compact import WORKGROUP

log = logging.getLogger(__name__)

KEY = "dashboard/dashboard.json"
MIN_GROUP = 100
RECENT_DAYS = 7  # days summed for the post age curve and the languages
DELETE_RUNS = 7 * 24  # hourly projection runs looked at for how long deletes stay visible

Query = Callable[[str], list[dict]]

DAILY_SQL = """
SELECT
    CAST(m.date_day AS VARCHAR) AS date,
    m.daily_active, m.weekly_active, m.monthly_active, m.weekly_complete, m.monthly_complete,
    m.new_accounts, m.posts, m.replies, m.likes, m.reposts, m.follows, m.blocks, m.deletes,
    m.top_1pct_share, m.gini,
    p.posts_engaged, p.engagement AS post_engagement, p.replies_to_unseen_posts, p.share_to_posts_under_a_day
FROM {marts}.mart_daily_metrics m
LEFT JOIN {marts}.mart_post_metrics_daily p ON p.date_day = m.date_day
ORDER BY m.date_day
"""

RETENTION_SQL = """
SELECT CAST(cohort_date AS VARCHAR) AS cohort_date, day_n, cohort_size, retained
FROM {marts}.mart_retention
ORDER BY cohort_date, day_n
"""

POST_AGE_SQL = """
SELECT age_hours, sum(engagement) AS engagement
FROM {marts}.mart_engagement_by_post_age
WHERE activity_date >= DATE '{start}' AND activity_date <= DATE '{end}'
GROUP BY age_hours
ORDER BY age_hours
"""

LANGUAGES_SQL = """
SELECT language, sum(posts) AS posts
FROM {marts}.mart_posts_by_language_daily
WHERE date_day >= DATE '{start}' AND date_day <= DATE '{end}'
GROUP BY language
ORDER BY posts DESC
"""

DELETES_SQL = """
SELECT date_diff('second', oldest_delete_time, run_at) AS visible_seconds
FROM {store}.projection_runs
WHERE oldest_delete_time IS NOT NULL
ORDER BY run_at DESC
LIMIT {runs}
"""


def complete_days(days: list[dict], collection_start: str) -> list[dict]:
    """The days the dashboard counts as whole: not the first, which began partway through."""
    return [d for d in days if d["date"] > collection_start]


def retention(rows: list[dict]) -> list[dict]:
    """Retention by cohort, leaving out cohorts too small to show."""
    return [
        {**row, "rate": row["retained"] / row["cohort_size"]}
        for row in rows
        if row["cohort_size"] >= MIN_GROUP
    ]


def languages(rows: list[dict], days: int) -> list[dict]:
    """Posts by language, with languages under MIN_GROUP posts a day folded into "other"."""
    kept = [r for r in rows if r["language"] != "other" and r["posts"] >= MIN_GROUP * days]
    other = sum(r["posts"] for r in rows) - sum(r["posts"] for r in kept)
    result = [{"language": r["language"], "posts": r["posts"]} for r in kept]
    if other:
        result.append({"language": "other", "posts": other})
    return result


def delete_visibility(rows: list[dict]) -> dict | None:
    seconds = [r["visible_seconds"] for r in rows if r["visible_seconds"] is not None]
    if not seconds:
        return None
    return {
        "runs": len(seconds),
        "average_minutes": round(sum(seconds) / len(seconds) / 60),
        "longest_minutes": round(max(seconds) / 60),
    }


def build(query: Query, marts: str, store: str, collection_start: str, now: datetime | None = None) -> dict:
    """Everything the page shows, from the marts."""
    now = now or datetime.now(UTC)
    days = query(DAILY_SQL.format(marts=marts))
    whole = complete_days(days, collection_start)
    recent = whole[-RECENT_DAYS:]
    window = {"start": recent[0]["date"], "end": recent[-1]["date"], "days": len(recent)} if recent else None
    post_age, langs = [], []
    if window:
        post_age = query(POST_AGE_SQL.format(marts=marts, **window))
        langs = languages(query(LANGUAGES_SQL.format(marts=marts, **window)), window["days"])
    return {
        "generated_at": now.isoformat(timespec="seconds"),
        "collection_start": collection_start,
        "days": [{**d, "partial": d["date"] <= collection_start} for d in days],
        "retention": retention(query(RETENTION_SQL.format(marts=marts))),
        "recent": window,
        "post_age": post_age,
        "languages": langs,
        "deletes": delete_visibility(query(DELETES_SQL.format(store=store, runs=DELETE_RUNS))),
        "min_group": MIN_GROUP,
    }


class Athena:
    """Runs a query in the workgroup, waits, and returns its rows with their types."""

    CASTS = {
        "boolean": lambda v: v == "true",
        "tinyint": int,
        "smallint": int,
        "integer": int,
        "bigint": int,
        "double": float,
        "real": float,
        "float": float,
    }

    def __init__(self, client, workgroup: str = WORKGROUP, poll_seconds: float = 1) -> None:
        self.client = client
        self.workgroup = workgroup
        self.poll_seconds = poll_seconds

    def __call__(self, sql: str) -> list[dict]:
        query_id = self.client.start_query_execution(QueryString=sql, WorkGroup=self.workgroup)["QueryExecutionId"]
        while True:
            status = self.client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
            if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(self.poll_seconds)
        if status["State"] != "SUCCEEDED":
            raise RuntimeError(f"Query failed: {status.get('StateChangeReason', status['State'])}\n{sql}")

        rows, columns = [], None
        for page in self.client.get_paginator("get_query_results").paginate(QueryExecutionId=query_id):
            result = page["ResultSet"]
            if columns is None:
                columns = [(c["Name"], self.CASTS.get(c["Type"], str)) for c in result["ResultSetMetadata"]["ColumnInfo"]]
                data = result["Rows"][1:]  # the first row of the first page is the header
            else:
                data = result["Rows"]
            for row in data:
                values = [cell.get("VarCharValue") for cell in row["Data"]]
                rows.append({name: None if v is None else cast(v) for (name, cast), v in zip(columns, values)})
        return rows


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Export the dashboard's numbers from the marts.")
    parser.add_argument("--bucket", help="write to s3://<bucket>/dashboard/dashboard.json")
    parser.add_argument("--out", type=Path, help="write to a local file instead, to preview the page")
    parser.add_argument("--collection-start", default="2026-10-24", help="first day of collection")
    parser.add_argument("--workgroup", default=WORKGROUP, help=f"Athena workgroup (default: {WORKGROUP})")
    parser.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1)")
    args = parser.parse_args()
    if not args.bucket and not args.out:
        parser.error("give --bucket or --out")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    athena = Athena(boto3.client("athena", region_name=args.region), args.workgroup)
    data = build(athena, "bluesky_marts", "bluesky_store", args.collection_start)
    body = json.dumps(data, separators=(",", ":")).encode()

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_bytes(body)
        where = str(args.out)
    else:
        boto3.client("s3", region_name=args.region).put_object(
            Bucket=args.bucket, Key=KEY, Body=body, ContentType="application/json"
        )
        where = f"s3://{args.bucket}/{KEY}"
    latest = data["days"][-1]["date"] if data["days"] else "no days"
    log.info("Wrote %s: %d days, through %s, %s bytes", where, len(data["days"]), latest, f"{len(body):,}")


if __name__ == "__main__":
    main()
