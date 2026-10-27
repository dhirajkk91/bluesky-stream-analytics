"""
Compaction of the projections, run in Athena from the server.

Folding only appends, so deleted records stay in the files (the views hide
them), edited posts keep their old versions, and every hour adds small files.
For a range of record types and creation days, compaction:

1. Redacts the event store: blanks what a deleted record pointed at (subject,
   reply links, languages) in the events that created or edited it.
2. Removes from `records` every row that has a newer delete or a newer version.
3. Rewrites the files of `records` and of the event store that had rows
   removed or redacted, and merges small ones (OPTIMIZE). Until a file is
   rewritten, a removed row is only masked by a delete file.

A separate run with --vacuum, a couple of hours later, expires snapshots more
than an hour old and deletes the files nothing refers to any more. Only then is
a deleted record's data gone from S3.

How far back a run reaches depends on the day of the week and month and on the
record type (TIERS): rewriting everything daily would cost more every day.
Athena runs inside AWS, so reading the tables costs only Athena's price per
byte scanned, not data transfer.
"""

import argparse
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import boto3

log = logging.getLogger(__name__)

DATABASE = "bluesky_store"
WORKGROUP = "bluesky-stream-analytics"
SNAPSHOT_RETENTION_SECONDS = 3600
# Creates are matched to their events by exact time; this margin only widens the
# range Athena reads, for apps whose clock was ahead when they made the key.
EVENT_TIME_MARGIN = timedelta(days=1)

POSTS = ("app.bsky.feed.post", "app.bsky.feed.repost")
INTERACTIONS = ("app.bsky.feed.like", "app.bsky.graph.follow", "app.bsky.graph.block")
GROUP_NAMES = {POSTS: "posts and reposts", INTERACTIONS: "likes, follows and blocks"}

# For each tier, how many days of record creation it reaches back, per group of
# record types. None means all of history. Over a third of deletes are of
# records under a day old, so the daily run catches those cheaply; the weekly
# and monthly runs catch the rest. Posts carry more, and are about a tenth of
# the rows, so they get the full pass weekly.
TIERS = {
    "daily": {POSTS: 3, INTERACTIONS: 3},
    "weekly": {POSTS: None, INTERACTIONS: 8},
    "monthly": {POSTS: None, INTERACTIONS: None},
}

Timestamp = Callable[[datetime], str]


def athena_timestamp(moment: datetime) -> str:
    return f"TIMESTAMP '{moment:%Y-%m-%d %H:%M:%S} UTC'"


def _log_timestamp(moment: datetime) -> str:
    # The run log's column is a plain timestamp, in UTC.
    return f"TIMESTAMP '{moment:%Y-%m-%d %H:%M:%S}'"


@dataclass(frozen=True)
class Scope:
    """The records a step covers: these record types, created on or after `since` (None for all)."""

    collections: tuple[str, ...]
    since: datetime | None

    def describe(self) -> str:
        reach = "all" if self.since is None else f"created since {self.since:%Y-%m-%d}"
        return f"{GROUP_NAMES.get(self.collections, ', '.join(self.collections))}, {reach}"


@dataclass(frozen=True)
class Step:
    name: str
    scope: str
    sql: str


@dataclass(frozen=True)
class StepResult:
    step: Step
    seconds: float
    scanned_bytes: int


def tier_for(day: date) -> str:
    """The 1st of the month is the full run, Sundays the weekly one, every other day the daily one."""
    if day.day == 1:
        return "monthly"
    if day.weekday() == 6:
        return "weekly"
    return "daily"


def scopes(tier: str, today: date) -> list[Scope]:
    """The scopes a tier covers. `since` falls on a midnight, a boundary of the day partitions."""
    midnight = datetime(today.year, today.month, today.day, tzinfo=UTC)
    return [
        Scope(group, None if days is None else midnight - timedelta(days=days))
        for group, days in TIERS[tier].items()
    ]


def _in(collections: tuple[str, ...]) -> str:
    return "(" + ", ".join(f"'{c}'" for c in collections) + ")"


def _since(column: str, since: datetime | None, ts: Timestamp) -> str:
    return "" if since is None else f"AND {column} >= {ts(since)}"


def _latest_deletes(scope: Scope, ts: Timestamp) -> str:
    # A record is deleted after it's created, so its delete is no older than `since` either.
    return f"""
        SELECT did, collection, rkey, max(rev) AS rev
        FROM {DATABASE}.record_deletes
        WHERE collection IN {_in(scope.collections)} {_since("event_time", scope.since, ts)}
        GROUP BY did, collection, rkey
    """


def redact_sql(scope: Scope, ts: Timestamp = athena_timestamp) -> str:
    """Blank the details of the events that created or edited a record that has since been deleted."""
    margin = None if scope.since is None else scope.since - EVENT_TIME_MARGIN
    return f"""
        MERGE INTO {DATABASE}.events e
        USING (
            SELECT DISTINCT r.did, r.collection, r.rkey, r.rev, r.event_time
            FROM {DATABASE}.records r
            JOIN ({_latest_deletes(scope, ts)}) d
              ON r.did = d.did AND r.collection = d.collection AND r.rkey = d.rkey AND r.rev < d.rev
            WHERE r.collection IN {_in(scope.collections)} {_since("r.record_time", scope.since, ts)}
        ) v
        ON e.did = v.did AND e.collection = v.collection AND e.rkey = v.rkey AND e.rev = v.rev
           AND e.event_time = v.event_time AND e.kind = 'commit' {_since("e.event_time", margin, ts)}
        WHEN MATCHED AND (
            e.subject_did IS NOT NULL OR e.subject_collection IS NOT NULL OR e.subject_rkey IS NOT NULL
            OR e.reply_parent_uri IS NOT NULL OR e.reply_root_uri IS NOT NULL OR e.langs IS NOT NULL
        ) THEN UPDATE SET
            subject_did = NULL, subject_collection = NULL, subject_rkey = NULL,
            reply_parent_uri = NULL, reply_root_uri = NULL, langs = NULL
    """


def remove_superseded_sql(scope: Scope, ts: Timestamp = athena_timestamp) -> str:
    """Remove every row of `records` that has a newer delete or a newer version of the same record."""
    return f"""
        MERGE INTO {DATABASE}.records r
        USING (
            SELECT did, collection, rkey, max(rev) AS rev
            FROM (
                SELECT did, collection, rkey, rev
                FROM {DATABASE}.record_deletes
                WHERE collection IN {_in(scope.collections)} {_since("event_time", scope.since, ts)}
                UNION ALL
                SELECT did, collection, rkey, rev
                FROM {DATABASE}.records
                WHERE collection IN {_in(scope.collections)} {_since("record_time", scope.since, ts)}
            ) changes
            GROUP BY did, collection, rkey
            HAVING count(*) > 1
        ) s
        ON r.did = s.did AND r.collection = s.collection AND r.rkey = s.rkey AND r.rev < s.rev
           AND r.collection IN {_in(scope.collections)} {_since("r.record_time", scope.since, ts)}
        WHEN MATCHED THEN DELETE
    """


def optimize_sql(scope: Scope, ts: Timestamp = athena_timestamp) -> str:
    where = f"WHERE collection IN {_in(scope.collections)} {_since('record_time', scope.since, ts)}"
    return f"OPTIMIZE {DATABASE}.records REWRITE DATA USING BIN_PACK {where}"


def optimize_events_sql(since: datetime | None, ts: Timestamp = athena_timestamp) -> str:
    """Rewrite the store's files that redaction left delete files on, so the original rows are gone."""
    if since is None:
        return f"OPTIMIZE {DATABASE}.events REWRITE DATA USING BIN_PACK"
    return f"OPTIMIZE {DATABASE}.events REWRITE DATA USING BIN_PACK WHERE event_time >= {ts(since - EVENT_TIME_MARGIN)}"


def plan(tier: str, today: date) -> list[Step]:
    """
    Every step of one compaction run. Redaction comes first, while `records`
    still has the deleted rows and their event times, then the removals, then
    the rewrites, once for the event store over the widest reach.
    """
    reaches = scopes(tier, today)
    widest = None if any(s.since is None for s in reaches) else min(s.since for s in reaches)
    steps = [Step("redact the event store", s.describe(), redact_sql(s)) for s in reaches]
    steps += [Step("remove superseded rows", s.describe(), remove_superseded_sql(s)) for s in reaches]
    steps += [Step("optimize records", s.describe(), optimize_sql(s)) for s in reaches]
    reach = "all" if widest is None else f"since {widest - EVENT_TIME_MARGIN:%Y-%m-%d}"
    steps.append(Step("optimize events", reach, optimize_events_sql(widest)))
    steps.append(Step("optimize record_deletes", "all", f"OPTIMIZE {DATABASE}.record_deletes REWRITE DATA USING BIN_PACK"))
    return steps


def vacuum_plan() -> list[Step]:
    return [Step(f"vacuum {t}", "all", f"VACUUM {DATABASE}.{t}") for t in ("records", "record_deletes", "events")]


def setup_steps(warehouse: str) -> list[Step]:
    """
    Create the run log if needed. Keep an hour of snapshots on every table
    compaction touches, and rewrite any file with even one delete file on it,
    so no removed row outlives the next OPTIMIZE.
    """
    location = f"{warehouse.rstrip('/')}/{DATABASE}/compaction_runs"
    steps = [
        Step(
            "create the run log",
            "",
            f"""
            CREATE TABLE IF NOT EXISTS {DATABASE}.compaction_runs (
                run_started_at timestamp, tier string, step string, scope string,
                seconds double, scanned_bytes bigint, succeeded boolean
            )
            LOCATION '{location}'
            TBLPROPERTIES ('table_type' = 'ICEBERG')
            """,
        )
    ]
    for table in ("records", "record_deletes", "events"):
        steps.append(
            Step(
                f"table properties: {table}",
                "",
                f"ALTER TABLE {DATABASE}.{table} SET TBLPROPERTIES "
                f"('vacuum_max_snapshot_age_seconds' = '{SNAPSHOT_RETENTION_SECONDS}', "
                "'optimize_rewrite_delete_file_threshold' = '1')",
            )
        )
    return steps


class Athena:
    """Runs one statement at a time in the workgroup and waits for it."""

    def __init__(self, client, workgroup: str = WORKGROUP, poll_seconds: float = 2) -> None:
        self.client = client
        self.workgroup = workgroup
        self.poll_seconds = poll_seconds

    def run(self, step: Step) -> StepResult:
        started = time.monotonic()
        query_id = self.client.start_query_execution(
            QueryString=step.sql,
            WorkGroup=self.workgroup,
            QueryExecutionContext={"Database": DATABASE},
        )["QueryExecutionId"]
        while True:
            execution = self.client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
            state = execution["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(self.poll_seconds)
        result = StepResult(step, time.monotonic() - started, execution.get("Statistics", {}).get("DataScannedInBytes", 0))
        if state != "SUCCEEDED":
            reason = execution["Status"].get("StateChangeReason", state)
            raise RuntimeError(f"{step.name} ({step.scope}) failed: {reason}")
        return result


def log_runs(athena: Athena, started: datetime, tier: str, results: list[StepResult], failed: Step | None) -> None:
    """Append one row per step to the run log, in a single statement."""
    def row(step: Step, seconds: float, scanned: int, succeeded: bool) -> str:
        values = [
            _log_timestamp(started),
            f"'{tier}'",
            f"'{step.name}'",
            f"'{step.scope}'",
            f"{seconds:.1f}",
            str(scanned),
            "true" if succeeded else "false",
        ]
        return "(" + ", ".join(values) + ")"

    rows = [row(r.step, r.seconds, r.scanned_bytes, True) for r in results]
    if failed is not None:
        rows.append(row(failed, 0, 0, False))
    if rows:
        athena.run(Step("log the run", "", f"INSERT INTO {DATABASE}.compaction_runs VALUES {', '.join(rows)}"))


def run_steps(athena: Athena, tier: str, steps: list[Step], started: datetime) -> list[StepResult]:
    """Run the steps in order, stopping at the first failure, and log whatever ran either way."""
    results: list[StepResult] = []
    for step in steps:
        try:
            result = athena.run(step)
        except Exception:
            try:
                log_runs(athena, started, tier, results, step)
            except Exception:
                log.exception("Couldn't log the failed run")
            raise
        results.append(result)
        log.info(
            "%s (%s): %.0fs, %s MB scanned",
            step.name,
            step.scope,
            result.seconds,
            f"{result.scanned_bytes / 1e6:,.0f}",
        )
    log_runs(athena, started, tier, results, None)
    return results


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Compact the projections and redact deleted records, in Athena.")
    parser.add_argument("--warehouse", required=True, help="s3://<bucket>/warehouse, where the run log lives")
    parser.add_argument("--tier", choices=["auto", *TIERS], default="auto", help="how far back to reach (default: by date)")
    parser.add_argument("--vacuum", action="store_true", help="expire old snapshots and delete unused files instead")
    parser.add_argument("--workgroup", default=WORKGROUP, help=f"Athena workgroup (default: {WORKGROUP})")
    parser.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    started = datetime.now(UTC).replace(microsecond=0)
    athena = Athena(boto3.client("athena", region_name=args.region), args.workgroup)
    for step in setup_steps(args.warehouse):
        athena.run(step)

    if args.vacuum:
        tier, steps = "vacuum", vacuum_plan()
    else:
        tier = tier_for(started.date()) if args.tier == "auto" else args.tier
        steps = plan(tier, started.date())
    log.info("Starting the %s run: %d steps", tier, len(steps))
    run_steps(athena, tier, steps, started)
    log.info("Finished the %s run", tier)
