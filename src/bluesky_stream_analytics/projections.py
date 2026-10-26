"""
Projections: the current state of every like, post, repost, follow and block, folded from the event store.

Each run reads only the event-store files added since the last run, found by
their Iceberg sequence number, and appends to three tables:

    records          every new version of a record (creates and edits)
    record_deletes   a marker for every delete: the record's key, rev and time, nothing else
    projection_runs  one row per run, with the sequence number it reached

Nothing already in the projection is read or rewritten, so a run costs the same
however large the tables grow. Which version is current is decided by the
current_records view: a record's highest rev, unless a delete with a higher rev
exists. Appending the same events twice, or in any order, gives the same answer.
"""

import argparse
import logging
import time
from dataclasses import dataclass
from itertools import groupby
from pathlib import Path

import duckdb

from bluesky_stream_analytics.process import attach_glue, connect

log = logging.getLogger(__name__)

TRACKED_COLLECTIONS = (
    "app.bsky.feed.post",
    "app.bsky.feed.like",
    "app.bsky.feed.repost",
    "app.bsky.graph.follow",
    "app.bsky.graph.block",
)

RECORD_COLUMNS = """
    did VARCHAR,
    collection VARCHAR,
    rkey VARCHAR,
    rev VARCHAR,
    record_time TIMESTAMPTZ,
    event_time TIMESTAMPTZ,
    operation VARCHAR,
    subject_did VARCHAR,
    subject_collection VARCHAR,
    subject_rkey VARCHAR,
    reply_parent_uri VARCHAR,
    reply_root_uri VARCHAR,
    langs VARCHAR[]
"""

DELETE_COLUMNS = """
    did VARCHAR,
    collection VARCHAR,
    rkey VARCHAR,
    rev VARCHAR,
    record_time TIMESTAMPTZ,
    event_time TIMESTAMPTZ
"""

RUN_COLUMNS = """
    run_at TIMESTAMPTZ,
    from_sequence BIGINT,
    to_sequence BIGINT,
    files INTEGER,
    events_folded BIGINT,
    versions_added BIGINT,
    deletes_added BIGINT
"""

# Table name -> (columns, Iceberg partitioning). Records are partitioned by when
# they were created, so a query on one month of likes reads only that month.
# Delete markers are partitioned by when the delete happened, so each run
# writes to the current month only, however old the deleted records are.
TABLES = {
    "records": (RECORD_COLUMNS, "collection, month(record_time)"),
    "record_deletes": (DELETE_COLUMNS, "collection, month(event_time)"),
    "projection_runs": (RUN_COLUMNS, None),
}

# Record keys of these types are TIDs: 13 base32-sortable characters of 5 bits
# holding a 64-bit number, a 0 bit, 53 bits of microseconds since 1970 and a
# 10-bit clock id. The last 2 characters are exactly the clock id, so the first
# 11 decode to the creation time. A key that isn't a TID, or decodes to before
# 2022 or to after the event was seen, gets no record_time.
TID_ALPHABET = "234567abcdefghijklmnopqrstuvwxyz"
TID_MACROS = f"""
CREATE OR REPLACE TEMP MACRO tid_micros(rkey) AS
    CASE WHEN regexp_full_match(rkey, '[2-7a-j][2-7a-z]{{12}}') THEN list_reduce(
        list_transform(string_split(rkey[1:11], ''), lambda c: CAST(instr('{TID_ALPHABET}', c) - 1 AS BIGINT)),
        lambda acc, d: acc * 32 + d
    ) END;
CREATE OR REPLACE TEMP MACRO tid_time(rkey, seen_at) AS
    CASE WHEN tid_micros(rkey) BETWEEN epoch_us(TIMESTAMPTZ '2022-01-01 00:00:00+00')
                                   AND epoch_us(seen_at) + 86400000000
        THEN make_timestamptz(tid_micros(rkey)) END;
"""

# Only the columns the fold uses, so reading the store's files skips the rest.
_EVENT_COLUMNS = """
    did, event_time, kind, collection, operation, rkey, rev,
    subject_did, subject_collection, subject_rkey, reply_parent_uri, reply_root_uri, langs
"""
_TRACKED = "(" + ", ".join(f"'{c}'" for c in TRACKED_COLLECTIONS) + ")"


@dataclass
class RunResult:
    sequence: int
    files: int
    events: int
    versions: int
    deletes: int
    seconds: float


def create_tables(con: duckdb.DuckDBPyConnection, schema: str, warehouse: str | None = None) -> None:
    """
    Create the projection tables if they don't exist. With a warehouse they're
    partitioned Iceberg tables under it; without one, plain tables (for tests).
    """
    for name, (columns, partitioning) in TABLES.items():
        if warehouse is None:
            con.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{name} ({columns})")
            continue
        namespace = schema.split(".")[-1]
        partitioned = f"PARTITIONED BY ({partitioning})" if partitioning else ""
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.{name} ({columns})
            {partitioned}
            WITH (
                'format-version' = '2',
                'location' = '{warehouse.rstrip("/")}/{namespace}/{name}',
                'write.parquet.compression-codec' = 'zstd'
            )
            """
        )


def last_sequence(con: duckdb.DuckDBPyConnection, schema: str) -> int:
    """The event-store sequence number the projections have been folded up to, 0 if never run."""
    return con.execute(f"SELECT coalesce(max(to_sequence), 0) FROM {schema}.projection_runs").fetchone()[0]


def store_files(con: duckdb.DuckDBPyConnection, events_table: str) -> list[tuple[str, str, int]]:
    """The event store's live files, from Iceberg's metadata: (content, path, data sequence number)."""
    return con.execute(
        f"""
        SELECT content, file_path, data_sequence_number
        FROM iceberg_metadata({events_table})
        WHERE status <> 'DELETED'
        """
    ).fetchall()


def new_files_by_sequence(files: list[tuple[str, str, int]], after: int) -> list[tuple[int, list[str]]]:
    """
    Group the data files added after sequence number `after` by the commit that
    added them, oldest first.

    The store is append-only, so its files are read directly. A delete file
    would mean rows were removed, which reading the files alone would miss, so
    it stops the run instead.
    """
    if any(content != "DATA" for content, _, _ in files):
        raise ValueError("The event store has delete files; it should only ever be appended to")
    new = sorted((sequence, path) for _, path, sequence in files if sequence > after)
    return [(sequence, [path for _, path in group]) for sequence, group in groupby(new, key=lambda f: f[0])]


def stage_events(con: duckdb.DuckDBPyConnection, files: list[str]) -> int:
    """Read the tracked record types' commits from these store files into a temp table `new_events`."""
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE new_events AS
        SELECT {_EVENT_COLUMNS}
        FROM read_parquet($files, union_by_name = true)
        WHERE kind = 'commit' AND collection IN {_TRACKED}
        """,
        {"files": files},
    )
    return con.execute("SELECT count(*) FROM new_events").fetchone()[0]


def fold(con: duckdb.DuckDBPyConnection) -> tuple[int, int]:
    """
    Turn `new_events` into temp tables `new_versions` (creates and edits) and
    `new_deletes` (markers). Returns how many of each.
    """
    con.execute(TID_MACROS)
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE new_versions AS
        SELECT
            did, collection, rkey, rev,
            tid_time(rkey, event_time) AS record_time,
            event_time, operation,
            subject_did, subject_collection, subject_rkey,
            reply_parent_uri, reply_root_uri, langs
        FROM new_events
        WHERE operation IN ('create', 'update')
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE new_deletes AS
        SELECT did, collection, rkey, rev, tid_time(rkey, event_time) AS record_time, event_time
        FROM new_events
        WHERE operation = 'delete'
        """
    )
    versions = con.execute("SELECT count(*) FROM new_versions").fetchone()[0]
    deletes = con.execute("SELECT count(*) FROM new_deletes").fetchone()[0]
    return versions, deletes


def fold_files(con: duckdb.DuckDBPyConnection, schema: str, files: list[str], from_sequence: int, to_sequence: int) -> RunResult:
    """
    Fold one commit's worth of store files into the projections, then log the run.

    The run is logged last. If the job dies before that, the next run folds the
    same files again and appends rows identical to ones already there, which the
    views ignore.
    """
    started = time.monotonic()
    events = stage_events(con, files)
    versions, deletes = fold(con)
    # Sorted by account and key, the files compress far better than in arrival order.
    if versions:
        con.execute(f"INSERT INTO {schema}.records SELECT * FROM new_versions ORDER BY collection, did, rkey")
    if deletes:
        con.execute(f"INSERT INTO {schema}.record_deletes SELECT * FROM new_deletes ORDER BY collection, did, rkey")
    con.execute(
        f"INSERT INTO {schema}.projection_runs VALUES (now(), ?, ?, ?, ?, ?, ?)",
        [from_sequence, to_sequence, len(files), events, versions, deletes],
    )
    for name in ("new_events", "new_versions", "new_deletes"):
        con.execute(f"DROP TABLE {name}")
    return RunResult(to_sequence, len(files), events, versions, deletes, time.monotonic() - started)


def project_all(con: duckdb.DuckDBPyConnection, schema: str, events_table: str) -> list[RunResult]:
    """Fold every store commit added since the last run, one commit at a time, oldest first."""
    after = last_sequence(con, schema)
    results = []
    for sequence, files in new_files_by_sequence(store_files(con, events_table), after):
        result = fold_files(con, schema, files, after, sequence)
        log.info(
            "Folded store commit %d (%d files) in %.0fs: %s events, %s versions, %s deletes",
            sequence,
            result.files,
            result.seconds,
            f"{result.events:,}",
            f"{result.versions:,}",
            f"{result.deletes:,}",
        )
        results.append(result)
        after = sequence
    if not results:
        log.info("Nothing new in the event store since commit %d", after)
    return results


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Fold new event-store commits into the projections.")
    parser.add_argument("--warehouse", required=True, help="s3://<bucket>/warehouse")
    parser.add_argument("--threads", type=int, default=2, help="DuckDB threads (default: 2)")
    parser.add_argument("--memory-limit", default="3GB", help="DuckDB memory limit (default: 3GB)")
    parser.add_argument("--extension-dir", type=Path, help="where DuckDB keeps its extensions")
    parser.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    con = connect(args.threads, args.memory_limit, args.extension_dir)
    try:
        events_table = attach_glue(con, args.region)
        schema = events_table.rsplit(".", 1)[0]
        create_tables(con, schema, args.warehouse)
        project_all(con, schema, events_table)
    finally:
        con.close()
