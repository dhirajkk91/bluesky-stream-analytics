"""
Look up the handle of every account whose identity changed, so account history can follow handles.

Jetstream's identity events say that an account's identity changed, a new handle
for instance, but almost never say what it changed to: the handle is optional in
the protocol, and in practice it's missing. So each run reads the identity events
in the event-store commits added since the last run (the same incremental read
as the projections), takes the newest per account, and asks Bluesky's public API
for those accounts' current handles, 25 at a time. It appends one row per account
to `account_handles`, and logs the run in `handle_runs`.

An account the API doesn't return (deleted, suspended or deactivated) gets a row
with no handle. Display names and other profile content are deliberately not
fetched.
"""

import argparse
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

import duckdb

from bluesky_stream_analytics.process import attach_glue, connect
from bluesky_stream_analytics.projections import (
    new_files_by_sequence,
    rewrite_sequences,
    store_files,
    store_metadata,
)

log = logging.getLogger(__name__)

API_URL = "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfiles"
BATCH_SIZE = 25  # the most getProfiles accepts in one call
USER_AGENT = "bluesky-stream-analytics (https://github.com/dhirajkk91/bluesky-stream-analytics)"
# Two calls a second at most, well inside the public API's limits.
MIN_INTERVAL_SECONDS = 0.5
MAX_ATTEMPTS = 5

HANDLE_COLUMNS = """
    did VARCHAR,
    identity_time TIMESTAMPTZ,
    identity_seq BIGINT,
    looked_up_at TIMESTAMPTZ,
    handle VARCHAR,
    found BOOLEAN
"""

RUN_COLUMNS = """
    run_at TIMESTAMPTZ,
    from_sequence BIGINT,
    to_sequence BIGINT,
    files INTEGER,
    identity_events BIGINT,
    accounts BIGINT,
    found BIGINT,
    api_calls BIGINT
"""

TABLES = {
    "account_handles": (HANDLE_COLUMNS, "month(identity_time)"),
    "handle_runs": (RUN_COLUMNS, None),
}

Fetch = Callable[[str], dict]


class LookupFailed(Exception):
    """The API kept failing for a batch of accounts."""


def fetch_json(url: str, timeout: float = 30) -> dict:
    """GET a URL and parse the JSON body. HTTP errors are raised as urllib's HTTPError."""
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read())


def _retry_after(error: urllib.error.HTTPError, attempt: int) -> float:
    header = error.headers.get("Retry-After") if error.headers else None
    if header and header.isdigit():
        return float(header)
    return float(2**attempt)


class HandleLookup:
    """Looks up accounts' current handles through app.bsky.actor.getProfiles, politely."""

    def __init__(
        self,
        fetch: Fetch = fetch_json,
        min_interval: float = MIN_INTERVAL_SECONDS,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.fetch = fetch
        self.min_interval = min_interval
        self.sleep = sleep
        self.clock = clock
        self.calls = 0
        self._last_call: float | None = None

    def lookup(self, dids: Iterable[str]) -> dict[str, str | None]:
        """Current handle per DID, None for accounts the API didn't return."""
        dids = list(dict.fromkeys(dids))
        handles: dict[str, str | None] = {}
        for i in range(0, len(dids), BATCH_SIZE):
            handles.update(self._lookup_batch(dids[i : i + BATCH_SIZE]))
        return handles

    def _lookup_batch(self, dids: list[str]) -> dict[str, str | None]:
        try:
            profiles = self._get(dids)
        except urllib.error.HTTPError as error:
            # A 400 for the batch usually means one malformed account ID; try them one by one.
            if error.code != 400 or len(dids) == 1:
                raise
            result: dict[str, str | None] = {}
            for did in dids:
                try:
                    result.update(self._lookup_batch([did]))
                except urllib.error.HTTPError as single:
                    if single.code != 400:
                        raise
                    result[did] = None
            return result
        found = {p.get("did"): p.get("handle") for p in profiles}
        return {did: found.get(did) for did in dids}

    def _get(self, dids: list[str]) -> list[dict]:
        url = f"{API_URL}?{urllib.parse.urlencode([('actors', did) for did in dids])}"
        for attempt in range(MAX_ATTEMPTS):
            self._pace()
            self.calls += 1
            try:
                return self.fetch(url).get("profiles", [])
            except urllib.error.HTTPError as error:
                if error.code == 429 or error.code >= 500:
                    wait = _retry_after(error, attempt)
                    log.warning("Profile lookup got HTTP %d, retrying in %.0fs", error.code, wait)
                    self.sleep(wait)
                    continue
                raise
            except (urllib.error.URLError, TimeoutError) as error:
                wait = float(2**attempt)
                log.warning("Profile lookup failed (%s), retrying in %.0fs", error, wait)
                self.sleep(wait)
        raise LookupFailed(f"Gave up after {MAX_ATTEMPTS} attempts for {len(dids)} accounts")

    def _pace(self) -> None:
        now = self.clock()
        if self._last_call is not None and now - self._last_call < self.min_interval:
            self.sleep(self.min_interval - (now - self._last_call))
        self._last_call = self.clock()


@dataclass
class RunResult:
    sequence: int
    files: int
    identity_events: int
    accounts: int
    found: int
    api_calls: int
    seconds: float


def create_tables(con: duckdb.DuckDBPyConnection, schema: str, warehouse: str | None = None) -> None:
    """Create the tables if they don't exist: Iceberg under the warehouse, or plain tables for tests."""
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
    return con.execute(f"SELECT coalesce(max(to_sequence), 0) FROM {schema}.handle_runs").fetchone()[0]


def identity_changes(con: duckdb.DuckDBPyConnection, files: list[str]) -> tuple[int, list[tuple[str, int, int]]]:
    """
    The identity events in these store files: how many, and the newest per
    account as (did, time in microseconds, seq).
    """
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE new_identity AS
        SELECT did, event_time, seq
        FROM read_parquet($files, union_by_name = true)
        WHERE kind = 'identity'
        """,
        {"files": files},
    )
    events = con.execute("SELECT count(*) FROM new_identity").fetchone()[0]
    newest = con.execute(
        """
        SELECT did, epoch_us(event_time), seq
        FROM new_identity
        QUALIFY row_number() OVER (PARTITION BY did ORDER BY seq DESC) = 1
        ORDER BY did
        """
    ).fetchall()
    con.execute("DROP TABLE new_identity")
    return events, newest


def resolve_files(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    lookup: HandleLookup,
    files: list[str],
    from_sequence: int,
    to_sequence: int,
) -> RunResult:
    """Look up the handles for one store commit's identity events, append them, then log the run."""
    started = time.monotonic()
    calls_before = lookup.calls
    events, newest = identity_changes(con, files)
    handles = lookup.lookup(did for did, _, _ in newest)
    rows = [(did, time_us, seq, handles.get(did), handles.get(did) is not None) for did, time_us, seq in newest]
    if rows:
        con.execute("CREATE OR REPLACE TEMP TABLE looked_up (did VARCHAR, time_us BIGINT, seq BIGINT, handle VARCHAR, found BOOLEAN)")
        con.executemany("INSERT INTO looked_up VALUES (?, ?, ?, ?, ?)", rows)
        con.execute(
            f"""
            INSERT INTO {schema}.account_handles
            SELECT did, make_timestamptz(time_us), seq, now(), handle, found
            FROM looked_up
            ORDER BY did
            """
        )
        con.execute("DROP TABLE looked_up")
    found = sum(1 for row in rows if row[4])
    calls = lookup.calls - calls_before
    con.execute(
        f"INSERT INTO {schema}.handle_runs VALUES (now(), ?, ?, ?, ?, ?, ?, ?)",
        [from_sequence, to_sequence, len(files), events, len(rows), found, calls],
    )
    return RunResult(to_sequence, len(files), events, len(rows), found, calls, time.monotonic() - started)


def resolve_all(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    events_table: str,
    lookup: HandleLookup,
    rewrites: set[int] = frozenset(),
) -> list[RunResult]:
    """Look up handles for every store commit added since the last run, oldest first."""
    after = last_sequence(con, schema)
    results = []
    for sequence, files in new_files_by_sequence(store_files(con, events_table), after, rewrites):
        result = resolve_files(con, schema, lookup, files, after, sequence)
        log.info(
            "Store commit %d: %s identity events, %s accounts looked up, %s found, %d API calls, %.0fs",
            sequence,
            f"{result.identity_events:,}",
            f"{result.accounts:,}",
            f"{result.found:,}",
            result.api_calls,
            result.seconds,
        )
        results.append(result)
        after = sequence
    if not results:
        log.info("Nothing new in the event store since commit %d", after)
    return results


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Look up the handles of accounts whose identity changed.")
    parser.add_argument("--warehouse", required=True, help="s3://<bucket>/warehouse")
    parser.add_argument("--extension-dir", type=Path, help="where DuckDB keeps its extensions")
    parser.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    con = connect(threads=2, memory_limit="1GB", extension_dir=args.extension_dir)
    try:
        events_table = attach_glue(con, args.region)
        schema = events_table.rsplit(".", 1)[0]
        create_tables(con, schema, args.warehouse)
        resolve_all(con, schema, events_table, HandleLookup(), rewrite_sequences(store_metadata(args.region)))
    finally:
        con.close()
