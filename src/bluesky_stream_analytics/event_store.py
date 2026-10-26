"""
SQL that turns raw Jetstream lines into rows for the event store, run with DuckDB.

One row per distinct event. Commits are identified by account, record type,
record key, revision and operation; identity and account events by their
sequence number. Record content (post text and the like) is not kept, and
neither are values the table can rebuild, such as a liked post's full URI.
"""

import duckdb

EVENT_KEY = ["did", "kind", "collection", "rkey", "rev", "operation", "seq"]

COLUMNS = """
    did VARCHAR,
    event_time TIMESTAMPTZ,
    kind VARCHAR,
    collection VARCHAR,
    operation VARCHAR,
    rkey VARCHAR,
    rev VARCHAR,
    seq BIGINT,
    subject_did VARCHAR,
    subject_collection VARCHAR,
    subject_rkey VARCHAR,
    reply_parent_uri VARCHAR,
    reply_root_uri VARCHAR,
    langs VARCHAR[],
    handle VARCHAR,
    account_active BOOLEAN,
    account_status VARCHAR,
    source_file VARCHAR
"""

# Only the fields the table keeps. A record stays JSON so a follow's subject (an
# account) and a like's subject (an object with the post's URI) can both be read.
_RAW_COLUMNS = """{
    'did': 'VARCHAR',
    'time_us': 'BIGINT',
    'kind': 'VARCHAR',
    'commit': 'STRUCT(rev VARCHAR, operation VARCHAR, collection VARCHAR, rkey VARCHAR, record JSON)',
    'identity': 'STRUCT(seq BIGINT, handle VARCHAR)',
    'account': 'STRUCT(seq BIGINT, active BOOLEAN, status VARCHAR)'
}"""

# Likes and reposts point at a record with an at:// URI: at://<account>/<collection>/<rkey>.
_SUBJECT_URI = "\"commit\".record ->> '$.subject.uri'"
_URI_PART = "nullif(regexp_extract(" + _SUBJECT_URI + ", '^at://([^/]+)/([^/]+)/([^/]+)$', {n}), '')"

PARSE = f"""
SELECT
    did,
    make_timestamptz(time_us) AS event_time,
    kind,
    "commit".collection AS collection,
    "commit".operation AS operation,
    "commit".rkey AS rkey,
    "commit".rev AS rev,
    coalesce(identity.seq, account.seq) AS seq,
    CASE
        WHEN "commit".collection IN ('app.bsky.graph.follow', 'app.bsky.graph.block') THEN "commit".record ->> '$.subject'
        ELSE {_URI_PART.format(n=1)}
    END AS subject_did,
    {_URI_PART.format(n=2)} AS subject_collection,
    {_URI_PART.format(n=3)} AS subject_rkey,
    "commit".record ->> '$.reply.parent.uri' AS reply_parent_uri,
    "commit".record ->> '$.reply.root.uri' AS reply_root_uri,
    CASE WHEN "commit".collection = 'app.bsky.feed.post'
        THEN CAST("commit".record -> '$.langs' AS VARCHAR[]) END AS langs,
    identity.handle AS handle,
    account.active AS account_active,
    account.status AS account_status,
    regexp_extract(filename, '(events/date=[^/]+/hour=[^/]+/[^/]+)$', 1) AS source_file
FROM read_ndjson($files, compression = 'zstd', ignore_errors = true, filename = true, columns = {_RAW_COLUMNS})
WHERE did IS NOT NULL AND time_us IS NOT NULL AND kind IS NOT NULL
"""


def key_match(left: str, right: str) -> str:
    """SQL condition for two rows with the same event key. Null-safe, since commits have no seq and vice versa."""
    return " AND ".join(f"{left}.{c} IS NOT DISTINCT FROM {right}.{c}" for c in EVENT_KEY)


def stage_batch(con: duckdb.DuckDBPyConnection, files: list[str]) -> tuple[int, int]:
    """
    Parse the files into a temp table `batch`, one row per event key.

    Returns (readable rows, distinct events).
    """
    con.execute(f"CREATE OR REPLACE TEMP TABLE parsed AS {PARSE}", {"files": files})
    readable = con.execute("SELECT count(*) FROM parsed").fetchone()[0]
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE batch AS
        SELECT * FROM parsed
        QUALIFY row_number() OVER (PARTITION BY {", ".join(EVENT_KEY)} ORDER BY source_file) = 1
        """
    )
    con.execute("DROP TABLE parsed")
    distinct = con.execute("SELECT count(*) FROM batch").fetchone()[0]
    return readable, distinct


def append_new(con: duckdb.DuckDBPyConnection, table: str) -> int:
    """
    Append the rows in `batch` that `table` doesn't have yet. Returns how many were added.

    Only table rows inside the batch's event-time range are compared, so an
    Iceberg table only reads the files that could hold a match.
    """
    lo, hi = con.execute("SELECT epoch_us(min(event_time)), epoch_us(max(event_time)) FROM batch").fetchone()
    if lo is None:
        return 0
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE new_rows AS
        SELECT b.* FROM batch b
        ANTI JOIN (
            SELECT {", ".join(EVENT_KEY)} FROM {table}
            WHERE event_time >= make_timestamptz({lo}) AND event_time <= make_timestamptz({hi})
        ) t ON {key_match("b", "t")}
        """
    )
    added = con.execute("SELECT count(*) FROM new_rows").fetchone()[0]
    if added:
        # Sorted by record type and account, the files compress much better than in arrival order.
        con.execute(f"INSERT INTO {table} SELECT * FROM new_rows ORDER BY collection, did, event_time")
    con.execute("DROP TABLE new_rows")
    return added
