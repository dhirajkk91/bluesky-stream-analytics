"""
Append uploaded hourly files to the event store, then delete them.

Reads every .jsonl.zst file under uploaded/ (files the uploader has already
stored in S3), parses and de-duplicates the events with DuckDB, and appends the
ones the store doesn't have yet. Running it twice on the same files adds nothing.

The store is an Iceberg table in the AWS Glue catalog. With --catalog local it
is a table in a DuckDB file instead, for trying the job out without AWS.

"""

import argparse
import logging
import time
from dataclasses import dataclass
from pathlib import Path

import boto3
import duckdb
from compression import zstd

from bluesky_stream_analytics.event_store import COLUMNS, append_new, stage_batch
from bluesky_stream_analytics.fs import fsync_directory
from bluesky_stream_analytics.upload import UPLOADED_DIR
from bluesky_stream_analytics.writer import FINAL_SUFFIX

log = logging.getLogger(__name__)

NAMESPACE = "bluesky_store"
TABLE = "events"


@dataclass
class BatchResult:
    files: int
    lines: int
    unreadable: int
    duplicates_in_batch: int
    already_in_table: int
    added: int
    seconds: float


def pending_files(data_dir: Path) -> list[Path]:
    """Return every file waiting to be processed, oldest partition first."""
    root = data_dir / UPLOADED_DIR / "events"
    if not root.exists():
        return []
    return sorted(root.rglob(f"*{FINAL_SUFFIX}"))


def connect(threads: int, memory_limit: str, extension_dir: Path | None) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection pinned to UTC, with limits so it shares the server with the collector."""
    con = duckdb.connect()
    if extension_dir:
        con.execute(f"SET extension_directory = '{extension_dir}'")
    con.execute(f"SET threads = {threads}")
    con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute("SET TimeZone = 'UTC'")
    return con


def attach_glue(con: duckdb.DuckDBPyConnection, region: str) -> str:
    """Attach the account's Glue catalog as `lake`, using the AWS credentials in the environment."""
    account = boto3.client("sts", region_name=region).get_caller_identity()["Account"]
    for extension in ("aws", "httpfs", "iceberg"):
        con.install_extension(extension)
        con.load_extension(extension)
    con.execute(f"CREATE SECRET (TYPE s3, PROVIDER credential_chain, CHAIN 'env', REGION '{region}')")
    con.execute(f"ATTACH '{account}' AS lake (TYPE iceberg, ENDPOINT_TYPE 'glue')")
    return f"lake.{NAMESPACE}.{TABLE}"


def create_glue_table(con: duckdb.DuckDBPyConnection, table: str, warehouse: str) -> None:
    """Create the store if it doesn't exist: Iceberg v2, partitioned by event hour, Parquet with zstd."""
    location = f"{warehouse.rstrip('/')}/{NAMESPACE}/{TABLE}"
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} ({COLUMNS})
        PARTITIONED BY (hour(event_time))
        WITH (
            'format-version' = '2',
            'location' = '{location}',
            'write.parquet.compression-codec' = 'zstd'
        )
        """
    )


def count_lines(files: list[Path]) -> int:
    """Lines in the compressed files, counted while streaming them, never held in memory whole."""
    total = 0
    for path in files:
        with zstd.open(path, "rb") as f:
            while chunk := f.read(16 * 1024 * 1024):
                total += chunk.count(b"\n")
    return total


def load_batch(con: duckdb.DuckDBPyConnection, table: str, files: list[Path]) -> BatchResult:
    """Append the events from these files that the store doesn't have yet. One commit per batch."""
    started = time.monotonic()
    lines = count_lines(files)
    readable, distinct = stage_batch(con, [str(f) for f in files])
    added = append_new(con, table)
    con.execute("DROP TABLE batch")
    return BatchResult(
        files=len(files),
        lines=lines,
        unreadable=lines - readable,
        duplicates_in_batch=readable - distinct,
        already_in_table=distinct - added,
        added=added,
        seconds=time.monotonic() - started,
    )


def remove_files(data_dir: Path, files: list[Path]) -> None:
    """Delete processed files, then any partition folders left empty."""
    root = data_dir / UPLOADED_DIR
    for path in files:
        path.unlink(missing_ok=True)
    for folder in sorted({p.parent for p in files}, reverse=True):
        fsync_directory(folder)
        while folder != root and not any(folder.iterdir()):
            folder.rmdir()
            folder = folder.parent


def process_all(con: duckdb.DuckDBPyConnection, table: str, data_dir: Path, batch_files: int) -> list[BatchResult]:
    """Process every pending file, a batch at a time, oldest first."""
    files = pending_files(data_dir)
    results = []
    for i in range(0, len(files), batch_files):
        batch = files[i : i + batch_files]
        result = load_batch(con, table, batch)
        remove_files(data_dir, batch)
        log.info(
            "Loaded %d files in %.0fs: %s lines, %s unreadable, %s repeated in the batch, %s already in the table, %s added",
            result.files,
            result.seconds,
            f"{result.lines:,}",
            f"{result.unreadable:,}",
            f"{result.duplicates_in_batch:,}",
            f"{result.already_in_table:,}",
            f"{result.added:,}",
        )
        results.append(result)
    return results


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Append uploaded hourly files to the event store.")
    parser.add_argument("--catalog", choices=["glue", "local"], default="glue", help="where the store lives (default: glue)")
    parser.add_argument("--warehouse", help="s3://<bucket>/warehouse, for --catalog glue")
    parser.add_argument("--database", type=Path, help="DuckDB file, for --catalog local")
    parser.add_argument("--data-dir", type=Path, default=Path("data"), help="collector data folder (default: ./data)")
    parser.add_argument("--batch-files", type=int, default=3, help="files per commit (default: 3)")
    parser.add_argument("--threads", type=int, default=2, help="DuckDB threads (default: 2)")
    parser.add_argument("--memory-limit", default="3GB", help="DuckDB memory limit (default: 3GB)")
    parser.add_argument("--extension-dir", type=Path, help="where DuckDB keeps its extensions")
    parser.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1)")
    args = parser.parse_args()
    if args.catalog == "glue" and not args.warehouse:
        parser.error("--warehouse is required with --catalog glue")
    if args.catalog == "local" and not args.database:
        parser.error("--database is required with --catalog local")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    if not pending_files(args.data_dir):
        log.info("Nothing to process")
        return

    con = connect(args.threads, args.memory_limit, args.extension_dir)
    if args.catalog == "glue":
        table = attach_glue(con, args.region)
        create_glue_table(con, table, args.warehouse)
    else:
        con.execute(f"ATTACH '{args.database}' AS lake")
        table = f"lake.main.{TABLE}"
        con.execute(f"CREATE TABLE IF NOT EXISTS {table} ({COLUMNS})")
    try:
        process_all(con, table, args.data_dir, args.batch_files)
    finally:
        con.close()


if __name__ == "__main__":
    main()
