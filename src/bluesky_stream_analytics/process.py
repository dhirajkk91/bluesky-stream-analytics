"""
Append uploaded hourly files to the event store, then delete them.

Reads every .jsonl.zst file under uploaded/ (files the uploader has already
stored in S3), parses and de-duplicates the events, and inserts the ones the
table doesn't have yet. Running it twice on the same files adds nothing.

Usage:
    uv run process --warehouse s3://<bucket>/warehouse
    uv run process --catalog hadoop --warehouse ./warehouse
"""

import argparse
import logging
import time
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from bluesky_stream_analytics.fs import fsync_directory
from bluesky_stream_analytics.event_store import TABLE_COLUMNS, distinct_events, key_condition, parse_lines, readable
from bluesky_stream_analytics.spark import CATALOG, build_session
from bluesky_stream_analytics.upload import UPLOADED_DIR
from bluesky_stream_analytics.writer import FINAL_SUFFIX

log = logging.getLogger(__name__)

DATABASE = "bluesky_store"
EVENTS_TABLE = f"{CATALOG}.{DATABASE}.events"


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


def create_events_table(spark: SparkSession) -> None:
    """Create the events table if it doesn't exist: Iceberg v2, partitioned by event hour."""
    if spark.catalog.tableExists(EVENTS_TABLE):
        return
    spark.sql(
        f"""
        CREATE TABLE {EVENTS_TABLE} ({TABLE_COLUMNS})
        USING iceberg
        PARTITIONED BY (hours(event_time))
        TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
        """
    )
    # Rows sorted by record type and account inside each file compress much better.
    spark.sql(f"ALTER TABLE {EVENTS_TABLE} WRITE LOCALLY ORDERED BY collection, did, time_us")


def read_lines(spark: SparkSession, files: list[Path]) -> DataFrame:
    """Read raw lines with source_file set to the path under the data folder, e.g. events/date=…/hour=…/x.jsonl.zst."""
    return spark.read.text([str(f) for f in files]).select(
        "value",
        F.regexp_extract("_metadata.file_path", r"(events/date=[^/]+/hour=[^/]+/[^/]+)$", 1).alias("source_file"),
    )


def load_batch(spark: SparkSession, files: list[Path]) -> BatchResult:
    """
    Insert the events from these files that the table doesn't have yet.

    The merge only looks at table rows in the batch's event-time range, so it
    reads the hours the batch touches and nothing else.
    """
    started = time.monotonic()
    # A .zst file can't be split, so each file arrives as one partition.
    # Spreading the lines out lets every core share the parsing.
    lines = read_lines(spark, files).repartition(spark.sparkContext.defaultParallelism * 2)
    parsed = parse_lines(lines).cache()
    events = distinct_events(readable(parsed)).cache()

    line_count = parsed.count()
    parsed_count = readable(parsed).count()
    bounds = events.agg(F.min("time_us").alias("lo"), F.max("time_us").alias("hi")).first()
    event_count = events.count()

    added = 0
    if event_count:
        before = _row_count(spark)
        events.createOrReplaceTempView("batch")
        spark.sql(
            f"""
            MERGE INTO {EVENTS_TABLE} t
            USING batch s
            ON t.event_time >= timestamp_micros({bounds.lo})
               AND t.event_time <= timestamp_micros({bounds.hi})
               AND {key_condition("t", "s")}
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        added = _row_count(spark) - before

    parsed.unpersist()
    events.unpersist()
    return BatchResult(
        files=len(files),
        lines=line_count,
        unreadable=line_count - parsed_count,
        duplicates_in_batch=parsed_count - event_count,
        already_in_table=event_count - added,
        added=added,
        seconds=time.monotonic() - started,
    )


def _row_count(spark: SparkSession) -> int:
    """Total rows in the table, read from Iceberg metadata rather than by scanning data."""
    row = spark.sql(f"SELECT summary['total-records'] AS n FROM {EVENTS_TABLE}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    return int(row.n) if row and row.n is not None else 0


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


def process_all(spark: SparkSession, data_dir: Path, batch_files: int) -> list[BatchResult]:
    """Process every pending file, a batch at a time, oldest first. Each batch is its own commit."""
    create_events_table(spark)
    files = pending_files(data_dir)
    results = []
    for i in range(0, len(files), batch_files):
        batch = files[i : i + batch_files]
        result = load_batch(spark, batch)
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
    parser.add_argument("--warehouse", required=True, help="table location: s3://<bucket>/warehouse, or a folder with --catalog hadoop")
    parser.add_argument("--catalog", choices=["glue", "hadoop"], default="glue", help="Iceberg catalog type (default: glue)")
    parser.add_argument("--data-dir", type=Path, default=Path("data"), help="collector data folder (default: ./data)")
    parser.add_argument("--jars-dir", type=Path, default=Path(".jars"), help="folder with the Iceberg jars (default: ./.jars)")
    parser.add_argument("--batch-files", type=int, default=3, help="files per commit (default: 3)")
    parser.add_argument("--driver-memory", default="4g", help="Spark memory (default: 4g)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)
    if not pending_files(args.data_dir):
        log.info("Nothing to process")
        return

    spark = build_session(args.warehouse, args.jars_dir, args.catalog, args.driver_memory)
    try:
        if args.catalog == "hadoop":
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{DATABASE}")
        process_all(spark, args.data_dir, args.batch_files)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
