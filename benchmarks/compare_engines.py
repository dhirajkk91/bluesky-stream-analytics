"""
Run the event store's transform on the same raw files with Spark and with DuckDB.

Both engines do the same work: read the zstd JSON lines, parse the fields the
event store keeps, drop repeats on the event key, and write Parquet with zstd.
Each engine runs in its own process, so its startup, time and peak memory are
measured separately, and both outputs are checked to hold the same events.

"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

THREADS = 2

DUCKDB_SQL = """
COPY (
    SELECT * FROM (
        SELECT
            did,
            time_us,
            make_timestamp(time_us) AS event_time,
            kind,
            "commit".collection AS collection,
            "commit".operation AS operation,
            "commit".rkey AS rkey,
            "commit".rev AS rev,
            "commit".cid AS cid,
            coalesce(identity.seq, account.seq) AS seq,
            CASE
                WHEN "commit".collection IN ('app.bsky.graph.follow', 'app.bsky.graph.block')
                    THEN "commit".record ->> '$.subject'
                ELSE nullif(regexp_extract("commit".record ->> '$.subject.uri', '^at://([^/]+)/', 1), '')
            END AS subject_did,
            CASE WHEN "commit".collection IN ('app.bsky.feed.like', 'app.bsky.feed.repost')
                THEN "commit".record ->> '$.subject.uri' END AS subject_uri,
            "commit".record ->> '$.reply.parent.uri' AS reply_parent_uri,
            "commit".record ->> '$.reply.root.uri' AS reply_root_uri,
            CASE WHEN "commit".collection = 'app.bsky.feed.post'
                THEN CAST("commit".record -> '$.langs' AS VARCHAR[]) END AS langs,
            identity.handle AS handle,
            account.active AS account_active,
            account.status AS account_status
        FROM read_ndjson(
            $files,
            compression = 'zstd',
            ignore_errors = true,
            columns = {
                'did': 'VARCHAR',
                'time_us': 'BIGINT',
                'kind': 'VARCHAR',
                'commit': 'STRUCT(rev VARCHAR, operation VARCHAR, collection VARCHAR, rkey VARCHAR, cid VARCHAR, record JSON)',
                'identity': 'STRUCT(seq BIGINT, handle VARCHAR)',
                'account': 'STRUCT(seq BIGINT, active BOOLEAN, status VARCHAR)'
            }
        )
        WHERE did IS NOT NULL AND time_us IS NOT NULL AND kind IS NOT NULL
    )
    QUALIFY row_number() OVER (PARTITION BY did, kind, collection, rkey, rev, operation, seq) = 1
    ORDER BY collection, did, time_us
) TO '{out}' (FORMAT parquet, COMPRESSION zstd)
"""


def run_duckdb(files: list[str], out: Path) -> dict:
    """Returns this process's peak memory and CPU time."""
    import duckdb

    con = duckdb.connect()
    con.execute(f"SET threads = {THREADS}")
    con.execute(DUCKDB_SQL.replace("{out}", str(out / "events.parquet")), {"files": files})
    return _usage("self")


def run_spark(files: list[str], out: Path) -> dict:
    """Returns the peak memory and CPU time of the Spark JVM plus this Python process."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    from bluesky_stream_analytics.event_store import distinct_events, parse_lines, readable

    spark = (
        SparkSession.builder.master(f"local[{THREADS}]")
        .config("spark.driver.memory", "4g")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    lines = spark.read.text(files).select("value", F.lit("").alias("source_file"))
    lines = lines.repartition(spark.sparkContext.defaultParallelism * 2)
    events = distinct_events(readable(parse_lines(lines))).drop("source_file")
    events = events.sortWithinPartitions("collection", "did", "time_us")
    events.write.mode("overwrite").option("compression", "zstd").parquet(str(out))
    jvm = _usage(spark.sparkContext._jvm.java.lang.ProcessHandle.current().pid())
    driver = _usage("self")
    spark.stop()
    return {k: jvm[k] + driver[k] for k in jvm}


def _usage(pid) -> dict:
    """Peak resident memory (MB) and CPU time (seconds) of a running process, from /proc. Linux only."""
    peak_kb = next(
        int(line.split()[1])
        for line in Path(f"/proc/{pid}/status").read_text().splitlines()
        if line.startswith("VmHWM:")
    )
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    ticks = int(fields[11]) + int(fields[12])
    return {"peak_memory_mb": peak_kb // 1024, "cpu_seconds": round(ticks / os.sysconf("SC_CLK_TCK"), 1)}


def summarize(out: Path) -> dict:
    """Row count and two checksums of an engine's output, computed the same way for both."""
    import duckdb

    row = (
        duckdb.connect()
        .execute(f"SELECT count(*), sum(time_us), count(DISTINCT did) FROM read_parquet('{out}/*.parquet')")
        .fetchone()
    )
    return {"rows": row[0], "sum_time_us": row[1], "accounts": row[2]}


def measure(engine: str, files: list[str], workdir: Path) -> dict:
    """Run one engine in a child process and return its time, peak memory and output summary."""
    out = workdir / engine
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    stats_file = workdir / f"{engine}.json"
    started = time.monotonic()
    subprocess.run(
        [sys.executable, __file__, "--engine", engine, "--out", str(out), "--stats-file", str(stats_file), *files],
        check=True,
    )
    seconds = time.monotonic() - started
    size = sum(p.stat().st_size for p in out.rglob("*.parquet"))
    return {
        "engine": engine,
        "seconds": round(seconds, 1),
        **json.loads(stats_file.read_text()),
        "output_mb": round(size / 1_000_000, 1),
        **summarize(out),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare Spark and DuckDB on the event store transform.")
    parser.add_argument("files", nargs="+", help="raw .jsonl.zst files")
    parser.add_argument("--engine", choices=["spark", "duckdb"], help=argparse.SUPPRESS)
    parser.add_argument("--out", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--stats-file", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--runs", type=int, default=2, help="runs per engine (default: 2)")
    args = parser.parse_args()
    files = [str(Path(f).resolve()) for f in args.files]

    if args.engine:
        stats = run_spark(files, args.out) if args.engine == "spark" else run_duckdb(files, args.out)
        args.stats_file.write_text(json.dumps(stats))
        return

    results = []
    with tempfile.TemporaryDirectory(prefix="compare-engines-") as tmp:
        for _ in range(args.runs):
            for engine in ("duckdb", "spark"):
                result = measure(engine, files, Path(tmp))
                results.append(result)
                print(json.dumps(result), flush=True)

    same = {(r["rows"], r["sum_time_us"], r["accounts"]) for r in results}
    print("Outputs match" if len(same) == 1 else "OUTPUTS DIFFER", flush=True)
    sys.exit(0 if len(same) == 1 else 1)


if __name__ == "__main__":
    main()
