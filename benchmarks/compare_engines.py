"""
Run the event store's transform on the same raw files with Spark and with DuckDB.

Both engines do the same work: read the zstd JSON lines, parse the fields the
event store keeps, drop repeats on the event key, and write Parquet with zstd.
Each engine runs in its own process, so its startup, time and peak memory are
measured separately, and both outputs are checked to hold the same events.
The DuckDB side is the pipeline's own SQL; the Spark side is spark_transform.py.
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


def run_duckdb(files: list[str], out: Path) -> dict:
    """Returns this process's peak memory and CPU time."""
    import duckdb

    from bluesky_stream_analytics.event_store import stage_batch

    con = duckdb.connect()
    con.execute(f"SET threads = {THREADS}")
    con.execute("SET TimeZone = 'UTC'")
    stage_batch(con, files)
    con.execute(
        f"COPY (SELECT * EXCLUDE (source_file) FROM batch ORDER BY collection, did, event_time) "
        f"TO '{out / 'events.parquet'}' (FORMAT parquet, COMPRESSION zstd)"
    )
    return _usage("self")


def run_spark(files: list[str], out: Path) -> dict:
    """Returns the peak memory and CPU time of the Spark JVM plus this Python process."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    from spark_transform import distinct_events, parse_events

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
    lines = spark.read.text(files).repartition(spark.sparkContext.defaultParallelism * 2)
    events = distinct_events(parse_events(lines))
    events = events.sortWithinPartitions("collection", "did", "event_time")
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
        .execute(f"SELECT count(*), sum(epoch_us(event_time)), count(DISTINCT did) FROM read_parquet('{out}/*.parquet')")
        .fetchone()
    )
    return {"rows": row[0], "sum_time_us": row[1], "accounts": row[2]}


def rows_in_only_one(a: Path, b: Path) -> int:
    """How many rows appear in one engine's output but not the other's, counting repeats."""
    import duckdb

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    left, right = f"read_parquet('{a}/*.parquet')", f"read_parquet('{b}/*.parquet')"
    return con.execute(
        f"SELECT (SELECT count(*) FROM (FROM {left} EXCEPT ALL FROM {right}))"
        f" + (SELECT count(*) FROM (FROM {right} EXCEPT ALL FROM {left}))"
    ).fetchone()[0]


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
        differing = rows_in_only_one(Path(tmp) / "duckdb", Path(tmp) / "spark")

    print("Outputs identical, row for row" if differing == 0 else f"OUTPUTS DIFFER in {differing:,} rows", flush=True)
    sys.exit(0 if differing == 0 else 1)


if __name__ == "__main__":
    main()
