"""
Download raw files from S3 into uploaded/, so `process` loads them.

Used to backfill or rebuild the event store from the raw files S3 still
keeps. Files already waiting locally are skipped, and loading an event twice
adds nothing, so it's safe to run for hours that were processed before.

Usage:
    uv run fetch-raw --bucket <bucket name> --date 2026-10-24
    uv run fetch-raw --bucket <bucket name> --date 2026-10-24 --hour 18
"""

import argparse
import logging
import os
from datetime import date
from pathlib import Path

import boto3

from bluesky_stream_analytics.upload import KEY_PREFIX, UPLOADED_DIR
from bluesky_stream_analytics.writer import FINAL_SUFFIX

log = logging.getLogger(__name__)


def raw_prefix(day: date, hour: int | None = None) -> str:
    """S3 prefix for one day of raw files, or one hour of it."""
    prefix = f"{KEY_PREFIX}/events/date={day.isoformat()}/"
    return prefix if hour is None else f"{prefix}hour={hour:02d}/"


def local_path(data_dir: Path, key: str) -> Path:
    """Where a raw object goes locally: its key without raw/, under uploaded/."""
    return data_dir / UPLOADED_DIR / key.removeprefix(f"{KEY_PREFIX}/")


def fetch(s3, bucket: str, data_dir: Path, prefix: str) -> int:
    """Download every finished file under the prefix that isn't already waiting locally. Returns how many were downloaded."""
    downloaded = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            target = local_path(data_dir, key)
            if not key.endswith(FINAL_SUFFIX) or target.exists():
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            partial = target.with_name(target.name + ".part")
            s3.download_file(bucket, key, str(partial))
            os.replace(partial, target)
            downloaded += 1
            log.info("Downloaded %s (%s bytes)", key, f"{obj['Size']:,}")
    return downloaded


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Download raw files from S3 for processing.")
    parser.add_argument("--bucket", required=True, help="bucket with the raw files")
    parser.add_argument("--date", required=True, type=date.fromisoformat, help="day to fetch, yyyy-mm-dd (UTC)")
    parser.add_argument("--hour", type=int, choices=range(24), metavar="0-23", help="only this hour")
    parser.add_argument("--data-dir", type=Path, default=Path("data"), help="collector data folder (default: ./data)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    count = fetch(boto3.client("s3"), args.bucket, args.data_dir, raw_prefix(args.date, args.hour))
    log.info("Downloaded %d files", count)


if __name__ == "__main__":
    main()
