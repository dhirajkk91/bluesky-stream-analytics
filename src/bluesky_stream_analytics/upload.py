"""
Upload finished hourly files to S3, then delete the local copy.

Each file keeps its partition path under raw/, for example
raw/events/date=2026-10-24/hour=16/<name>.jsonl.zst. Uploads carry a SHA-256
checksum that S3 verifies, and only succeed if the key doesn't exist yet.

Usage:
    uv run upload --bucket <bucket name>
    uv run upload --bucket <bucket name> --data-dir /path/to/data
"""

import argparse
import base64
import hashlib
import logging
import sys
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from bluesky_stream_analytics.writer import FINAL_SUFFIX

log = logging.getLogger(__name__)

KEY_PREFIX = "raw"


def finished_files(data_dir: Path) -> list[Path]:
    """Return every finished .jsonl.zst file under data_dir, oldest partition first."""
    events_dir = data_dir / "events"
    if not events_dir.exists():
        return []
    return sorted(events_dir.rglob(f"*{FINAL_SUFFIX}"))


def object_key(data_dir: Path, path: Path) -> str:
    """Return the S3 key for a local file: raw/ followed by its path under data_dir."""
    return f"{KEY_PREFIX}/{path.relative_to(data_dir).as_posix()}"


def sha256_base64(path: Path) -> str:
    """Return the file's SHA-256 digest, base64-encoded as S3 expects."""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            digest.update(chunk)
    return base64.b64encode(digest.digest()).decode()


def upload_file(s3, bucket: str, data_dir: Path, path: Path) -> bool:
    """
    Upload one file, then delete it locally.

    Returns True if it was uploaded now, False if S3 already had it (an earlier
    run uploaded it but stopped before deleting the local copy).
    """
    key = object_key(data_dir, path)
    with open(path, "rb") as body:
        try:
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=body,
                ChecksumSHA256=sha256_base64(path),
                IfNoneMatch="*",
            )
            uploaded = True
        except ClientError as exc:
            if not _already_exists(exc):
                raise
            uploaded = False

    path.unlink()
    return uploaded


def _already_exists(exc: ClientError) -> bool:
    error = exc.response.get("Error", {})
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return error.get("Code") == "PreconditionFailed" or status == 412


def upload_all(s3, bucket: str, data_dir: Path) -> int:
    """Upload every finished file. Returns the number of files that failed."""
    failed = 0
    for path in finished_files(data_dir):
        size = path.stat().st_size
        try:
            uploaded = upload_file(s3, bucket, data_dir, path)
        except (ClientError, BotoCoreError, OSError) as exc:
            failed += 1
            log.error("Failed %s: %s", path.relative_to(data_dir), exc)
            continue
        status = "Uploaded" if uploaded else "Already in S3, removed local copy of"
        log.info("%s %s (%s bytes)", status, path.relative_to(data_dir), f"{size:,}")
    return failed


def main() -> None:
    """Command-line entry point. Exits non-zero if any file failed to upload."""
    parser = argparse.ArgumentParser(description="Upload finished hourly files to S3.")
    parser.add_argument("--bucket", required=True, help="destination S3 bucket")
    parser.add_argument("--data-dir", type=Path, default=Path("data"), help="collector data folder (default: ./data)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    failed = upload_all(boto3.client("s3"), args.bucket, args.data_dir)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
