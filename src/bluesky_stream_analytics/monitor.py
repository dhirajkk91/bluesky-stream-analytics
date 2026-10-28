"""
Alerts by email when something breaks, through healthchecks.io.

Every scheduled job pings its check on healthchecks.io when it succeeds, and a
job that fails pings the check's /fail URL with the end of its log (through
bluesky-alert@.service). healthchecks.io emails when a check fails, or when a
ping doesn't arrive on schedule, which also covers the server itself being down:
a dead server can't report on itself.

`check-health` runs every 10 minutes and looks at what only the server can see:
how old the newest collected event is, whether finished files are waiting too
long to be uploaded or processed, and how full the disk is. It pings its own
check with what it found, so an alert says what's wrong.

Pings use the project's ping key and the check's name (its slug), from
HC_PING_KEY. Without a key, pings are skipped and only logged.
"""

import argparse
import logging
import os
import shutil
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path

from bluesky_stream_analytics.cursor import load_cursor
from bluesky_stream_analytics.upload import UPLOADED_DIR, finished_files
from bluesky_stream_analytics.writer import FINAL_SUFFIX

log = logging.getLogger(__name__)

PING_BASE_URL = "https://hc-ping.com"
CHECK_SLUG = "bluesky-check"
MAX_MESSAGE_BYTES = 10_000  # the end of a log is what matters; healthchecks.io keeps up to 100 KB

NEWEST_EVENT_LIMIT_SECONDS = 5 * 60  # the collector syncs every 5 seconds
UPLOAD_WAIT_LIMIT_SECONDS = 30 * 60  # uploads run every 10 minutes
PROCESS_WAIT_LIMIT_SECONDS = 2 * 60 * 60  # processing runs hourly
DISK_USED_LIMIT = 0.80

Post = Callable[[str, bytes], None]


def post_body(url: str, body: bytes, timeout: float = 10) -> None:
    """POST a body, raising on any HTTP or network error."""
    request = urllib.request.Request(url, data=body, method="POST", headers={"Content-Type": "text/plain"})
    with urllib.request.urlopen(request, timeout=timeout):
        pass


def slug_for(name: str) -> str:
    """The check's slug for a check or unit name: bluesky-process.service is bluesky-process."""
    return name.removesuffix(".service")


def ping_url(key: str, slug: str, fail: bool = False, base_url: str = PING_BASE_URL) -> str:
    return f"{base_url.rstrip('/')}/{key}/{slug_for(slug)}{'/fail' if fail else ''}"


def ping(
    slug: str,
    fail: bool = False,
    message: str = "",
    key: str | None = None,
    post: Post = post_body,
    attempts: int = 3,
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    """
    Ping a check, with an optional message. Returns whether it was delivered.
    Never raises: a monitoring outage mustn't fail the job being monitored.
    """
    key = key if key is not None else os.environ.get("HC_PING_KEY", "")
    if not key:
        log.info("No HC_PING_KEY, not pinging %s", slug_for(slug))
        return False
    url = ping_url(key, slug, fail)
    body = message.encode()[-MAX_MESSAGE_BYTES:]
    for attempt in range(attempts):
        try:
            post(url, body)
            return True
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            log.warning("Ping to %s failed (%s), attempt %d of %d", slug_for(slug), error, attempt + 1, attempts)
            if attempt + 1 < attempts:
                sleep(2**attempt)
    return False


def _minutes(seconds: float) -> str:
    return f"{seconds / 60:.0f} min"


def _oldest_age(paths: list[Path], now: float) -> float | None:
    ages = []
    for path in paths:
        try:
            ages.append(now - path.stat().st_mtime)
        except FileNotFoundError:  # uploaded or processed while we looked
            pass
    return max(ages, default=None)


def find_problems(
    data_dir: Path,
    now: float | None = None,
    disk_usage: Callable[[Path], tuple] = shutil.disk_usage,
) -> tuple[list[str], str]:
    """What's wrong on the server, as messages, and a one-line summary of what was measured."""
    now = time.time() if now is None else now
    problems: list[str] = []
    summary: list[str] = []

    try:
        newest_us = load_cursor(data_dir / "state" / "cursor.json")
    except ValueError as error:
        newest_us = None
        problems.append(f"The collector's cursor can't be read: {error}")
    if newest_us is None:
        if not problems:
            problems.append("The collector hasn't saved a cursor yet")
    else:
        lag = now - newest_us / 1_000_000
        summary.append(f"newest event {lag:.0f}s old")
        if lag > NEWEST_EVENT_LIMIT_SECONDS:
            problems.append(
                f"The newest event is {_minutes(lag)} old (limit {_minutes(NEWEST_EVENT_LIMIT_SECONDS)}): "
                "the collector or Jetstream has stalled"
            )

    waiting_upload = finished_files(data_dir)
    upload_age = _oldest_age(waiting_upload, now)
    summary.append(f"{len(waiting_upload)} files to upload")
    if upload_age is not None and upload_age > UPLOAD_WAIT_LIMIT_SECONDS:
        problems.append(
            f"{len(waiting_upload)} finished files are waiting to be uploaded, the oldest for {_minutes(upload_age)} "
            f"(limit {_minutes(UPLOAD_WAIT_LIMIT_SECONDS)})"
        )

    uploaded_dir = data_dir / UPLOADED_DIR
    waiting_process = sorted(uploaded_dir.rglob(f"*{FINAL_SUFFIX}")) if uploaded_dir.exists() else []
    process_age = _oldest_age(waiting_process, now)
    summary.append(f"{len(waiting_process)} files to process")
    if process_age is not None and process_age > PROCESS_WAIT_LIMIT_SECONDS:
        problems.append(
            f"{len(waiting_process)} uploaded files are waiting to be processed, the oldest for "
            f"{_minutes(process_age)} (limit {_minutes(PROCESS_WAIT_LIMIT_SECONDS)})"
        )

    total, used, _free = disk_usage(data_dir)
    share = used / total
    summary.append(f"disk {share:.0%} used")
    if share > DISK_USED_LIMIT:
        problems.append(f"The disk is {share:.0%} full (limit {DISK_USED_LIMIT:.0%})")

    return problems, ", ".join(summary)


def run_check(data_dir: Path, **ping_options) -> int:
    """Check the server, ping the result, and return the exit code: 1 if anything is wrong."""
    problems, summary = find_problems(data_dir)
    if problems:
        for problem in problems:
            log.error(problem)
        ping(CHECK_SLUG, fail=True, message="\n".join([*problems, "", summary]), **ping_options)
        return 1
    log.info("All good: %s", summary)
    ping(CHECK_SLUG, message=summary, **ping_options)
    return 0


def _logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def check_main() -> None:
    """Command-line entry point for check-health."""
    parser = argparse.ArgumentParser(description="Check the collector, the file backlog and the disk, and ping the result.")
    parser.add_argument("--data-dir", type=Path, default=Path("data"), help="collector data folder (default: ./data)")
    args = parser.parse_args()
    _logging()
    sys.exit(run_check(args.data_dir))


def ping_main() -> None:
    """Command-line entry point for ping-check. Always exits 0."""
    parser = argparse.ArgumentParser(description="Ping a check on healthchecks.io.")
    parser.add_argument("check", help="the check's slug, or a unit name like bluesky-process.service")
    parser.add_argument("--fail", action="store_true", help="report a failure")
    parser.add_argument("--stdin", action="store_true", help="send what's on standard input as the message")
    args = parser.parse_args()
    _logging()
    message = sys.stdin.read() if args.stdin else ""
    ping(args.check, fail=args.fail, message=message)
