"""
Write raw events to hourly JSON Lines files, compressed with zstd once each hour is finished.

Layout: <root>/events/date=YYYY-MM-DD/hour=HH/<first time_us>_<session>.jsonl.zst
The active hour is an uncompressed .jsonl.open file, so a crash can only damage its last line.
Finished hours are compressed on a background thread so writing never waits for it.
"""

import json
import logging
import os
import secrets
import shutil
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from compression import zstd

from bluesky_stream_analytics.fs import fsync_directory

log = logging.getLogger(__name__)

OPEN_SUFFIX = ".jsonl.open"
FINAL_SUFFIX = ".jsonl.zst"
TMP_SUFFIX = ".jsonl.zst.tmp"


def hour_of(time_us: int) -> datetime:
    """Return the UTC hour containing time_us."""
    moment = datetime.fromtimestamp(time_us / 1_000_000, tz=UTC)
    return moment.replace(minute=0, second=0, microsecond=0)


def partition_dir(root: Path, hour: datetime) -> Path:
    """Return the folder for one hour."""
    return root / "events" / f"date={hour:%Y-%m-%d}" / f"hour={hour:%H}"


class HourlyFileWriter:
    """Appends events to the current hour's file and rolls over when a later hour starts."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.session_id = secrets.token_hex(4)
        self._hour: datetime | None = None
        self._path: Path | None = None
        self._file = None
        self._last_written_time_us: int | None = None
        self._last_synced_time_us: int | None = None
        self._compressor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="compress")
        self._pending: list[Future] = []

    def write(self, raw: str, time_us: int) -> None:
        """Append one raw event. Events from an earlier hour go into the current file."""
        hour = hour_of(time_us)
        if self._hour is None or hour > self._hour:
            self._finish_current_file()
            self._start_file(hour, time_us)

        if "\n" in raw:  # keep one event per line
            raw = json.dumps(json.loads(raw), separators=(",", ":"))
        self._file.write(raw.encode() + b"\n")
        self._last_written_time_us = time_us

    def sync(self) -> int | None:
        """Flush to disk and return the newest time_us that is now safe to save as the cursor."""
        if self._file is not None:
            self._file.flush()
            os.fsync(self._file.fileno())
        self._last_synced_time_us = self._last_written_time_us
        return self._last_synced_time_us

    def check(self) -> None:
        """Raise the error of any background compression that has failed."""
        running = []
        for future in self._pending:
            if future.done():
                future.result()
            else:
                running.append(future)
        self._pending = running

    def drain(self) -> None:
        """Wait for all background compression to finish, raising the first failure."""
        pending, self._pending = self._pending, []
        for future in pending:
            future.result()

    def close(self) -> int | None:
        """Finish the current file, wait for compression, and return the newest time_us on disk."""
        try:
            self._finish_current_file()
            self.drain()
        finally:
            self._compressor.shutdown(wait=True)
        return self._last_synced_time_us

    @property
    def last_written_time_us(self) -> int | None:
        """Newest time_us written, synced or not."""
        return self._last_written_time_us

    @property
    def last_synced_time_us(self) -> int | None:
        """Newest time_us known to be on disk."""
        return self._last_synced_time_us

    def _start_file(self, hour: datetime, first_time_us: int) -> None:
        directory = partition_dir(self.root, hour)
        directory.mkdir(parents=True, exist_ok=True)
        self._hour = hour
        self._path = directory / f"{first_time_us}_{self.session_id}{OPEN_SUFFIX}"
        self._file = open(self._path, "ab")
        fsync_directory(directory)
        log.info("Started %s", self._path.relative_to(self.root))

    def _finish_current_file(self) -> None:
        if self._file is None:
            return
        self._file.flush()
        os.fsync(self._file.fileno())
        self._file.close()
        self._file = None
        self._last_synced_time_us = self._last_written_time_us
        self._pending.append(self._compressor.submit(self._compress, self._path))

    def _compress(self, open_path: Path) -> None:
        finalize_open_file(open_path)
        log.info("Finished %s", final_path_for(open_path).relative_to(self.root))


def final_path_for(open_path: Path) -> Path:
    """Map x.jsonl.open to x.jsonl.zst."""
    return open_path.with_name(open_path.name.removesuffix(OPEN_SUFFIX) + FINAL_SUFFIX)


def finalize_open_file(open_path: Path) -> Path | None:
    """Compress an .open file into its final .zst file and delete it. Empty files are just removed."""
    final_path = final_path_for(open_path)
    if final_path.exists():
        raise FileExistsError(f"Refusing to overwrite finished file {final_path}")

    if open_path.stat().st_size == 0:
        open_path.unlink()
        return None

    tmp_path = open_path.with_name(open_path.name.removesuffix(OPEN_SUFFIX) + TMP_SUFFIX)
    with open(open_path, "rb") as src, zstd.open(tmp_path, "wb") as dst:
        shutil.copyfileobj(src, dst, length=1024 * 1024)
    with open(tmp_path, "rb") as f:
        os.fsync(f.fileno())

    os.replace(tmp_path, final_path)
    fsync_directory(final_path.parent)
    open_path.unlink()
    return final_path


@dataclass
class Recovery:
    """Result of recover_open_files()."""

    files: list[Path] = field(default_factory=list)
    newest_time_us: int | None = None


def recover_open_files(root: Path) -> Recovery:
    """Finish files left behind by a crash and report the newest time_us recovered."""
    recovery = Recovery()
    events_dir = root / "events"
    if not events_dir.exists():
        return recovery

    for tmp_path in events_dir.rglob(f"*{TMP_SUFFIX}"):
        tmp_path.unlink()
        log.warning("Removed incomplete %s", tmp_path.relative_to(root))

    for open_path in sorted(events_dir.rglob(f"*{OPEN_SUFFIX}")):
        if final_path_for(open_path).exists():
            open_path.unlink()
            continue

        dropped = truncate_partial_last_line(open_path)
        newest = _time_us_of_last_line(open_path)
        final_path = finalize_open_file(open_path)
        if final_path is not None:
            recovery.files.append(final_path)
        if newest is not None:
            recovery.newest_time_us = max(newest, recovery.newest_time_us or newest)
        log.warning(
            "Recovered %s (dropped %d bytes of a partial line)",
            open_path.relative_to(root),
            dropped,
        )
    return recovery


def _time_us_of_last_line(path: Path) -> int | None:
    """Return the time_us of the last line in a JSON Lines file."""
    size = path.stat().st_size
    if size == 0:
        return None
    with open(path, "rb") as f:
        start = max(0, size - 1024 * 1024)
        f.seek(start)
        tail = f.read()
    last_line = tail.rstrip(b"\n").rsplit(b"\n", 1)[-1]
    try:
        return int(json.loads(last_line)["time_us"])
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None


def truncate_partial_last_line(path: Path) -> int:
    """Cut a file back to its last newline and return the number of bytes removed."""
    size = path.stat().st_size
    chunk_size = 64 * 1024

    with open(path, "r+b") as f:
        end = size
        while end > 0:
            start = max(0, end - chunk_size)
            f.seek(start)
            chunk = f.read(end - start)
            newline = chunk.rfind(b"\n")
            if newline != -1:
                keep = start + newline + 1
                break
            end = start
        else:
            keep = 0

        f.truncate(keep)
        f.flush()
        os.fsync(f.fileno())

    return size - keep
