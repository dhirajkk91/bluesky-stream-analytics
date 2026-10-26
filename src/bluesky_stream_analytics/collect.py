"""
Collect every Jetstream event into hourly raw files and resume after restarts without gaps.

Usage:
    uv run collect
    uv run collect --data-dir /path/to/data
"""

import argparse
import asyncio
import json
import logging
import os
import random
import signal
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from bluesky_stream_analytics.cursor import load_cursor, save_cursor
from bluesky_stream_analytics.writer import HourlyFileWriter, recover_open_files

log = logging.getLogger(__name__)

JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe"
REWIND_SECONDS = 5
SYNC_INTERVAL_SECONDS = 5
STATUS_INTERVAL_SECONDS = 60
MAX_BACKOFF_SECONDS = 60
SLOW_SYNC_SECONDS = 1.0
LOOP_CHECK_SECONDS = 0.5
LOOP_PAUSE_WARNING_SECONDS = 2.0


def build_url(base_url: str, resume_from_us: int | None) -> str:
    """Return the subscribe URL, starting REWIND_SECONDS before the resume point (or live if None)."""
    if resume_from_us is None:
        return base_url
    return f"{base_url}?cursor={resume_from_us - REWIND_SECONDS * 1_000_000}"


def backoff_delay(attempt: int) -> float:
    """Seconds to wait before a reconnect: doubles per attempt up to MAX_BACKOFF_SECONDS, with jitter."""
    ceiling = min(MAX_BACKOFF_SECONDS, 2**attempt)
    return random.uniform(ceiling / 2, ceiling)


def fsync_and_save(fd: int | None, cursor_path: Path, time_us: int | None) -> None:
    """Make the written events durable, then save the cursor. Runs on the sync thread."""
    if fd is not None:
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    if time_us is not None:
        save_cursor(cursor_path, time_us)


class Collector:
    """Streams events into an HourlyFileWriter and keeps the cursor up to date."""

    def __init__(self, data_dir: Path, url: str = JETSTREAM_URL) -> None:
        self.url = url
        self.cursor_path = data_dir / "state" / "cursor.json"
        self.writer = HourlyFileWriter(data_dir)
        self.data_dir = data_dir
        self._stop = asyncio.Event()
        self._attempt = 0
        self._events_since_status = 0
        self._skipped_since_status = 0
        self._longest_sync = 0.0
        self._longest_pause = 0.0
        # Disk syncs run here so a slow disk never holds up reading the connection.
        self._syncer = ThreadPoolExecutor(max_workers=1, thread_name_prefix="sync")

    def stop(self) -> None:
        """Request a clean shutdown."""
        self._stop.set()

    async def run(self) -> None:
        """Recover leftover files, stream until stopped, then finish the current file and save the cursor."""
        recovery = recover_open_files(self.data_dir)
        if recovery.files:
            log.warning("Recovered %d file(s) from an unclean shutdown", len(recovery.files))

        # Recovered files can hold events newer than the last cursor save.
        saved = load_cursor(self.cursor_path)
        candidates = [t for t in (saved, recovery.newest_time_us) if t is not None]
        resume_from = max(candidates) if candidates else None

        if resume_from is None:
            log.info("No saved cursor; starting from the live edge")
        else:
            log.info("Resuming from %d (%ds rewind)", resume_from, REWIND_SECONDS)

        tasks = [
            asyncio.create_task(self._stream_forever(resume_from)),
            asyncio.create_task(self._housekeeping()),
            asyncio.create_task(self._watch_loop()),
            asyncio.create_task(self._stop.wait()),
        ]
        try:
            # Returns on a stop request or when a task fails.
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            # A sync still running on its thread must finish before the final cursor save.
            self._syncer.shutdown(wait=True)

            try:
                self.writer.close()
            finally:
                final_time_us = self.writer.last_synced_time_us
                if final_time_us is None:
                    log.info("Stopped. No events were written.")
                else:
                    save_cursor(self.cursor_path, final_time_us)
                    log.info("Stopped. Cursor saved at %d", final_time_us)

        for task in done:
            if not task.cancelled() and task.exception() is not None:
                raise task.exception()

    async def _stream_forever(self, resume_from: int | None) -> None:
        """Keep a connection open, reconnecting with backoff when it drops."""
        while True:
            resume_from = self.writer.last_written_time_us or resume_from
            try:
                await self._stream_once(build_url(self.url, resume_from))
                log.warning("Connection closed by server")
            except (OSError, WebSocketException) as exc:
                log.warning("Connection error: %s", exc)

            delay = backoff_delay(self._attempt)
            self._attempt += 1
            log.info("Reconnecting in %.1fs", delay)
            await asyncio.sleep(delay)

    async def _stream_once(self, url: str) -> None:
        """Write events from one connection until it closes."""
        async with connect(url) as ws:
            self._attempt = 0
            log.info("Connected to %s", url)
            async for message in ws:
                self._handle(message)

    def _handle(self, message: str) -> None:
        try:
            time_us = int(json.loads(message)["time_us"])
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            self._skipped_since_status += 1
            return
        self.writer.write(message, time_us)
        self._events_since_status += 1

    async def _housekeeping(self) -> None:
        """
        Every SYNC_INTERVAL_SECONDS: stop on a failed compression, make the written
        events durable on the sync thread, then save the cursor. Logs status every
        STATUS_INTERVAL_SECONDS.
        """
        loop = asyncio.get_running_loop()
        last_status = time.monotonic()
        while True:
            await asyncio.sleep(SYNC_INTERVAL_SECONDS)
            self.writer.check()

            fd, time_us = self.writer.start_sync()
            started = time.monotonic()
            await loop.run_in_executor(self._syncer, fsync_and_save, fd, self.cursor_path, time_us)
            took = time.monotonic() - started
            self.writer.mark_synced(time_us)
            self._longest_sync = max(self._longest_sync, took)
            if took >= SLOW_SYNC_SECONDS:
                log.warning("Sync to disk took %.1fs", took)

            elapsed = time.monotonic() - last_status
            if elapsed >= STATUS_INTERVAL_SECONDS:
                self._log_status(elapsed)
                last_status = time.monotonic()

    async def _watch_loop(self) -> None:
        """Measure how late the event loop wakes up: that's how long something blocked it."""
        while True:
            started = time.monotonic()
            await asyncio.sleep(LOOP_CHECK_SECONDS)
            pause = time.monotonic() - started - LOOP_CHECK_SECONDS
            self._longest_pause = max(self._longest_pause, pause)
            if pause >= LOOP_PAUSE_WARNING_SECONDS:
                log.warning("Event loop was blocked for %.1fs", pause)

    def _log_status(self, elapsed: float) -> None:
        synced_time_us = self.writer.last_synced_time_us
        lag = "n/a" if synced_time_us is None else f"{time.time() - synced_time_us / 1e6:.1f}s"
        log.info(
            "%s events in %.0fs (%.1f/s), %s behind live, longest sync %.2fs, longest pause %.2fs%s",
            f"{self._events_since_status:,}",
            elapsed,
            self._events_since_status / elapsed,
            lag,
            self._longest_sync,
            self._longest_pause,
            f", {self._skipped_since_status} unreadable skipped" if self._skipped_since_status else "",
        )
        self._events_since_status = 0
        self._skipped_since_status = 0
        self._longest_sync = 0.0
        self._longest_pause = 0.0


async def _run(data_dir: Path, url: str) -> None:
    collector = Collector(data_dir, url)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, collector.stop)
    await collector.run()


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Collect Bluesky Jetstream events to hourly files.")
    parser.add_argument("--data-dir", type=Path, default=Path("data"), help="output folder (default: ./data)")
    parser.add_argument("--url", default=JETSTREAM_URL, help="Jetstream subscribe URL")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    asyncio.run(_run(args.data_dir, args.url))


if __name__ == "__main__":
    main()
