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
import random
import signal
import time
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


def build_url(base_url: str, resume_from_us: int | None) -> str:
    """Return the subscribe URL, starting REWIND_SECONDS before the resume point (or live if None)."""
    if resume_from_us is None:
        return base_url
    return f"{base_url}?cursor={resume_from_us - REWIND_SECONDS * 1_000_000}"


def backoff_delay(attempt: int) -> float:
    """Seconds to wait before a reconnect: doubles per attempt up to MAX_BACKOFF_SECONDS, with jitter."""
    ceiling = min(MAX_BACKOFF_SECONDS, 2**attempt)
    return random.uniform(ceiling / 2, ceiling)


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
            asyncio.create_task(self._stop.wait()),
        ]
        try:
            # Returns on a stop request or when a task fails.
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

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
        """Sync data and save the cursor every SYNC_INTERVAL_SECONDS, stop on a failed compression, log status every STATUS_INTERVAL_SECONDS."""
        last_status = time.monotonic()
        while True:
            await asyncio.sleep(SYNC_INTERVAL_SECONDS)
            self.writer.check()
            synced_time_us = self.writer.sync()
            if synced_time_us is not None:
                save_cursor(self.cursor_path, synced_time_us)

            elapsed = time.monotonic() - last_status
            if elapsed >= STATUS_INTERVAL_SECONDS:
                self._log_status(elapsed, synced_time_us)
                last_status = time.monotonic()

    def _log_status(self, elapsed: float, synced_time_us: int | None) -> None:
        lag = "n/a" if synced_time_us is None else f"{time.time() - synced_time_us / 1e6:.1f}s"
        log.info(
            "%s events in %.0fs (%.1f/s), %s behind live%s",
            f"{self._events_since_status:,}",
            elapsed,
            self._events_since_status / elapsed,
            lag,
            f", {self._skipped_since_status} unreadable skipped" if self._skipped_since_status else "",
        )
        self._events_since_status = 0
        self._skipped_since_status = 0


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
