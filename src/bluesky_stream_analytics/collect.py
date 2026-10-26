"""
Collect every Jetstream event into hourly raw files and resume after restarts without gaps.

The asyncio loop only reads from the connection and queues each event. A writer
thread takes events off the queue, writes them to the hourly files, syncs them
to disk and saves the cursor, so a slow disk never holds up reading.
"""

import argparse
import asyncio
import json
import logging
import queue
import random
import signal
import threading
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
SLOW_SYNC_SECONDS = 1.0
LOOP_CHECK_SECONDS = 0.5
LOOP_PAUSE_WARNING_SECONDS = 2.0
# About 7 minutes of events at the usual rate, roughly 150 MB.
QUEUE_MAX_EVENTS = 200_000
QUEUE_FULL_RETRY_SECONDS = 0.1

_END = object()  # tells the writer thread that no more events are coming


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
    """Reads Jetstream on the asyncio loop and hands events to a writer thread that owns the files."""

    def __init__(self, data_dir: Path, url: str = JETSTREAM_URL) -> None:
        self.url = url
        self.cursor_path = data_dir / "state" / "cursor.json"
        self.writer = HourlyFileWriter(data_dir)
        self.data_dir = data_dir
        self._stop = asyncio.Event()
        self._attempt = 0

        self._queue: queue.Queue = queue.Queue(maxsize=QUEUE_MAX_EVENTS)
        self._writer_thread = threading.Thread(target=self._write_forever, name="writer", daemon=True)
        self._writer_done = asyncio.Event()
        self._writer_error: BaseException | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._last_received_time_us: int | None = None

        # Status counters, reset every STATUS_INTERVAL_SECONDS.
        self._events_since_status = 0
        self._skipped_since_status = 0
        self._longest_queue = 0
        self._longest_sync = 0.0
        self._longest_pause = 0.0
        self._queue_full_logged = False

    def stop(self) -> None:
        """Request a clean shutdown."""
        self._stop.set()

    @property
    def last_received_time_us(self) -> int | None:
        """Newest time_us read from the connection, written to disk or not."""
        return self._last_received_time_us

    async def run(self) -> None:
        """Recover leftover files, stream until stopped, then write what's queued and save the cursor."""
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

        self._loop = asyncio.get_running_loop()
        self._writer_thread.start()

        tasks = [
            asyncio.create_task(self._stream_forever(resume_from)),
            asyncio.create_task(self._report_status()),
            asyncio.create_task(self._watch_loop()),
            asyncio.create_task(self._stop.wait()),
            asyncio.create_task(self._writer_done.wait()),
        ]
        try:
            # Returns on a stop request, when the writer thread stops, or when a task fails.
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            # The writer thread writes everything still queued, closes the file and saves the cursor.
            await asyncio.to_thread(self._close_writer)

        if self._writer_error is not None:
            raise self._writer_error
        for task in done:
            if not task.cancelled() and task.exception() is not None:
                raise task.exception()

    async def _stream_forever(self, resume_from: int | None) -> None:
        """Keep a connection open, reconnecting with backoff when it drops."""
        while True:
            # Queued events will still be written, so resume after the newest one received.
            resume_from = self._last_received_time_us or resume_from
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
        """Queue events from one connection until it closes."""
        async with connect(url) as ws:
            self._attempt = 0
            log.info("Connected to %s", url)
            async for message in ws:
                if self._queue.full():
                    await self._wait_for_room()
                self._handle(message)

    def _handle(self, message: str) -> None:
        try:
            time_us = int(json.loads(message)["time_us"])
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            self._skipped_since_status += 1
            return
        # Only this loop adds to the queue, so after the check above there is room.
        self._queue.put_nowait((message, time_us))
        self._last_received_time_us = time_us
        self._events_since_status += 1
        self._longest_queue = max(self._longest_queue, self._queue.qsize())

    async def _wait_for_room(self) -> None:
        """Stop reading until the writer thread has caught up a little. Jetstream holds the events meanwhile."""
        if not self._queue_full_logged:
            log.warning(
                "Write queue is full (%s events); pausing reads until the disk catches up", f"{QUEUE_MAX_EVENTS:,}"
            )
            self._queue_full_logged = True
        while self._queue.full():
            await asyncio.sleep(QUEUE_FULL_RETRY_SECONDS)

    def _write_forever(self) -> None:
        """
        The writer thread: write queued events, and every SYNC_INTERVAL_SECONDS sync
        them to disk and save the cursor. On the end marker or an error, close the
        current file and save the final cursor.
        """
        try:
            next_sync = time.monotonic() + SYNC_INTERVAL_SECONDS
            while True:
                try:
                    item = self._queue.get(timeout=max(0.0, next_sync - time.monotonic()))
                except queue.Empty:
                    item = None
                if item is _END:
                    break
                if item is not None:
                    self.writer.write(*item)
                if time.monotonic() >= next_sync:
                    self._sync()
                    next_sync = time.monotonic() + SYNC_INTERVAL_SECONDS
        except BaseException as exc:
            self._writer_error = exc
        finally:
            try:
                self.writer.close()
            except BaseException as exc:
                self._writer_error = self._writer_error or exc
            finally:
                self._save_final_cursor()
                self._loop.call_soon_threadsafe(self._writer_done.set)

    def _sync(self) -> None:
        """Stop on a failed compression, make the written events durable, then save the cursor."""
        self.writer.check()
        started = time.monotonic()
        time_us = self.writer.sync()
        if time_us is not None:
            save_cursor(self.cursor_path, time_us)
        took = time.monotonic() - started
        self._longest_sync = max(self._longest_sync, took)
        if took >= SLOW_SYNC_SECONDS:
            log.warning("Sync to disk took %.1fs", took)

    def _save_final_cursor(self) -> None:
        final_time_us = self.writer.last_synced_time_us
        if final_time_us is None:
            log.info("Stopped. No events were written.")
        else:
            save_cursor(self.cursor_path, final_time_us)
            log.info("Stopped. Cursor saved at %d", final_time_us)

    def _close_writer(self) -> None:
        """Send the end marker and wait for the writer thread. Runs off the loop, as it can block."""
        while self._writer_thread.is_alive():
            try:
                self._queue.put(_END, timeout=0.1)
                break
            except queue.Full:
                continue
        self._writer_thread.join()

    async def _report_status(self) -> None:
        last_status = time.monotonic()
        while True:
            await asyncio.sleep(STATUS_INTERVAL_SECONDS)
            now = time.monotonic()
            self._log_status(now - last_status)
            last_status = now

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
        def ago(time_us: int | None) -> str:
            return "n/a" if time_us is None else f"{time.time() - time_us / 1e6:.1f}s"

        log.info(
            "%s events in %.0fs (%.1f/s), %s behind live, queue max %s, on disk up to %s ago, "
            "longest sync %.2fs, longest pause %.2fs%s",
            f"{self._events_since_status:,}",
            elapsed,
            self._events_since_status / elapsed,
            ago(self._last_received_time_us),
            f"{self._longest_queue:,}",
            ago(self.writer.last_synced_time_us),
            self._longest_sync,
            self._longest_pause,
            f", {self._skipped_since_status} unreadable skipped" if self._skipped_since_status else "",
        )
        self._events_since_status = 0
        self._skipped_since_status = 0
        self._longest_queue = self._queue.qsize()
        self._longest_sync = 0.0
        self._longest_pause = 0.0
        self._queue_full_logged = False


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
