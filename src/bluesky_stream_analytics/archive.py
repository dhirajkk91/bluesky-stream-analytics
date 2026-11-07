"""
Archive the Kafka topic into hourly raw files, the same files the collector writes.

Reads jetstream.events as the `archiver` consumer group and writes every event
with HourlyFileWriter. Every few seconds it syncs the files to disk and only then
commits its offsets, so a committed offset never points past an event that isn't
on disk. After a crash it finishes any half-written file and resumes from the last
commit. Events written after that commit are written again, and the event store
drops the repeats.
"""

import argparse
import json
import logging
import signal
import threading
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from bluesky_stream_analytics.kafka_sink import TIME_US_HEADER
from bluesky_stream_analytics.topic import BOOTSTRAP_SERVERS, TOPIC
from bluesky_stream_analytics.writer import HourlyFileWriter, recover_open_files

log = logging.getLogger(__name__)

GROUP_ID = "archiver"
SYNC_INTERVAL_SECONDS = 5
STATUS_INTERVAL_SECONDS = 60
BATCH_SIZE = 1_000
POLL_TIMEOUT_SECONDS = 0.5


def make_consumer(bootstrap_servers: str, group_id: str = GROUP_ID) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            # Offsets are committed by hand, only after the events are on disk.
            "enable.auto.commit": False,
            # A new group starts from the oldest event Kafka still has.
            "auto.offset.reset": "earliest",
            "client.id": "archiver",
        }
    )


def event_time_us(message) -> int:
    """The event's time_us, from the header the collector adds, or from the event itself."""
    for name, value in message.headers() or []:
        if name == TIME_US_HEADER:
            return int(value)
    return int(json.loads(message.value())["time_us"])


class Archiver:
    """Copies the topic into hourly files, committing each offset once its event is on disk."""

    def __init__(self, data_dir: Path, consumer: Consumer, topic: str = TOPIC) -> None:
        self.data_dir = data_dir
        self.consumer = consumer
        self.topic = topic
        self.writer = HourlyFileWriter(data_dir)
        self._stop = threading.Event()
        # Next offset to read in each partition, for events written but not yet committed.
        self._uncommitted: dict[int, int] = {}

        # Status counters, reset every STATUS_INTERVAL_SECONDS.
        self._events_since_status = 0
        self._skipped_since_status = 0

    def stop(self) -> None:
        """Request a clean shutdown: write what's been read, sync it, and commit."""
        self._stop.set()

    def run(self) -> None:
        """Archive until stopped. Raises if reading, writing or committing fails."""
        recovery = recover_open_files(self.data_dir)
        if recovery.files:
            log.warning("Recovered %d file(s) from an unclean shutdown", len(recovery.files))

        self.consumer.subscribe([self.topic], on_revoke=self._on_revoke)
        log.info("Reading %s as group %s", self.topic, GROUP_ID)
        next_sync = time.monotonic() + SYNC_INTERVAL_SECONDS
        last_status = time.monotonic()
        try:
            while not self._stop.is_set():
                for message in self.consumer.consume(BATCH_SIZE, timeout=POLL_TIMEOUT_SECONDS):
                    self._write(message)
                now = time.monotonic()
                if now >= next_sync:
                    self._sync_and_commit()
                    next_sync = now + SYNC_INTERVAL_SECONDS
                if now - last_status >= STATUS_INTERVAL_SECONDS:
                    self._log_status(now - last_status)
                    last_status = now
        finally:
            # Commit only if closing the files succeeded, so offsets never pass what's on disk.
            try:
                try:
                    self.writer.close()
                except BaseException:
                    # The files may not be on disk, so nothing more may be committed, not even
                    # by the revoke callback that closing the consumer triggers.
                    self._uncommitted.clear()
                    raise
                self._commit()
            finally:
                self.consumer.close()
                log.info("Stopped. Files on disk up to time_us %s", self.writer.last_synced_time_us)

    def _write(self, message) -> None:
        error = message.error()
        if error is not None:
            # The client retries passing problems, like a broker restart, by itself.
            if error.fatal():
                raise KafkaException(error)
            if error.code() != KafkaError._PARTITION_EOF:
                log.warning("Kafka reported: %s", error.str())
            return
        try:
            if message.value() is None:
                raise ValueError("empty message")
            time_us = event_time_us(message)
            raw = message.value().decode()
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            self._skipped_since_status += 1
        else:
            self.writer.write(raw, time_us)
            self._events_since_status += 1
        self._uncommitted[message.partition()] = message.offset() + 1

    def _sync_and_commit(self) -> None:
        """Stop on a failed compression, make the written events durable, then commit their offsets."""
        self.writer.check()
        self.writer.sync()
        self._commit()

    def _commit(self, partitions: list[int] | None = None) -> None:
        """Commit the offsets of events already synced to disk, for all partitions or the ones given."""
        if partitions is None:
            chosen = dict(self._uncommitted)
        else:
            chosen = {p: self._uncommitted[p] for p in partitions if p in self._uncommitted}
        if not chosen:
            return
        offsets = [TopicPartition(self.topic, partition, offset) for partition, offset in chosen.items()]
        self.consumer.commit(offsets=offsets, asynchronous=False)
        for partition in chosen:
            del self._uncommitted[partition]

    def _on_revoke(self, consumer, partitions) -> None:
        # Another member is taking these partitions: hand them over with everything read so far on disk.
        revoked = [tp.partition for tp in partitions]
        if any(p in self._uncommitted for p in revoked):
            self.writer.sync()
            self._commit(revoked)

    def _log_status(self, elapsed: float) -> None:
        synced = self.writer.last_synced_time_us
        behind = "n/a" if synced is None else f"{time.time() - synced / 1e6:.1f}s"
        log.info(
            "%s events in %.0fs (%.1f/s), on disk up to %s ago%s",
            f"{self._events_since_status:,}",
            elapsed,
            self._events_since_status / elapsed,
            behind,
            f", {self._skipped_since_status} unreadable skipped" if self._skipped_since_status else "",
        )
        self._events_since_status = 0
        self._skipped_since_status = 0


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description=f"Archive the {TOPIC} topic into hourly raw files.")
    parser.add_argument("--data-dir", type=Path, default=Path("data"), help="output folder (default: ./data)")
    parser.add_argument(
        "--kafka", default=BOOTSTRAP_SERVERS, metavar="BOOTSTRAP_SERVERS", help=f"default: {BOOTSTRAP_SERVERS}"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    archiver = Archiver(args.data_dir, make_consumer(args.kafka))
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: archiver.stop())
    archiver.run()


if __name__ == "__main__":
    main()
