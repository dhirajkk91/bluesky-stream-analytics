"""
Send collected events to the Kafka topic instead of writing them to files.

KafkaSink has the same methods as HourlyFileWriter, so the collector's writer
thread drives it the same way: write each event, and every few seconds sync and
save the cursor. Here, syncing means waiting until Kafka has acknowledged every
event sent so far, so the cursor never points past an event Kafka doesn't have.
"""

import json
import logging

from confluent_kafka import KafkaException, Producer

from bluesky_stream_analytics.topic import TOPIC

log = logging.getLogger(__name__)

TIME_US_HEADER = "time_us"
# How long a sync waits for Kafka to acknowledge what's been sent before giving up.
FLUSH_TIMEOUT_SECONDS = 30
# How long to serve delivery reports while the producer's local queue is full.
QUEUE_FULL_WAIT_SECONDS = 0.1

PRODUCER_CONFIG = {
    "acks": "all",
    # Retries can't create duplicates or reorder events within a partition.
    "enable.idempotence": True,
    "compression.type": "zstd",
    # Wait up to 50 ms to fill a batch, so events are sent and compressed in groups.
    "linger.ms": 50,
    "client.id": "collector",
}


def make_producer(bootstrap_servers: str) -> Producer:
    return Producer({"bootstrap.servers": bootstrap_servers, **PRODUCER_CONFIG})


class KafkaSink:
    """
    Produces each event to the topic, keyed by account, with its time_us in a header.

    Not thread-safe: the collector uses it from its writer thread only.
    """

    def __init__(self, producer: Producer, topic: str = TOPIC) -> None:
        self._producer = producer
        self._topic = topic
        self._error: KafkaException | None = None
        self._last_written_time_us: int | None = None
        self._last_synced_time_us: int | None = None

    def write(self, raw: str, time_us: int) -> None:
        """Queue one raw event for sending. If the producer's queue is full, wait for room."""
        # An event without an account still goes through, unkeyed, rather than stopping collection.
        did = json.loads(raw).get("did")
        key = did.encode() if isinstance(did, str) else None
        while True:
            try:
                self._producer.produce(
                    self._topic,
                    value=raw.encode(),
                    key=key,
                    headers=[(TIME_US_HEADER, str(time_us).encode())],
                    on_delivery=self._delivered,
                )
                break
            except BufferError:
                self._producer.poll(QUEUE_FULL_WAIT_SECONDS)
                self.check()
        self._producer.poll(0)
        self._last_written_time_us = time_us

    def sync(self) -> int | None:
        """
        Wait until Kafka has acknowledged everything sent, and return the newest time_us
        that is now safe to save as the cursor. Raises if any event wasn't delivered.
        """
        remaining = self._producer.flush(FLUSH_TIMEOUT_SECONDS)
        self.check()
        if remaining:
            raise TimeoutError(f"Kafka hasn't acknowledged {remaining} events after {FLUSH_TIMEOUT_SECONDS}s")
        self._last_synced_time_us = self._last_written_time_us
        return self._last_synced_time_us

    def check(self) -> None:
        """Raise the first delivery failure reported so far."""
        self._producer.poll(0)
        if self._error is not None:
            raise self._error

    def close(self) -> int | None:
        """Send everything still queued and return the newest time_us Kafka has acknowledged."""
        return self.sync()

    @property
    def last_written_time_us(self) -> int | None:
        """Newest time_us sent, acknowledged or not."""
        return self._last_written_time_us

    @property
    def last_synced_time_us(self) -> int | None:
        """Newest time_us Kafka is known to have."""
        return self._last_synced_time_us

    def _delivered(self, error, message) -> None:
        # Called by the producer, from poll() or flush(), once per event.
        if error is not None and self._error is None:
            self._error = KafkaException(error)
            log.error("Kafka didn't accept an event: %s", error.str())
