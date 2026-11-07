"""Stand-ins for confluent-kafka's Producer and Consumer, for tests that don't need a broker."""

import time

from confluent_kafka import KafkaError, TopicPartition


class FakeProducer:
    """
    Keeps what's produced and acknowledges it on poll() or flush(), like the real one.
    Events at or after fail_from_time_us are rejected; hold=True leaves everything unacknowledged.
    """

    def __init__(self, fail_from_time_us: int | None = None, queue_limit: int | None = None) -> None:
        self.fail_from_time_us = fail_from_time_us
        self.queue_limit = queue_limit
        self.hold = False
        self.acknowledged: list[tuple] = []  # (topic, key, value, headers)
        self.buffer_errors = 0
        self._pending: list[tuple] = []

    def produce(self, topic, value, key, headers, on_delivery) -> None:
        if self.queue_limit is not None and len(self._pending) >= self.queue_limit:
            self.buffer_errors += 1
            raise BufferError("Local: Queue full")
        self._pending.append(((topic, key, value, headers), on_delivery))

    def poll(self, timeout: float = 0) -> int:
        if self.hold:
            return 0
        pending, self._pending = self._pending, []
        for record, on_delivery in pending:
            time_us = int(dict(record[3])["time_us"])
            if self.fail_from_time_us is not None and time_us >= self.fail_from_time_us:
                on_delivery(KafkaError(KafkaError._MSG_TIMED_OUT, "Message timed out"), None)
            else:
                self.acknowledged.append(record)
                on_delivery(None, None)
        return len(pending)

    def flush(self, timeout: float | None = None) -> int:
        self.poll()
        return len(self._pending)


class FakeMessage:
    def __init__(self, topic: str, partition: int, offset: int, value: bytes, headers=None, error=None) -> None:
        self._topic, self._partition, self._offset = topic, partition, offset
        self._value, self._headers, self._error = value, headers, error

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def headers(self):
        return self._headers

    def error(self):
        return self._error


class FakeConsumer:
    """
    Serves messages from each partition, starting at its committed offset, a few at a time
    and alternating between partitions. Calls when_empty() once everything has been served.
    Closing it revokes its partitions, as leaving the group does.
    """

    def __init__(self, topic: str, partitions: dict[int, list[FakeMessage]], committed=None, step: int = 7) -> None:
        self.topic = topic
        self.partitions = partitions
        self.committed: dict[int, int] = dict(committed or {})
        self.position = {p: self.committed.get(p, 0) for p in partitions}
        self.step = step
        self.commits: list[dict[int, int]] = []
        self.when_empty = lambda: None
        self.on_revoke = None
        self.closed = False

    def subscribe(self, topics, on_revoke=None) -> None:
        assert topics == [self.topic]
        self.on_revoke = on_revoke

    def consume(self, num_messages: int = 1, timeout: float = -1) -> list[FakeMessage]:
        batch = []
        while len(batch) < num_messages:
            progressed = False
            for partition, messages in self.partitions.items():
                start = self.position[partition]
                taken = messages[start : start + self.step]
                batch.extend(taken)
                self.position[partition] += len(taken)
                progressed = progressed or bool(taken)
            if not progressed:
                break
        if not batch:
            self.when_empty()
            time.sleep(0.01)
        return batch

    def commit(self, offsets, asynchronous: bool = True) -> None:
        assert not asynchronous
        done = {tp.partition: tp.offset for tp in offsets}
        self.commits.append(done)
        self.committed.update(done)

    def close(self) -> None:
        if self.on_revoke is not None:
            self.on_revoke(self, [TopicPartition(self.topic, p) for p in self.partitions])
        self.closed = True
