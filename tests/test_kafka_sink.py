import json

import pytest
from confluent_kafka import KafkaException
from kafka_fakes import FakeProducer

from bluesky_stream_analytics import kafka_sink
from bluesky_stream_analytics.kafka_sink import KafkaSink

START_US = 1_793_000_000_000_000


def event(i: int) -> tuple[str, int]:
    time_us = START_US + i
    return json.dumps({"did": f"did:plc:{i}", "time_us": time_us, "kind": "commit"}), time_us


def test_each_event_is_keyed_by_account_and_carries_its_time():
    producer = FakeProducer()
    sink = KafkaSink(producer, topic="t")
    raw, time_us = event(1)
    sink.write(raw, time_us)
    assert sink.sync() == time_us
    assert producer.acknowledged == [("t", b"did:plc:1", raw.encode(), [("time_us", str(time_us).encode())])]


def test_the_cursor_only_moves_once_kafka_has_acknowledged():
    producer = FakeProducer()
    sink = KafkaSink(producer)
    sink.write(*event(1))
    sink.sync()

    producer.hold = True
    sink.write(*event(2))
    assert sink.last_written_time_us == START_US + 2
    with pytest.raises(TimeoutError, match="hasn't acknowledged 1 events"):
        sink.sync()
    assert sink.last_synced_time_us == START_US + 1

    producer.hold = False
    assert sink.sync() == START_US + 2


def test_a_rejected_event_stops_the_sink_at_the_last_acknowledged_one():
    producer = FakeProducer(fail_from_time_us=START_US + 10)
    sink = KafkaSink(producer)
    for i in range(5):
        sink.write(*event(i))
    assert sink.sync() == START_US + 4

    for i in range(5, 15):
        sink.write(*event(i))
    with pytest.raises(KafkaException, match="Message timed out"):
        sink.sync()
    assert sink.last_synced_time_us == START_US + 4
    with pytest.raises(KafkaException):
        sink.close()


def test_a_full_producer_queue_waits_for_room(monkeypatch):
    monkeypatch.setattr(kafka_sink, "QUEUE_FULL_WAIT_SECONDS", 0.001)
    producer = FakeProducer(queue_limit=3)
    sink = KafkaSink(producer)

    # Nothing is acknowledged until the sink waits for room, as when Kafka is slow.
    def release_on_wait(timeout=0):
        producer.hold = timeout == 0
        return FakeProducer.poll(producer, timeout)

    producer.poll = release_on_wait
    for i in range(10):
        sink.write(*event(i))
    del producer.poll
    producer.hold = False
    assert sink.sync() == START_US + 9
    assert producer.buffer_errors > 0
    assert [json.loads(value)["time_us"] for _, _, value, _ in producer.acknowledged] == [START_US + i for i in range(10)]


def test_an_event_without_an_account_is_sent_unkeyed_rather_than_stopping_collection():
    producer = FakeProducer()
    sink = KafkaSink(producer, topic="t")
    raw = json.dumps({"time_us": START_US, "kind": "unknown"})
    sink.write(raw, START_US)
    assert sink.sync() == START_US
    assert producer.acknowledged[0][1] is None
