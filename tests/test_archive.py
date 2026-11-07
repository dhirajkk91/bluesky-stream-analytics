import json
import threading
import zlib
from datetime import UTC, datetime

import pytest
from compression import zstd
from confluent_kafka import KafkaError
from kafka_fakes import FakeConsumer, FakeMessage

from bluesky_stream_analytics import archive
from bluesky_stream_analytics.archive import Archiver
from bluesky_stream_analytics.writer import HourlyFileWriter

TOPIC = "jetstream.events"
PARTITIONS = 3
# 900 events, one every 10 ms, spanning the 13:00 -> 14:00 UTC boundary.
START_US = int(datetime(2026, 11, 7, 13, 59, 57, tzinfo=UTC).timestamp() * 1_000_000)
EVENTS = [
    json.dumps({"did": f"did:plc:{i % 50}", "time_us": START_US + i * 10_000, "kind": "commit"}) for i in range(900)
]


def partition_of(raw: str) -> int:
    return zlib.crc32(json.loads(raw)["did"].encode()) % PARTITIONS


def topic_messages(events=EVENTS, headers=True) -> dict[int, list[FakeMessage]]:
    """The events as Kafka would hold them: split by account, in order within each partition."""
    partitions: dict[int, list[FakeMessage]] = {p: [] for p in range(PARTITIONS)}
    for raw in events:
        p = partition_of(raw)
        header = [("time_us", str(json.loads(raw)["time_us"]).encode())] if headers else None
        partitions[p].append(FakeMessage(TOPIC, p, len(partitions[p]), raw.encode(), header))
    return partitions


def archived_lines(data_dir) -> list[str]:
    lines = []
    for path in sorted((data_dir / "events").rglob("*.jsonl.zst")):
        with zstd.open(path, "rt") as f:
            lines.extend(line.rstrip("\n") for line in f)
    return lines


def run_until_empty(archiver: Archiver, consumer: FakeConsumer) -> None:
    consumer.when_empty = archiver.stop
    archiver.run()


@pytest.fixture(autouse=True)
def fast_sync(monkeypatch):
    monkeypatch.setattr(archive, "SYNC_INTERVAL_SECONDS", 0)


def test_archives_every_event_byte_for_byte_and_commits_every_offset(tmp_path):
    consumer = FakeConsumer(TOPIC, topic_messages())
    run_until_empty(Archiver(tmp_path, consumer), consumer)

    assert sorted(archived_lines(tmp_path)) == sorted(EVENTS)
    assert consumer.committed == {p: len(m) for p, m in topic_messages().items()}
    hours = sorted({p.parent.name for p in (tmp_path / "events").rglob("*.jsonl.zst")})
    assert hours == ["hour=13", "hour=14"]
    assert not list((tmp_path / "events").rglob("*.open"))
    assert consumer.closed


def test_offsets_are_committed_only_for_events_already_synced(tmp_path, monkeypatch):
    consumer = FakeConsumer(TOPIC, topic_messages())
    archiver = Archiver(tmp_path, consumer)
    synced_count = 0
    real_sync = HourlyFileWriter.sync

    def counting_sync(self):
        nonlocal synced_count
        synced_count = archiver._events_since_status  # every event written so far is now on disk
        return real_sync(self)

    real_commit = consumer.commit

    def checking_commit(offsets, asynchronous=True):
        committed = {**consumer.committed, **{tp.partition: tp.offset for tp in offsets}}
        assert sum(committed.values()) <= synced_count
        real_commit(offsets, asynchronous)

    monkeypatch.setattr(HourlyFileWriter, "sync", counting_sync)
    monkeypatch.setattr(archive, "STATUS_INTERVAL_SECONDS", 10**6)
    monkeypatch.setattr(archive, "BATCH_SIZE", 40)
    consumer.commit = checking_commit
    run_until_empty(archiver, consumer)

    assert len(consumer.commits) > 1
    assert sum(consumer.committed.values()) == len(EVENTS)


def test_a_failed_close_commits_nothing_more_even_when_the_consumer_revokes(tmp_path, monkeypatch):
    monkeypatch.setattr(archive, "SYNC_INTERVAL_SECONDS", 10**6)

    def broken_close(self):
        raise OSError("disk full")

    monkeypatch.setattr(HourlyFileWriter, "close", broken_close)
    consumer = FakeConsumer(TOPIC, topic_messages())
    with pytest.raises(OSError, match="disk full"):
        run_until_empty(Archiver(tmp_path, consumer), consumer)

    assert consumer.commits == []
    assert consumer.closed


def test_resumes_after_a_crash_from_the_committed_offsets_without_losing_events(tmp_path):
    partitions = topic_messages()
    # A run that was killed: it wrote the first 300 events of each partition into an
    # .open file, half a line of the next, and had committed only the first 100.
    writer = HourlyFileWriter(tmp_path)
    for messages in partitions.values():
        for message in messages[:100]:
            writer.write(message.value().decode(), json.loads(message.value())["time_us"])
    writer.sync()
    open_file = next((tmp_path / "events").rglob("*.jsonl.open"))
    with open(open_file, "ab") as f:
        f.write(b'{"did":"did:plc:1","time_')

    consumer = FakeConsumer(TOPIC, partitions, committed={p: 100 for p in partitions})
    run_until_empty(Archiver(tmp_path, consumer), consumer)

    lines = archived_lines(tmp_path)
    assert set(lines) == set(EVENTS)
    assert len(lines) == len(EVENTS)  # what was committed is never read again
    assert consumer.committed == {p: len(m) for p, m in partitions.items()}


def test_an_event_without_the_header_is_dated_from_the_event_itself(tmp_path):
    consumer = FakeConsumer(TOPIC, topic_messages(headers=False))
    run_until_empty(Archiver(tmp_path, consumer), consumer)
    assert sorted(archived_lines(tmp_path)) == sorted(EVENTS)


def test_unreadable_or_empty_events_are_skipped_but_their_offsets_still_committed(tmp_path):
    partitions = {
        0: [
            FakeMessage(TOPIC, 0, 0, b"not json"),
            FakeMessage(TOPIC, 0, 1, None),
            FakeMessage(TOPIC, 0, 2, EVENTS[0].encode()),
        ]
    }
    consumer = FakeConsumer(TOPIC, partitions)
    run_until_empty(Archiver(tmp_path, consumer), consumer)
    assert archived_lines(tmp_path) == [EVENTS[0]]
    assert consumer.committed == {0: 3}


def test_a_passing_kafka_error_is_logged_and_archiving_carries_on(tmp_path, caplog):
    error = KafkaError(KafkaError._TRANSPORT, "Broker went away")
    partitions = topic_messages()
    partitions[0].insert(50, FakeMessage(TOPIC, 0, -1001, None, error=error))
    consumer = FakeConsumer(TOPIC, partitions)
    run_until_empty(Archiver(tmp_path, consumer), consumer)

    assert sorted(archived_lines(tmp_path)) == sorted(EVENTS)
    assert "Kafka reported: Broker went away" in caplog.text


def test_a_fatal_kafka_error_stops_the_archiver_after_saving_what_it_has(tmp_path):
    error = KafkaError(KafkaError._FATAL, "Producer fenced", fatal=True)
    partitions = topic_messages()
    partitions[0].insert(50, FakeMessage(TOPIC, 0, -1001, None, error=error))
    consumer = FakeConsumer(TOPIC, partitions)
    with pytest.raises(Exception, match="Producer fenced"):
        run_until_empty(Archiver(tmp_path, consumer), consumer)

    lines = archived_lines(tmp_path)
    assert lines and set(lines) <= set(EVENTS)
    assert sum(consumer.committed.values()) == len(lines)


def test_stop_from_another_thread_finishes_cleanly(tmp_path):
    consumer = FakeConsumer(TOPIC, topic_messages())
    archiver = Archiver(tmp_path, consumer)
    thread = threading.Thread(target=archiver.run)
    thread.start()
    consumer.when_empty = archiver.stop
    thread.join(timeout=10)
    assert not thread.is_alive()
    assert sorted(archived_lines(tmp_path)) == sorted(EVENTS)
