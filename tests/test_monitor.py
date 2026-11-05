import os
import re
import subprocess
import sys
import time
import urllib.error
from pathlib import Path
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaError, KafkaException

from bluesky_stream_analytics import monitor
from bluesky_stream_analytics.cursor import save_cursor

NOW = 1_793_000_000.0
SYSTEMD = Path(__file__).parent.parent / "deploy" / "systemd"


class FakePost:
    def __init__(self, failures: int = 0):
        self.calls: list[tuple[str, bytes]] = []
        self.failures = failures

    def __call__(self, url: str, body: bytes) -> None:
        self.calls.append((url, body))
        if len(self.calls) <= self.failures:
            raise urllib.error.URLError("unreachable")


def roomy_disk(_path):
    return (100, 30, 70)


def healthy_dir(tmp_path: Path) -> Path:
    save_cursor(tmp_path / "state" / "cursor.json", int((NOW - 3) * 1_000_000))
    return tmp_path


def add_file(path: Path, age_seconds: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x")
    os.utime(path, (NOW - age_seconds, NOW - age_seconds))


def test_ping_urls_use_the_key_and_slug():
    assert monitor.ping_url("k3y", "bluesky-process.service") == "https://hc-ping.com/k3y/bluesky-process"
    assert monitor.ping_url("k3y", "bluesky-check", fail=True) == "https://hc-ping.com/k3y/bluesky-check/fail"


def test_no_key_means_no_ping(monkeypatch):
    monkeypatch.delenv("HC_PING_KEY", raising=False)
    post = FakePost()
    assert monitor.ping("bluesky-upload", post=post) is False
    assert post.calls == []


def test_a_failure_ping_carries_the_end_of_the_message():
    post = FakePost()
    message = "x" * 20_000 + "the error"
    assert monitor.ping("bluesky-marts.service", fail=True, message=message, key="k", post=post)
    url, body = post.calls[0]
    assert url == "https://hc-ping.com/k/bluesky-marts/fail"
    assert len(body) == monitor.MAX_MESSAGE_BYTES and body.endswith(b"the error")


def test_a_ping_retries_then_gives_up_without_raising():
    post = FakePost(failures=99)
    assert monitor.ping("bluesky-upload", key="k", post=post, sleep=lambda s: None) is False
    assert len(post.calls) == 3

    post = FakePost(failures=1)
    assert monitor.ping("bluesky-upload", key="k", post=post, sleep=lambda s: None) is True


def test_a_healthy_server_has_no_problems(tmp_path):
    data = healthy_dir(tmp_path)
    add_file(data / "events" / "date=2026-10-28" / "hour=13" / "a.jsonl.zst", 5 * 60)
    add_file(data / "uploaded" / "events" / "date=2026-10-28" / "hour=12" / "b.jsonl.zst", 50 * 60)
    problems, summary = monitor.find_problems(data, now=NOW, disk_usage=roomy_disk)
    assert problems == []
    assert summary == "newest event 3s old, 1 files to upload, 1 files to process, disk 30% used"


def test_a_stalled_collector(tmp_path):
    save_cursor(tmp_path / "state" / "cursor.json", int((NOW - 14 * 60) * 1_000_000))
    problems, _ = monitor.find_problems(tmp_path, now=NOW, disk_usage=roomy_disk)
    assert problems == ["The newest event is 14 min old (limit 5 min): the collector or Jetstream has stalled"]


def test_no_cursor_or_a_broken_one(tmp_path):
    problems, _ = monitor.find_problems(tmp_path, now=NOW, disk_usage=roomy_disk)
    assert problems == ["The collector hasn't saved a cursor yet"]

    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "cursor.json").write_text("{")
    problems, _ = monitor.find_problems(tmp_path, now=NOW, disk_usage=roomy_disk)
    assert len(problems) == 1 and problems[0].startswith("The collector's cursor can't be read")


def test_files_waiting_too_long(tmp_path):
    data = healthy_dir(tmp_path)
    add_file(data / "events" / "date=2026-10-28" / "hour=10" / "a.jsonl.zst", 45 * 60)
    add_file(data / "events" / "date=2026-10-28" / "hour=11" / "b.jsonl.zst", 10 * 60)
    add_file(data / "uploaded" / "events" / "date=2026-10-28" / "hour=08" / "c.jsonl.zst", 3 * 60 * 60)
    problems, _ = monitor.find_problems(data, now=NOW, disk_usage=roomy_disk)
    assert problems == [
        "2 finished files are waiting to be uploaded, the oldest for 45 min (limit 30 min)",
        "1 uploaded files are waiting to be processed, the oldest for 180 min (limit 120 min)",
    ]


def test_a_full_disk(tmp_path):
    problems, _ = monitor.find_problems(healthy_dir(tmp_path), now=NOW, disk_usage=lambda p: (100, 91, 9))
    assert problems == ["The disk is 91% full (limit 80%)"]


def test_run_check_pings_success_or_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    monkeypatch.setattr(monitor.shutil, "disk_usage", roomy_disk)
    post = FakePost()
    assert monitor.run_check(healthy_dir(tmp_path), key="k", post=post) == 0
    assert post.calls[-1][0] == "https://hc-ping.com/k/bluesky-check"

    save_cursor(tmp_path / "state" / "cursor.json", int((NOW - 20 * 60) * 1_000_000))
    assert monitor.run_check(tmp_path, key="k", post=post) == 1
    url, body = post.calls[-1]
    assert url == "https://hc-ping.com/k/bluesky-check/fail"
    assert body.startswith(b"The newest event is 20 min old")


def cluster(partitions: dict[int, int] | None = None, error=None):
    """Cluster metadata like AdminClient.list_topics returns: partition id to leader, or no topic."""
    topics = {}
    if partitions is not None:
        parts = {i: SimpleNamespace(id=i, leader=leader) for i, leader in partitions.items()}
        topics["jetstream.events"] = SimpleNamespace(partitions=parts, error=error)
    return SimpleNamespace(topics=topics)


def test_a_healthy_kafka():
    problems, summary = monitor.find_kafka_problems(lambda: cluster({0: 1, 1: 1, 2: 1}))
    assert problems == [] and summary == "kafka up, 3 partitions"


def test_kafka_not_answering():
    def unreachable():
        raise KafkaException(KafkaError(KafkaError._TRANSPORT, "Connection refused"))

    assert monitor.find_kafka_problems(unreachable) == (["Kafka didn't answer: Connection refused"], "kafka down")


def test_kafka_without_the_topic_or_with_a_broken_one():
    assert monitor.find_kafka_problems(lambda: cluster()) == (
        ["Kafka has no jetstream.events topic"],
        "kafka up, no topic",
    )
    problems, _ = monitor.find_kafka_problems(lambda: cluster({0: 1}, error=KafkaError(KafkaError.LEADER_NOT_AVAILABLE)))
    assert problems[0].startswith("Kafka reports an error on jetstream.events")


def test_kafka_partitions_missing_or_without_a_leader():
    problems, summary = monitor.find_kafka_problems(lambda: cluster({0: 1, 1: -1}))
    assert problems == [
        "jetstream.events has 2 partitions, expected 3",
        "jetstream.events partitions without a leader: 1",
    ]
    assert summary == "kafka up, 2 partitions"


def test_a_real_client_with_no_broker_reports_kafka_down():
    admin = monitor.kafka_admin("127.0.0.1:1")
    problems, summary = monitor.find_kafka_problems(lambda: admin.list_topics(timeout=2))
    assert summary == "kafka down" and problems[0].startswith("Kafka didn't answer")


def test_run_check_includes_kafka_when_given_a_broker(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: NOW)
    monkeypatch.setattr(monitor.shutil, "disk_usage", roomy_disk)
    monkeypatch.setattr(monitor, "kafka_admin", lambda servers: SimpleNamespace(list_topics=lambda timeout: cluster()))
    post = FakePost()
    assert monitor.run_check(healthy_dir(tmp_path), kafka_bootstrap="127.0.0.1:9092", key="k", post=post) == 1
    url, body = post.calls[-1]
    assert url.endswith("/bluesky-check/fail")
    assert body == b"Kafka has no jetstream.events topic\n\nnewest event 3s old, 0 files to upload, 0 files to process, disk 30% used, kafka up, no topic"


def test_ping_check_never_loads_the_kafka_client():
    code = "import sys, bluesky_stream_analytics.monitor; print('confluent_kafka' in sys.modules)"
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert result.stdout.strip() == "False"


def job_units() -> list[str]:
    """Every unit that runs a job: everything but the collector, the health check and the alert."""
    skip = {"bluesky-collect", "bluesky-check", "bluesky-alert@"}
    return sorted(p.stem for p in SYSTEMD.glob("*.service") if p.stem not in skip)


@pytest.mark.parametrize("name", job_units())
def test_every_job_reports_to_healthchecks(name):
    unit = (SYSTEMD / f"{name}.service").read_text()
    assert "EnvironmentFile=-/etc/bluesky/monitor.env" in unit
    assert "OnFailure=bluesky-alert@%n.service" in unit
    assert re.search(rf"^ExecStartPost=\S+/ping-check {name.removesuffix('@')}$", unit, re.MULTILINE)


def test_the_health_check_runs_on_a_timer_and_reports():
    unit = (SYSTEMD / "bluesky-check.service").read_text()
    assert "EnvironmentFile=-/etc/bluesky/monitor.env" in unit and "check-health" in unit
    assert "--kafka 127.0.0.1:9092" in unit
    assert (SYSTEMD / "bluesky-check.timer").exists()


@pytest.mark.parametrize(
    "unit, check",
    [
        ("bluesky-process.service", "bluesky-process"),
        ("bluesky-project.service", "bluesky-process"),
        ("bluesky-handles.service", "bluesky-process"),
        ("bluesky-marts@2026-10-30.service", "bluesky-marts"),
        ("bluesky-export.service", "bluesky-marts"),
        ("bluesky-vacuum", "bluesky-vacuum"),
        ("bluesky-check", "bluesky-check"),
    ],
)
def test_each_unit_reports_to_its_jobs_check(unit, check):
    assert monitor.slug_for(unit) == check
    assert monitor.ping_url("k", unit, fail=True) == f"https://hc-ping.com/k/{check}/fail"
