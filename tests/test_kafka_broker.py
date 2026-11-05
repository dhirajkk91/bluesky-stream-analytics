"""
Runs ensure-topic and the health check against a real Kafka broker. Skipped
unless KAFKA_BOOTSTRAP names one; CI starts one for these tests.

Each test works on its own throwaway topic and deletes only that one, so the
tests are safe even against a broker that holds jetstream.events.
"""

import os
import time
import uuid

import pytest
from confluent_kafka import KafkaException
from confluent_kafka.admin import NewTopic

from bluesky_stream_analytics import monitor, topic

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP")
pytestmark = pytest.mark.skipif(not BOOTSTRAP, reason="needs a Kafka broker in KAFKA_BOOTSTRAP")


def wait_until(condition, seconds: float = 60, step: float = 0.5):
    deadline = time.monotonic() + seconds
    while True:
        try:
            if condition():
                return
        except KafkaException:
            pass
        if time.monotonic() > deadline:
            raise TimeoutError("the broker didn't get there in time")
        time.sleep(step)


def has_leaders(admin, name: str) -> bool:
    found = admin.list_topics(timeout=5).topics.get(name)
    return found is not None and bool(found.partitions) and all(p.leader >= 0 for p in found.partitions.values())


@pytest.fixture
def admin():
    client = topic.admin_client(BOOTSTRAP)
    wait_until(lambda: client.list_topics(timeout=5) is not None)
    return client


@pytest.fixture
def name(admin):
    """A topic name no one else uses, deleted after the test if the test created it."""
    test_topic = f"test.ensure-topic.{uuid.uuid4().hex[:12]}"
    assert test_topic != topic.TOPIC
    yield test_topic
    if test_topic in admin.list_topics(timeout=10).topics:
        admin.delete_topics([test_topic])[test_topic].result()


def test_creates_the_topic_then_changes_nothing(admin, name):
    assert topic.ensure_topic(admin, name).create
    wait_until(lambda: has_leaders(admin, name))

    assert topic.topic_partitions(admin, name) == topic.PARTITIONS
    config = topic.topic_config(admin, name)
    assert {setting: config[setting] for setting in topic.TOPIC_CONFIG} == topic.TOPIC_CONFIG

    assert topic.ensure_topic(admin, name) == topic.TopicPlan()


def test_puts_back_a_setting_changed_by_hand(admin, name):
    topic.ensure_topic(admin, name)
    wait_until(lambda: has_leaders(admin, name))
    topic.set_topic_config(admin, {"retention.ms": "3600000"}, name)
    wait_until(lambda: topic.topic_config(admin, name)["retention.ms"] == "3600000")

    plan = topic.ensure_topic(admin, name)
    assert plan.set_config == {"retention.ms": topic.TOPIC_CONFIG["retention.ms"]}
    wait_until(lambda: topic.topic_config(admin, name)["retention.ms"] == topic.TOPIC_CONFIG["retention.ms"])


def test_refuses_a_topic_with_another_partition_count(admin, name):
    admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)])[name].result()
    wait_until(lambda: has_leaders(admin, name))
    with pytest.raises(RuntimeError, match="has 1 partitions, expected 3"):
        topic.ensure_topic(admin, name)
    assert topic.topic_partitions(admin, name) == 1


def test_the_health_check_sees_the_topic(admin, name):
    def check():
        return monitor.find_kafka_problems(lambda: admin.list_topics(timeout=10), name)

    assert check() == ([f"Kafka has no {name} topic"], "kafka up, no topic")

    topic.ensure_topic(admin, name)
    wait_until(lambda: has_leaders(admin, name))
    assert check() == ([], "kafka up, 3 partitions")
