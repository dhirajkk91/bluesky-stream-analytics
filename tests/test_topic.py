import re
from pathlib import Path

from bluesky_stream_analytics import topic

DEPLOY = Path(__file__).parent.parent / "deploy" / "kafka"
JETSTREAM_LOOKBACK_MS = 36 * 60 * 60 * 1000


def properties(path: Path) -> dict[str, str]:
    lines = [line.strip() for line in path.read_text().splitlines()]
    return dict(line.split("=", 1) for line in lines if line and not line.startswith("#"))


def test_a_missing_topic_is_created():
    assert topic.plan_topic(None, {}) == topic.TopicPlan(create=True)


def test_a_matching_topic_is_left_alone():
    plan = topic.plan_topic(topic.PARTITIONS, {**topic.TOPIC_CONFIG, "flush.ms": "9223372036854775807"})
    assert plan == topic.TopicPlan()


def test_only_the_settings_that_differ_are_changed():
    current = {**topic.TOPIC_CONFIG, "retention.ms": "604800000"}
    del current["segment.bytes"]
    plan = topic.plan_topic(topic.PARTITIONS, current)
    assert plan.set_config == {
        "retention.ms": topic.TOPIC_CONFIG["retention.ms"],
        "segment.bytes": topic.TOPIC_CONFIG["segment.bytes"],
    }
    assert not plan.create and plan.problem is None


def test_a_different_partition_count_is_reported_not_changed():
    plan = topic.plan_topic(6, {})
    assert not plan.create and plan.set_config == {}
    assert plan.problem.startswith("jetstream.events has 6 partitions, expected 3")


def test_a_problem_names_the_topic_it_was_about():
    assert topic.plan_topic(1, {}, "test.other").problem.startswith("test.other has 1 partitions")


def test_consumers_can_fall_further_behind_than_jetstream_keeps():
    assert int(topic.TOPIC_CONFIG["retention.ms"]) > JETSTREAM_LOOKBACK_MS


def test_the_broker_listens_on_localhost_only():
    config = properties(DEPLOY / "server.properties")
    for key in ("listeners", "advertised.listeners"):
        hosts = re.findall(r"://([^:]+):", config[key])
        assert hosts and set(hosts) == {"127.0.0.1"}, key


def test_the_broker_never_creates_topics_by_itself():
    assert properties(DEPLOY / "server.properties")["auto.create.topics.enable"] == "false"


def test_the_unit_runs_the_installed_config_and_owns_the_data_directory():
    unit = (DEPLOY / "kafka.service").read_text()
    config = properties(DEPLOY / "server.properties")
    assert "ExecStart=/opt/kafka/bin/kafka-server-start.sh /etc/kafka/server.properties" in unit
    assert "StateDirectory=kafka" in unit and config["log.dirs"].startswith("/var/lib/kafka/")
    assert 'Environment="KAFKA_HEAP_OPTS=-Xms512m -Xmx1g"' in unit


def test_the_shadow_units_write_only_where_nothing_uploads_or_processes():
    for name, command in [("bluesky-shadow-collect", "collect"), ("bluesky-shadow-archive", "archive")]:
        unit = (DEPLOY / f"{name}.service").read_text()
        exec_start = next(line for line in unit.splitlines() if line.startswith("ExecStart="))
        data_dir = exec_start.split("--data-dir ")[1].split()[0]
        assert exec_start.startswith(f"ExecStart=/opt/bluesky-stream-analytics/.venv/bin/{command} ")
        assert "--kafka 127.0.0.1:9092" in exec_start
        assert data_dir.startswith("/var/lib/bluesky/shadow/")
        assert "After=" in unit and "kafka.service" in unit.split("After=")[1].splitlines()[0]
        assert "User=bluesky" in unit
