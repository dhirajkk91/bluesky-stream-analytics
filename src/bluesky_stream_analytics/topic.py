"""
The Kafka topic every Jetstream event goes through, and `ensure-topic`, which
creates it or brings an existing topic's settings in line with the ones here.

The settings live in code so they're reviewed and tested like anything else.
Running `ensure-topic` again changes nothing if the topic already matches.
"""

import argparse
import logging
import sys
from dataclasses import dataclass, field

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, AlterConfigOpType, ConfigEntry, ConfigResource, NewTopic

log = logging.getLogger(__name__)

TOPIC = "jetstream.events"
BOOTSTRAP_SERVERS = "127.0.0.1:9092"

# Messages are keyed by account, so changing the count later would move accounts
# to other partitions. It's set once here and never changed by ensure-topic.
PARTITIONS = 3
REPLICATION_FACTOR = 1  # one broker

HOUR_MS = 60 * 60 * 1000
GIB = 1024**3
MIB = 1024**2

TOPIC_CONFIG = {
    "cleanup.policy": "delete",
    # Longer than Jetstream's 36-hour live lookback, so consumers can fall
    # further behind than the collector can.
    "retention.ms": str(72 * HOUR_MS),
    # A cap per partition, so the 3 partitions hold at most 12 GiB. The disk holds
    # other data too, and this keeps it under the health check's 80% line. Whichever
    # comes first, this cap or 72 hours, decides how far back the topic goes.
    "retention.bytes": str(4 * GIB),
    # Retention only deletes whole, closed segments, so smaller segments keep
    # disk use close to the limits above.
    "segment.bytes": str(256 * MIB),
    # Keep the batches as the collector compressed them (zstd).
    "compression.type": "producer",
    # The collector's receive time, which the archiver assigns hourly files by.
    "message.timestamp.type": "CreateTime",
    "min.insync.replicas": "1",
}

TIMEOUT_SECONDS = 30


@dataclass
class TopicPlan:
    """What ensure-topic will do: create the topic, change settings, or stop with a problem."""

    create: bool = False
    set_config: dict[str, str] = field(default_factory=dict)
    problem: str | None = None


def plan_topic(partitions: int | None, current_config: dict[str, str], name: str = TOPIC) -> TopicPlan:
    """
    Compare a topic as it is (partitions is None if it doesn't exist) with the
    settings here. A different partition count is reported, never changed.
    """
    if partitions is None:
        return TopicPlan(create=True)
    if partitions != PARTITIONS:
        return TopicPlan(
            problem=(
                f"{name} has {partitions} partitions, expected {PARTITIONS}. Changing the count moves accounts "
                "to other partitions, so ensure-topic leaves it alone"
            )
        )
    changes = {name: value for name, value in TOPIC_CONFIG.items() if current_config.get(name) != value}
    return TopicPlan(set_config=changes)


def admin_client(bootstrap_servers: str) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap_servers})


def topic_partitions(admin: AdminClient, name: str = TOPIC) -> int | None:
    """The topic's partition count, or None if it doesn't exist."""
    metadata = admin.list_topics(timeout=TIMEOUT_SECONDS)
    topic = metadata.topics.get(name)
    return None if topic is None else len(topic.partitions)


def topic_config(admin: AdminClient, name: str = TOPIC) -> dict[str, str]:
    """The topic's current settings, as setting to value."""
    resource = ConfigResource("TOPIC", name)
    entries = admin.describe_configs([resource], request_timeout=TIMEOUT_SECONDS)[resource].result()
    return {setting: entry.value for setting, entry in entries.items()}


def create_topic(admin: AdminClient, name: str = TOPIC) -> bool:
    """Create the topic with its settings. Returns False if it already existed."""
    new = NewTopic(name, num_partitions=PARTITIONS, replication_factor=REPLICATION_FACTOR, config=TOPIC_CONFIG)
    try:
        admin.create_topics([new], request_timeout=TIMEOUT_SECONDS)[name].result()
    except KafkaException as exc:
        if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
            return False
        raise
    return True


def set_topic_config(admin: AdminClient, changes: dict[str, str], name: str = TOPIC) -> None:
    """Set only the given settings, leaving the topic's others as they are."""
    entries = [
        ConfigEntry(setting, value, incremental_operation=AlterConfigOpType.SET) for setting, value in changes.items()
    ]
    resource = ConfigResource("TOPIC", name, incremental_configs=entries)
    admin.incremental_alter_configs([resource], request_timeout=TIMEOUT_SECONDS)[resource].result()


def ensure_topic(admin: AdminClient, name: str = TOPIC) -> TopicPlan:
    """Create the topic or align its settings, and return what was planned. Raises on a problem."""
    partitions = topic_partitions(admin, name)
    if partitions is None:
        if create_topic(admin, name):
            log.info("Created %s with %d partitions", name, PARTITIONS)
            return TopicPlan(create=True)
        log.info("%s was created by someone else meanwhile; checking its settings", name)
        partitions = topic_partitions(admin, name)

    plan = plan_topic(partitions, topic_config(admin, name), name)
    if plan.problem:
        raise RuntimeError(plan.problem)
    if plan.set_config:
        set_topic_config(admin, plan.set_config, name)
        for setting, value in sorted(plan.set_config.items()):
            log.info("Set %s = %s", setting, value)
    else:
        log.info("%s already matches: %d partitions, settings unchanged", name, PARTITIONS)
    return plan


def main() -> None:
    """Command-line entry point. Exits non-zero if the topic couldn't be made to match."""
    parser = argparse.ArgumentParser(description=f"Create the {TOPIC} topic, or bring its settings in line.")
    parser.add_argument(
        "--bootstrap-servers", default=BOOTSTRAP_SERVERS, help=f"Kafka broker to connect to (default: {BOOTSTRAP_SERVERS})"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        ensure_topic(admin_client(args.bootstrap_servers))
    except (KafkaException, RuntimeError) as exc:
        log.error("%s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
