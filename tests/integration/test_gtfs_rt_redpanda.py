"""Producteur GTFS-RT contre le vrai Redpanda local (`make up` requis)."""

import json
import uuid
from datetime import UTC, datetime

import pytest
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient

from star_pulse.config import Settings
from star_pulse.ingestion.gtfs_rt import (
    Deduplicator,
    Feed,
    FeedSource,
    KafkaPublisher,
    build_batch,
    decode_feed,
    ensure_topics,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def bootstrap() -> str:
    servers = Settings().kafka_bootstrap_servers
    try:
        AdminClient({"bootstrap.servers": servers}).list_topics(timeout=3)
    except KafkaException:
        pytest.skip("Redpanda indisponible : lancer `make up`")
    return servers


def test_publish_and_read_back(bootstrap: str, rt_payloads: dict[str, bytes]) -> None:
    # Topic jetable : le test ne touche pas aux topics réels du pipeline.
    topic = f"test.trip-updates.{uuid.uuid4().hex[:8]}"
    ensure_topics(bootstrap, [topic])
    ensure_topics(bootstrap, [topic])  # idempotent

    source = FeedSource(Feed.TRIP_UPDATES, "https://rt.test", topic)
    feed = decode_feed(rt_payloads["trip_update"])
    batch = build_batch(source, feed, Deduplicator(), datetime.now(tz=UTC))
    assert batch is not None
    KafkaPublisher(bootstrap).publish(batch.messages)

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"test-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    received = []
    try:
        for _ in range(30):
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                received.append(msg)
            if len(received) == len(batch.messages):
                break
    finally:
        consumer.close()
        AdminClient({"bootstrap.servers": bootstrap}).delete_topics([topic])

    assert len(received) == len(batch.messages)
    keys = {m.key().decode() for m in received if m.key() is not None}
    assert keys == {m.key for m in batch.messages}
    value = received[0].value()
    assert value is not None
    assert json.loads(value)["feed"] == "trip_updates"
