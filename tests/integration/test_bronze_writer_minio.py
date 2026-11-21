"""Chaîne complète : producteur → Redpanda → consumer → Parquet sur MinIO (`make up`)."""

import threading
import uuid
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import httpx
import pytest
from confluent_kafka.admin import AdminClient

from star_pulse.common.kafka import BrokerUnavailableError, check_broker
from star_pulse.common.storage import S3ObjectStore
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
from star_pulse.streaming.bronze_writer import BronzeWriter, KafkaMessageSource

pytestmark = pytest.mark.integration


@pytest.fixture
def settings() -> Settings:
    settings = Settings().model_copy(update={"s3_bucket": "lake-tests"})
    try:
        check_broker(settings.kafka_bootstrap_servers, timeout_s=3)
        httpx.get(f"{settings.s3_endpoint_url}minio/health/live", timeout=2).raise_for_status()
    except (BrokerUnavailableError, httpx.HTTPError):
        pytest.skip("Infra indisponible : lancer `make up`")
    return settings


def test_end_to_end_bronze(
    settings: Settings, rt_payloads: dict[str, bytes], tmp_path: Path
) -> None:
    run_id = uuid.uuid4().hex[:8]
    topic = f"test.vehicle-positions.{run_id}"
    ensure_topics(settings.kafka_bootstrap_servers, [topic])

    source = FeedSource(Feed.VEHICLE_POSITIONS, "https://rt.test", topic)
    feed = decode_feed(rt_payloads["vehicle_position"])
    batch = build_batch(source, feed, Deduplicator(), datetime.now(tz=UTC))
    assert batch is not None
    KafkaPublisher(settings.kafka_bootstrap_servers).publish(batch.messages)

    store = S3ObjectStore.from_settings(settings)
    store.ensure_bucket()
    before = set(store.list_keys("bronze/gtfs_rt/vehicle_positions/"))
    consumer = KafkaMessageSource(settings.kafka_bootstrap_servers, f"test-{run_id}", [topic])
    try:
        BronzeWriter(consumer, store).run(threading.Event(), once=True)
    finally:
        AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers}).delete_topics([topic])

    new_keys = sorted(set(store.list_keys("bronze/gtfs_rt/vehicle_positions/")) - before)
    assert new_keys, "aucun fichier écrit"
    rows = 0
    with duckdb.connect() as con:
        for i, key in enumerate(new_keys):
            path = tmp_path / f"{i}.parquet"
            path.write_bytes(store.get_bytes(key))
            row = con.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()
            assert row is not None
            rows += row[0]
    assert rows == len(batch.messages)
