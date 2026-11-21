"""Tests du consumer bronze (sans broker ni MinIO)."""

import json
import threading
from collections import deque
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from star_pulse.common.storage import InMemoryObjectStore
from star_pulse.ingestion.gtfs_rt import Deduplicator, Feed, FeedSource, build_batch, decode_feed
from star_pulse.streaming.bronze_writer import (
    PARQUET_COLUMNS,
    REJECTED_FEED,
    BronzeWriter,
    ConsumedMessage,
    group_records,
    object_key,
    records_to_parquet,
    to_record,
)

NOW = datetime(2026, 11, 21, 10, 0, tzinfo=UTC)
TOPIC = "gtfs-rt.trip-updates"


def _envelope(feed_ts: int, key: str = "12_SAMEDI-1", version: str = "1") -> bytes:
    return json.dumps(
        {
            "feed": "trip_updates",
            "feed_timestamp": feed_ts,
            "fetched_at": NOW.isoformat(),
            "entity_key": key,
            "entity_version": version,
            "entity": {"id": key, "trip_update": {"trip": {"trip_id": key}}},
        }
    ).encode()


def _msg(offset: int, value: bytes, partition: int = 0) -> ConsumedMessage:
    return ConsumedMessage(TOPIC, partition, offset, 1_795_255_000_000, value)


class FakeSource:
    """Source en mémoire qui enregistre les commits."""

    def __init__(self, messages: list[ConsumedMessage]) -> None:
        self.queue = deque(messages)
        self.commits: list[dict[tuple[str, int], int]] = []
        self.closed = False

    def poll(self, timeout_s: float) -> ConsumedMessage | None:
        return self.queue.popleft() if self.queue else None

    def commit(self, next_offsets: dict[tuple[str, int], int]) -> None:
        self.commits.append(dict(next_offsets))

    def close(self) -> None:
        self.closed = True


class FailingStore(InMemoryObjectStore):
    def put_bytes(
        self, key: str, data: bytes, *, content_type: str = "application/octet-stream"
    ) -> None:
        raise OSError("MinIO indisponible")


def _read_parquet(data: bytes, tmp_path: Path) -> list[tuple[object, ...]]:
    path = tmp_path / "f.parquet"
    path.write_bytes(data)
    with duckdb.connect() as con:
        return con.execute("SELECT * FROM read_parquet(?)", [str(path)]).fetchall()


# --- Conversion


def test_to_record_parses_envelope() -> None:
    record = to_record(_msg(7, _envelope(1_795_255_354)), NOW)
    assert record.feed == "trip_updates"
    assert record.entity_key == "12_SAMEDI-1"
    assert json.loads(record.entity_json)["id"] == "12_SAMEDI-1"
    assert (record.kafka_topic, record.kafka_offset) == (TOPIC, 7)


def test_invalid_message_is_kept_as_rejected() -> None:
    record = to_record(_msg(3, b"\xffpas du json"), NOW)
    assert record.feed == REJECTED_FEED
    assert record.entity_key is None
    assert "pas du json" in record.entity_json


def test_real_producer_output_is_accepted(rt_payloads: dict[str, bytes]) -> None:
    source = FeedSource(Feed.VEHICLE_POSITIONS, "https://rt.test", "gtfs-rt.vehicle-positions")
    batch = build_batch(source, decode_feed(rt_payloads["vehicle_position"]), Deduplicator(), NOW)
    assert batch is not None
    records = [
        to_record(ConsumedMessage(m.topic, 0, i, None, m.value), NOW)
        for i, m in enumerate(batch.messages)
    ]
    assert {r.feed for r in records} == {"vehicle_positions"}


# --- Partitionnement et nommage


def test_batch_spanning_two_hours_is_split() -> None:
    # 10:59:50 et 11:00:10 UTC le 2026-11-21
    early, late = 1_795_258_790, 1_795_258_810
    records = [to_record(_msg(1, _envelope(early)), NOW), to_record(_msg(2, _envelope(late)), NOW)]

    groups = group_records(records)

    assert sorted((g.date, g.hour) for g in groups) == [("2026-11-21", "10"), ("2026-11-21", "11")]


def test_object_key_depends_only_on_offsets() -> None:
    records = [to_record(_msg(o, _envelope(1_795_258_790)), NOW) for o in (42, 40, 41)]
    (group,) = group_records(records)
    key = object_key(group, records)
    assert key == (
        "bronze/gtfs_rt/trip_updates/date=2026-11-21/hour=10/"
        "part-p0-o000000000040-000000000042.parquet"
    )
    assert object_key(group, list(reversed(records))) == key


def test_parquet_has_expected_schema_and_order(tmp_path: Path) -> None:
    records = [to_record(_msg(o, _envelope(1_795_258_790)), NOW) for o in (5, 3, 4)]
    data = records_to_parquet(records)

    path = tmp_path / "f.parquet"
    path.write_bytes(data)
    with duckdb.connect() as con:
        schema = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
        offsets = con.execute("SELECT kafka_offset FROM read_parquet(?)", [str(path)]).fetchall()
    assert [(name, dtype) for name, dtype, *_ in schema] == [
        (n, "TIMESTAMP WITH TIME ZONE" if t == "TIMESTAMPTZ" else t)
        for n, t in PARQUET_COLUMNS.items()
    ]
    assert [o for (o,) in offsets] == [3, 4, 5]


# --- Lots, commits et garanties


def test_flush_writes_files_then_commits_next_offsets(tmp_path: Path) -> None:
    source = FakeSource([_msg(o, _envelope(1_795_258_790)) for o in range(3)])
    store = InMemoryObjectStore()
    writer = BronzeWriter(source, store, max_records=100)

    writer.run(threading.Event(), once=True, idle_polls_before_exit=1)

    assert source.commits == [{(TOPIC, 0): 3}]
    assert source.closed
    (key,) = store.objects
    assert len(_read_parquet(store.objects[key], tmp_path)) == 3


def test_flush_on_size_threshold() -> None:
    source = FakeSource([_msg(o, _envelope(1_795_258_790)) for o in range(5)])
    writer = BronzeWriter(source, InMemoryObjectStore(), max_records=2)

    writer.run(threading.Event(), once=True, idle_polls_before_exit=1)

    assert source.commits == [{(TOPIC, 0): 2}, {(TOPIC, 0): 4}, {(TOPIC, 0): 5}]


def test_flush_on_age_threshold() -> None:
    ticks = iter([0.0, 30.0, 61.0])
    source = FakeSource([])
    writer = BronzeWriter(source, InMemoryObjectStore(), max_age_s=60, clock=lambda: next(ticks))
    writer.add(_msg(0, _envelope(1_795_258_790)))

    assert not writer.should_flush()  # 30 s
    assert writer.should_flush()  # 61 s


def test_no_commit_when_storage_fails() -> None:
    source = FakeSource([_msg(0, _envelope(1_795_258_790))])
    writer = BronzeWriter(source, FailingStore())

    with pytest.raises(OSError, match="MinIO"):
        writer.run(threading.Event(), once=True, idle_polls_before_exit=1)

    assert source.commits == []
    assert source.closed


def test_replaying_same_offsets_overwrites_same_file() -> None:
    store = InMemoryObjectStore()
    for _ in range(2):
        source = FakeSource([_msg(o, _envelope(1_795_258_790)) for o in range(3)])
        BronzeWriter(source, store).run(threading.Event(), once=True, idle_polls_before_exit=1)
    assert len(store.objects) == 1


def test_stop_event_flushes_pending_batch() -> None:
    stop = threading.Event()
    source = FakeSource([_msg(0, _envelope(1_795_258_790))])
    store = InMemoryObjectStore()
    writer = BronzeWriter(source, store, max_records=100)
    writer.add(source.poll(1.0))  # type: ignore[arg-type]
    stop.set()

    writer.run(stop)

    assert len(store.objects) == 1
    assert source.commits == [{(TOPIC, 0): 1}]


def test_offsets_are_committed_per_partition() -> None:
    source = FakeSource(
        [
            _msg(10, _envelope(1_795_258_790), partition=0),
            _msg(4, _envelope(1_795_258_790), partition=2),
            _msg(11, _envelope(1_795_258_790), partition=0),
        ]
    )
    store = InMemoryObjectStore()
    BronzeWriter(source, store).run(threading.Event(), once=True, idle_polls_before_exit=1)

    assert source.commits == [{(TOPIC, 0): 12, (TOPIC, 2): 5}]
    assert len(store.objects) == 2  # un fichier par partition Kafka
