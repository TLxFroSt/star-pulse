"""Tests du producteur GTFS-RT (sans réseau ni broker)."""

import json
from collections.abc import Sequence
from datetime import UTC, datetime

import httpx
import pytest
from google.transit import gtfs_realtime_pb2

from star_pulse.ingestion.gtfs_rt import (
    Deduplicator,
    Feed,
    FeedSource,
    InvalidFeedError,
    PublishError,
    RtMessage,
    build_batch,
    decode_feed,
    entity_key,
    entity_version,
    poll_once,
)

NOW = datetime(2026, 11, 21, 10, 0, tzinfo=UTC)
SOURCES = [
    FeedSource(Feed.TRIP_UPDATES, "https://rt.test/trip_update", "t.trip-updates"),
    FeedSource(Feed.VEHICLE_POSITIONS, "https://rt.test/vehicle_position", "t.vehicle-positions"),
    FeedSource(Feed.ALERTS, "https://rt.test/alerts", "t.alerts"),
]


class ListPublisher:
    """Publisher en mémoire."""

    def __init__(self, *, fail: bool = False) -> None:
        self.messages: list[RtMessage] = []
        self.fail = fail

    def publish(self, messages: Sequence[RtMessage]) -> None:
        if self.fail:
            raise PublishError("broker indisponible")
        self.messages.extend(messages)


def _server(payloads: dict[str, bytes]) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        name = request.url.path.strip("/")
        if name not in payloads:
            return httpx.Response(404)
        return httpx.Response(200, content=payloads[name])

    return httpx.Client(transport=httpx.MockTransport(handler))


def _with_header_timestamp(payload: bytes, timestamp: int) -> bytes:
    feed = decode_feed(payload)
    feed.header.timestamp = timestamp
    return feed.SerializeToString()


# --- Décodage


def test_decode_real_trip_updates(rt_payloads: dict[str, bytes]) -> None:
    feed = decode_feed(rt_payloads["trip_update"])
    assert feed.header.incrementality == gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    assert len(feed.entity) == 8
    assert all(e.HasField("trip_update") for e in feed.entity)


def test_decode_rejects_garbage() -> None:
    with pytest.raises(InvalidFeedError):
        decode_feed(b"<html>Bad gateway</html>")


# --- Clés et versions


def test_keys_are_business_identifiers(rt_payloads: dict[str, bytes]) -> None:
    trip = decode_feed(rt_payloads["trip_update"]).entity[0]
    vehicle = decode_feed(rt_payloads["vehicle_position"]).entity[0]

    assert entity_key(Feed.TRIP_UPDATES, trip) == trip.trip_update.trip.trip_id
    assert entity_key(Feed.VEHICLE_POSITIONS, vehicle) == vehicle.vehicle.vehicle.id
    assert entity_version(Feed.TRIP_UPDATES, trip) == str(trip.trip_update.timestamp)


def test_alert_version_follows_content(rt_payloads: dict[str, bytes]) -> None:
    alert = decode_feed(rt_payloads["alerts"]).entity[0]
    before = entity_version(Feed.ALERTS, alert)
    alert.alert.header_text.translation[0].text += " (mise à jour)"
    assert entity_version(Feed.ALERTS, alert) != before


# --- Messages et déduplication


def _publish(
    source: FeedSource, feed: gtfs_realtime_pb2.FeedMessage, dedup: Deduplicator
) -> list[RtMessage] | None:
    """Construit le lot et le marque publié, comme le fait `poll_once`."""
    batch = build_batch(source, feed, dedup, NOW)
    if batch is None:
        return None
    dedup.commit(batch)
    return batch.messages


def test_build_messages_envelope(rt_payloads: dict[str, bytes]) -> None:
    feed = decode_feed(rt_payloads["trip_update"])

    messages = _publish(SOURCES[0], feed, Deduplicator())

    assert messages is not None
    assert len(messages) == 8
    first = messages[0]
    payload = json.loads(first.value)
    assert first.topic == "t.trip-updates"
    assert first.key == payload["entity_key"] == payload["entity"]["trip_update"]["trip"]["trip_id"]
    assert payload["feed"] == "trip_updates"
    assert payload["feed_timestamp"] == feed.header.timestamp
    assert payload["fetched_at"] == NOW.isoformat()


def test_same_feed_twice_publishes_nothing(rt_payloads: dict[str, bytes]) -> None:
    feed = decode_feed(rt_payloads["vehicle_position"])
    dedup = Deduplicator()

    assert _publish(SOURCES[1], feed, dedup)
    assert _publish(SOURCES[1], feed, dedup) is None  # en-tête inchangé


def test_only_changed_entities_are_republished(rt_payloads: dict[str, bytes]) -> None:
    dedup = Deduplicator()
    feed = decode_feed(rt_payloads["vehicle_position"])
    _publish(SOURCES[1], feed, dedup)

    feed.header.timestamp += 30
    feed.entity[0].vehicle.timestamp += 30
    messages = _publish(SOURCES[1], feed, dedup)

    assert messages is not None
    assert [m.key for m in messages] == [feed.entity[0].vehicle.vehicle.id]


def test_entity_that_reappears_is_republished(rt_payloads: dict[str, bytes]) -> None:
    dedup = Deduplicator()
    feed = decode_feed(rt_payloads["vehicle_position"])
    _publish(SOURCES[1], feed, dedup)

    removed = feed.entity.pop()
    feed.header.timestamp += 30
    assert _publish(SOURCES[1], feed, dedup) == []

    feed.entity.append(removed)
    feed.header.timestamp += 30
    assert [m.key for m in _publish(SOURCES[1], feed, dedup) or []] == [removed.vehicle.vehicle.id]


def test_deleted_entities_are_skipped(rt_payloads: dict[str, bytes]) -> None:
    feed = decode_feed(rt_payloads["alerts"])
    feed.entity[0].is_deleted = True
    assert len(_publish(SOURCES[2], feed, Deduplicator()) or []) == len(feed.entity) - 1


# --- Cycle de collecte


def test_poll_once_publishes_all_feeds(rt_payloads: dict[str, bytes]) -> None:
    publisher = ListPublisher()
    with _server(rt_payloads) as client:
        counts = poll_once(SOURCES, client, publisher, Deduplicator(), now=NOW)

    assert counts == {Feed.TRIP_UPDATES: 8, Feed.VEHICLE_POSITIONS: 8, Feed.ALERTS: 4}
    assert {m.topic for m in publisher.messages} == {s.topic for s in SOURCES}


def test_poll_once_isolates_a_failing_feed(rt_payloads: dict[str, bytes]) -> None:
    payloads = {**rt_payloads, "alerts": b"not protobuf"}
    payloads.pop("vehicle_position")  # 404
    publisher = ListPublisher()
    with _server(payloads) as client:
        counts = poll_once(SOURCES, client, publisher, Deduplicator(), now=NOW)

    assert counts == {Feed.TRIP_UPDATES: 8}


def test_failed_publish_is_retried_next_cycle(rt_payloads: dict[str, bytes]) -> None:
    """Un échec de publication ne doit pas marquer les entités comme déjà envoyées."""
    dedup = Deduplicator()
    with _server(rt_payloads) as client:
        failing = ListPublisher(fail=True)
        assert poll_once(SOURCES[:1], client, failing, dedup, now=NOW) == {}

        publisher = ListPublisher()
        assert poll_once(SOURCES[:1], client, publisher, dedup, now=NOW) == {Feed.TRIP_UPDATES: 8}


def test_unchanged_header_is_skipped_across_cycles(rt_payloads: dict[str, bytes]) -> None:
    dedup, publisher = Deduplicator(), ListPublisher()
    with _server(rt_payloads) as client:
        poll_once(SOURCES[:1], client, publisher, dedup, now=NOW)
    later = {"trip_update": _with_header_timestamp(rt_payloads["trip_update"], 1)}
    with _server(later) as client:
        counts = poll_once(SOURCES[:1], client, publisher, dedup, now=NOW)

    assert counts == {Feed.TRIP_UPDATES: 0}  # en-tête changé, mais aucune entité modifiée
