"""Tests du jeu de démonstration (réseau fictif + temps réel simulé)."""

import json
import random
from datetime import date

import pytest
from google.transit import gtfs_realtime_pb2

from star_pulse import demo
from star_pulse.common.storage import InMemoryObjectStore
from star_pulse.ingestion.gtfs_rt import Feed
from star_pulse.ingestion.gtfs_static import STATE_KEY, VERSIONS_PREFIX, read_archive

TOPICS = {Feed.TRIP_UPDATES: "tu", Feed.VEHICLE_POSITIONS: "vp"}
MONDAY = date(2026, 11, 16)


@pytest.fixture(scope="module")
def network() -> demo.Network:
    return demo.load_network(read_archive(demo.gtfs_zip()))


def test_network_reads_the_fictional_gtfs(network: demo.Network) -> None:
    assert len(network.trips) == 188
    assert {t.route_id for t in network.trips} == {"DEMO-A", "DEMO-B"}
    assert all(len(t.stops) == 7 for t in network.trips)


@pytest.mark.parametrize(
    ("day", "expected"),
    [
        (MONDAY, {"SEM"}),
        (date(2026, 11, 21), {"WE"}),
        (date(2026, 12, 25), {"WE"}),  # férié : service du dimanche
        (date(2028, 1, 3), set()),  # hors validité
    ],
)
def test_active_services_follow_the_calendar(
    network: demo.Network, day: date, expected: set[str]
) -> None:
    assert network.active_services(day) == expected


def test_gtfs_zip_is_reproducible() -> None:
    assert demo.gtfs_zip() == demo.gtfs_zip()


def test_simulated_trip_starts_at_its_schedule(network: demo.Network) -> None:
    trip = next(t for t in network.trips if t.trip_id == "A-SEM-0-0600")
    passages = demo.simulate_trip(trip, MONDAY, network.timezone, random.Random(1))

    # 06:00 à Paris en novembre = 05:00 UTC.
    assert passages[0].scheduled_at == 1_794_805_200
    assert [p.stop_sequence for p in passages] == list(range(1, 8))
    assert not passages[0].skipped and not passages[-1].skipped
    assert all(p.delay_s >= demo.MIN_DELAY_S for p in passages)


def test_last_snapshot_before_a_stop_predicts_the_actual_time(network: demo.Network) -> None:
    trip = network.trips[0]
    passages = demo.simulate_trip(trip, MONDAY, network.timezone, random.Random(7))
    plan = demo.snapshot_plan(passages)

    timestamps = [ts for ts, _ in plan]
    assert timestamps == sorted(set(timestamps))
    last_prediction: dict[str, int] = {}
    for timestamp, next_index in plan:
        assert timestamp < passages[next_index].actual_at
        entity = demo.trip_update_entity(trip, passages, next_index, timestamp)
        assert entity.trip_update.timestamp == timestamp
        for stu in entity.trip_update.stop_time_update:
            if stu.HasField("arrival"):
                last_prediction[stu.stop_id] = stu.arrival.time
    for passage in passages:
        if not passage.skipped:
            assert last_prediction[passage.stop_id] == passage.actual_at


def test_skipped_stop_has_no_predicted_time(network: demo.Network) -> None:
    trip = network.trips[0]
    passages = demo.simulate_trip(trip, MONDAY, network.timezone, random.Random(7))
    passages[3] = demo.Passage(
        passages[3].stop_id, passages[3].stop_sequence, passages[3].scheduled_at, 0, skipped=True
    )

    entity = demo.trip_update_entity(trip, passages, 0, passages[0].scheduled_at - 300)

    stu = entity.trip_update.stop_time_update[3]
    skipped = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
    assert stu.schedule_relationship == skipped
    assert not stu.HasField("arrival") and not stu.HasField("departure")


def test_simulation_is_deterministic(network: demo.Network) -> None:
    def run(seed: int) -> list[bytes]:
        feeds, _ = demo.simulate_feeds(network, [MONDAY], seed)
        return [m.SerializeToString() for m in feeds[Feed.TRIP_UPDATES]]

    assert run(3) == run(3)
    assert run(3) != run(4)


def test_load_demo_writes_static_and_rt_bronze() -> None:
    store = InMemoryObjectStore()

    result = demo.load_demo(store, topics=TOPICS, start=MONDAY, days=2)

    assert result.days == (MONDAY, date(2026, 11, 17))
    assert result.passages == 2 * 128 * 7  # 128 courses de semaine par jour
    assert result.entities[Feed.TRIP_UPDATES] > 0
    assert store.exists(STATE_KEY)
    assert store.list_keys(f"{VERSIONS_PREFIX}/")
    tu_keys = store.list_keys("bronze/gtfs_rt/trip_updates/")
    assert tu_keys[0].startswith("bronze/gtfs_rt/trip_updates/date=2026-11-16/hour=04/")
    assert store.list_keys("bronze/gtfs_rt/vehicle_positions/date=2026-11-17/")
    assert json.loads(store.get_bytes(demo.MARKER_KEY)) == {
        "start": "2026-11-16",
        "days": 2,
        "seed": demo.DEMO_SEED,
    }


def test_load_demo_is_idempotent() -> None:
    store = InMemoryObjectStore()
    demo.load_demo(store, topics=TOPICS, start=MONDAY, days=1)
    first = dict(store.objects)

    demo.load_demo(store, topics=TOPICS, start=MONDAY, days=1)

    assert store.objects == first


def test_load_demo_refuses_a_bucket_with_real_data() -> None:
    store = InMemoryObjectStore()
    store.put_bytes(STATE_KEY, b'{"etag": "", "content_sha256": "abc"}')

    with pytest.raises(demo.DemoTargetError):
        demo.load_demo(store, topics=TOPICS, start=MONDAY, days=1)
    assert not store.exists(demo.MARKER_KEY)
