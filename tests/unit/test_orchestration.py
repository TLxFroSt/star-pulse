from datetime import UTC, datetime, timedelta

import dagster as dg

from star_pulse.common.storage import InMemoryObjectStore
from star_pulse.orchestration.checks import (
    check_gtfs_volume,
    check_rt_freshness,
    latest_rt_write,
    rt_partition_prefixes,
    should_rebuild_dbt,
)
from star_pulse.orchestration.definitions import (
    GTFS_STATIC_TABLES,
    bronze_key,
    defs,
    new_gtfs_version_sensor,
)

NOW = datetime(2026, 11, 21, 12, 0, tzinfo=UTC)


def test_rt_partition_prefixes_cover_today_and_yesterday() -> None:
    assert rt_partition_prefixes("trip_updates", NOW) == [
        "bronze/gtfs_rt/trip_updates/date=2026-11-21/",
        "bronze/gtfs_rt/trip_updates/date=2026-11-20/",
    ]


def test_latest_rt_write_reads_recent_partitions() -> None:
    store = InMemoryObjectStore()
    store.put_bytes("bronze/gtfs_rt/trip_updates/date=2026-11-21/hour=11/a.parquet", b"x")
    store.put_bytes("bronze/gtfs_rt/trip_updates/date=2026-11-01/hour=11/old.parquet", b"x")
    store.modified["bronze/gtfs_rt/trip_updates/date=2026-11-21/hour=11/a.parquet"] = NOW
    store.modified["bronze/gtfs_rt/trip_updates/date=2026-11-01/hour=11/old.parquet"] = (
        NOW + timedelta(days=1)
    )

    assert latest_rt_write(store, "trip_updates", NOW) == NOW


def test_rt_freshness_passes_when_recent() -> None:
    outcome = check_rt_freshness(NOW - timedelta(minutes=3), NOW, timedelta(minutes=10))
    assert outcome.passed
    assert outcome.metadata["age_minutes"] == 3.0


def test_rt_freshness_fails_when_stale_or_missing() -> None:
    assert not check_rt_freshness(NOW - timedelta(minutes=11), NOW, timedelta(minutes=10)).passed
    assert not check_rt_freshness(None, NOW, timedelta(minutes=10)).passed


def test_gtfs_volume() -> None:
    full = {"routes": 150, "stops": 3000, "trips": 86_000, "stop_times": 2_000_000}
    assert check_gtfs_volume(full).passed
    outcome = check_gtfs_volume({**full, "stop_times": 10})
    assert not outcome.passed
    assert outcome.metadata["rows_stop_times"] == 10


def test_should_rebuild_only_for_new_version() -> None:
    assert should_rebuild_dbt("ingested")
    assert not should_rebuild_dbt("not_modified")
    assert not should_rebuild_dbt("duplicate")
    assert not should_rebuild_dbt(None)


def test_definitions_load() -> None:
    repo = defs.get_repository_def()
    assert {job.name for job in repo.get_all_jobs()} >= {"gtfs_static_job", "transform_job"}
    assert {s.name for s in repo.schedule_defs} == {"gtfs_static_daily", "transform_every_15_min"}
    keys = repo.asset_graph.get_all_asset_keys()
    assert bronze_key("gtfs_static", "stop_times") in keys
    assert dg.AssetKey("int_stop_passages_matched") in keys
    assert dg.AssetKey("fct_punctuality_route_day") in keys


def test_dbt_sources_depend_on_bronze_tables() -> None:
    graph = defs.get_repository_def().asset_graph
    parents = graph.get(dg.AssetKey("stg_gtfs__trips")).parent_keys
    assert parents == {bronze_key("gtfs_static", "trips")}
    assert len(GTFS_STATIC_TABLES) == 7


def _sensor_result(status: str) -> list[dg.RunRequest]:
    instance = dg.DagsterInstance.ephemeral()
    instance.report_runless_asset_event(
        dg.AssetMaterialization(
            asset_key=bronze_key("gtfs_static", "trips"),
            metadata={"status": status, "content_sha256": "abc"},
        )
    )
    context = dg.build_sensor_context(instance=instance, definitions=defs)
    return list(new_gtfs_version_sensor.evaluate_tick(context).run_requests or [])


def test_sensor_triggers_dbt_on_new_version() -> None:
    requests = _sensor_result("ingested")
    assert [r.run_key for r in requests] == ["gtfs-abc"]


def test_sensor_skips_when_gtfs_unchanged() -> None:
    assert _sensor_result("not_modified") == []
