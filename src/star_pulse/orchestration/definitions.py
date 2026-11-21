"""Définitions Dagster du pipeline STAR Pulse.

- bronze : tables `bronze/gtfs_static/*` (ingestion quotidienne) et `bronze/gtfs_rt/*`,
  écrites en continu par `produce-rt` / `consume-rt` hors de Dagster (streaming) ;
- silver / gold : chaque modèle dbt est un asset, chaque test dbt un asset check.

Lancement : `make dagster` (interface sur http://localhost:3000).
"""

import os
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from star_pulse.common.http import build_client
from star_pulse.common.storage import ObjectStore, S3ObjectStore
from star_pulse.config import get_settings
from star_pulse.ingestion.gtfs_static import ingest_gtfs_static
from star_pulse.orchestration.checks import (
    CheckOutcome,
    check_gtfs_volume,
    check_rt_freshness,
    latest_rt_write,
    read_latest_manifest,
    should_rebuild_dbt,
)
from star_pulse.transform.dbt_runner import DBT_PROJECT_DIR, dbt_env

TIMEZONE = "Europe/Paris"
RT_MAX_AGE = timedelta(minutes=10)
# Une seule exécution dbt à la fois : DuckDB n'accepte qu'un écrivain.
DUCKDB_TAG = {"star_pulse/duckdb": "write"}


class LakeResource(dg.ConfigurableResource):  # type: ignore[type-arg]
    """Accès au lakehouse MinIO, avec la configuration de l'application."""

    def store(self) -> ObjectStore:
        """Client du stockage objet."""
        return S3ObjectStore.from_settings(get_settings())


def _check_result(outcome: CheckOutcome, severity: dg.AssetCheckSeverity) -> dg.AssetCheckResult:
    return dg.AssetCheckResult(
        passed=outcome.passed,
        severity=severity,
        description=outcome.description,
        metadata=outcome.metadata,
    )


# --- Bronze -----------------------------------------------------------------

# Une clé d'asset par table bronze, pour que chaque source dbt ait son amont.
GTFS_STATIC_TABLES = (
    "feed_info",
    "routes",
    "stops",
    "trips",
    "stop_times",
    "calendar",
    "calendar_dates",
)
GTFS_RT_FEEDS = ("trip_updates", "vehicle_positions")


def bronze_key(layer: str, table: str) -> dg.AssetKey:
    """Clé d'asset d'une table bronze (`bronze/<layer>/<table>`)."""
    return dg.AssetKey(["bronze", layer, table])


@dg.multi_asset(
    name="gtfs_static_bronze",
    specs=[
        dg.AssetSpec(
            bronze_key("gtfs_static", table),
            group_name="bronze",
            kinds={"python", "s3"},
            description=f"Table GTFS `{table}` en Parquet, une partition par version.",
        )
        for table in GTFS_STATIC_TABLES
    ],
)
def gtfs_static_bronze(lake: LakeResource) -> Iterator[dg.MaterializeResult]:  # type: ignore[type-arg]
    """Ingère le GTFS théorique ; idempotent (ETag puis empreinte du contenu)."""
    settings = get_settings()
    with build_client(settings) as client:
        result = ingest_gtfs_static(lake.store(), client, str(settings.gtfs_static_url))
    for table in GTFS_STATIC_TABLES:
        yield dg.MaterializeResult(
            asset_key=bronze_key("gtfs_static", table),
            metadata={
                "status": result.status.value,
                "content_sha256": result.content_sha256 or "",
            },
        )


@dg.asset_check(
    asset=bronze_key("gtfs_static", "stop_times"),
    description="Volume minimal des tables GTFS principales.",
)
def gtfs_static_volume(lake: LakeResource) -> dg.AssetCheckResult:
    """Vérifie que la dernière version ingérée n'est pas tronquée."""
    manifest = read_latest_manifest(lake.store())
    if manifest is None:
        return dg.AssetCheckResult(passed=False, description="Aucune version GTFS en bronze.")
    return _check_result(check_gtfs_volume(manifest["row_counts"]), dg.AssetCheckSeverity.ERROR)


gtfs_rt_bronze = [
    dg.AssetSpec(
        bronze_key("gtfs_rt", feed),
        group_name="bronze",
        kinds={"kafka", "s3"},
        description=(
            f"Messages GTFS-RT `{feed}` en Parquet, écrits en continu par "
            "`make produce-rt` et `make consume-rt` (streaming, hors Dagster)."
        ),
    )
    for feed in GTFS_RT_FEEDS
]


@dg.asset_check(
    asset=bronze_key("gtfs_rt", "trip_updates"),
    description="Bronze temps réel écrit il y a moins de 10 min.",
)
def gtfs_rt_freshness(lake: LakeResource) -> dg.AssetCheckResult:
    """Alerte si le producteur ou le consumer temps réel est arrêté."""
    now = datetime.now(UTC)
    last = latest_rt_write(lake.store(), "trip_updates", now)
    return _check_result(check_rt_freshness(last, now, RT_MAX_AGE), dg.AssetCheckSeverity.WARN)


# --- Silver / gold (dbt) ----------------------------------------------------


class StarPulseDbtTranslator(DagsterDbtTranslator):
    """Relie les sources dbt aux tables bronze et groupe les modèles par couche."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        if dbt_resource_props["resource_type"] == "source":
            layer = "gtfs_static" if dbt_resource_props["source_name"] == "gtfs" else "gtfs_rt"
            return bronze_key(layer, dbt_resource_props["name"])
        key: dg.AssetKey = super().get_asset_key(dbt_resource_props)
        return key

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        fqn: list[str] = dbt_resource_props.get("fqn", [])
        return fqn[1] if len(fqn) > 2 else None


dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()
if not dbt_project.manifest_path.exists():
    dbt_project.preparer.prepare(dbt_project)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    dagster_dbt_translator=StarPulseDbtTranslator(),
)
def star_pulse_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource) -> Iterator[Any]:
    """Construit et teste le silver et le gold (`dbt build`)."""
    # dbt ne lit pas `.env` : on lui passe la configuration au moment du run.
    os.environ.update(dbt_env(get_settings()))
    yield from dbt.cli(["build"], context=context).stream()


# --- Jobs, schedules, sensor ------------------------------------------------

gtfs_static_job = dg.define_asset_job(
    "gtfs_static_job",
    selection=dg.AssetSelection.assets(gtfs_static_bronze),
    description="Ingestion du GTFS théorique.",
)

transform_job = dg.define_asset_job(
    "transform_job",
    selection=dg.AssetSelection.assets(star_pulse_dbt)
    | dg.AssetSelection.checks(gtfs_rt_freshness),
    tags=DUCKDB_TAG,
    description="Fraîcheur du temps réel, puis silver et gold (dbt build).",
)

gtfs_static_daily = dg.ScheduleDefinition(
    name="gtfs_static_daily",
    job=gtfs_static_job,
    cron_schedule="0 4 * * *",
    execution_timezone=TIMEZONE,
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="Chaque nuit à 4 h, après la republication STAR (≈ 23 h 50).",
)

transform_every_15_min = dg.ScheduleDefinition(
    name="transform_every_15_min",
    job=transform_job,
    cron_schedule="*/15 * * * *",
    execution_timezone=TIMEZONE,
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


@dg.asset_sensor(
    asset_key=bronze_key("gtfs_static", "trips"),
    job=transform_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="Reconstruit silver et gold dès qu'une nouvelle version GTFS est ingérée.",
)
def new_gtfs_version_sensor(
    context: dg.SensorEvaluationContext, asset_event: dg.EventLogEntry
) -> dg.RunRequest | dg.SkipReason:
    """Déclenche dbt seulement si l'ingestion a écrit une nouvelle version."""
    materialization = asset_event.asset_materialization
    metadata = materialization.metadata if materialization else {}
    status = metadata.get("status")
    sha = metadata.get("content_sha256")
    if not should_rebuild_dbt(str(status.value) if status else None):
        return dg.SkipReason("GTFS inchangé : rien à reconstruire.")
    return dg.RunRequest(run_key=f"gtfs-{sha.value if sha else asset_event.run_id}")


defs = dg.Definitions(
    assets=[gtfs_static_bronze, *gtfs_rt_bronze, star_pulse_dbt],
    asset_checks=[gtfs_static_volume, gtfs_rt_freshness],
    jobs=[gtfs_static_job, transform_job],
    schedules=[gtfs_static_daily, transform_every_15_min],
    sensors=[new_gtfs_version_sensor],
    resources={"dbt": DbtCliResource(project_dir=dbt_project), "lake": LakeResource()},
)
