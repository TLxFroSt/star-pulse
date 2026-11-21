"""Règles des contrôles qualité, en fonctions pures (testables sans Dagster)."""

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from star_pulse.common.storage import ObjectStore
from star_pulse.ingestion.gtfs_static import STATE_KEY, VERSIONS_PREFIX

RT_BRONZE_PREFIX = "bronze/gtfs_rt"

# Seuils minimaux d'un GTFS STAR complet (≈ 87 000 courses, ≈ 2 M horaires) :
# en dessous, l'archive est probablement tronquée.
GTFS_MIN_ROWS: Mapping[str, int] = {
    "routes": 50,
    "stops": 1_000,
    "trips": 10_000,
    "stop_times": 200_000,
}


@dataclass(frozen=True)
class CheckOutcome:
    """Résultat d'un contrôle, indépendant de Dagster."""

    passed: bool
    description: str
    metadata: dict[str, Any] = field(default_factory=dict)


def rt_partition_prefixes(feed: str, now: datetime) -> list[str]:
    """Préfixes bronze du jour et de la veille (UTC) pour un flux temps réel.

    Args:
        feed: Nom du flux (`trip_updates`, `vehicle_positions`…).
        now: Instant de référence, avec fuseau.

    Returns:
        Les préfixes à lister, sans parcourir tout l'historique.
    """
    days = [now.date(), (now - timedelta(days=1)).date()]
    return [f"{RT_BRONZE_PREFIX}/{feed}/date={day.isoformat()}/" for day in days]


def latest_rt_write(store: ObjectStore, feed: str, now: datetime) -> datetime | None:
    """Dernière écriture bronze d'un flux temps réel (jour et veille)."""
    dates = [store.latest_modified(p) for p in rt_partition_prefixes(feed, now)]
    return max((d for d in dates if d is not None), default=None)


def check_rt_freshness(
    last_written: datetime | None, now: datetime, max_age: timedelta
) -> CheckOutcome:
    """Le bronze temps réel a-t-il été écrit récemment ?

    Args:
        last_written: Dernière écriture connue, None si aucune.
        now: Instant de référence.
        max_age: Âge maximal toléré.

    Returns:
        Le résultat du contrôle, avec l'âge en minutes.
    """
    if last_written is None:
        return CheckOutcome(
            False, "Aucun fichier temps réel récent : lancer produce-rt / consume-rt."
        )
    age_min = (now - last_written).total_seconds() / 60
    passed = now - last_written <= max_age
    verdict = "à jour" if passed else "en retard"
    return CheckOutcome(
        passed,
        f"Bronze temps réel {verdict} : dernière écriture il y a {age_min:.1f} min.",
        {"age_minutes": round(age_min, 1), "last_written": last_written.isoformat()},
    )


def read_latest_manifest(store: ObjectStore) -> dict[str, Any] | None:
    """Manifeste de la dernière version GTFS ingérée, ou None si aucune."""
    if not store.exists(STATE_KEY):
        return None
    state = json.loads(store.get_bytes(STATE_KEY))
    sha = state.get("content_sha256")
    key = f"{VERSIONS_PREFIX}/{sha}.json"
    if not sha or not store.exists(key):
        return None
    manifest: dict[str, Any] = json.loads(store.get_bytes(key))
    return manifest


def check_gtfs_volume(
    row_counts: Mapping[str, int], minimums: Mapping[str, int] = GTFS_MIN_ROWS
) -> CheckOutcome:
    """Les tables principales du GTFS ont-elles un volume plausible ?

    Args:
        row_counts: Nombre de lignes par table (issu du manifeste).
        minimums: Seuils minimaux par table.

    Returns:
        Le résultat du contrôle, avec les volumes par table.
    """
    too_small = {
        table: row_counts.get(table, 0)
        for table, minimum in minimums.items()
        if row_counts.get(table, 0) < minimum
    }
    metadata = {f"rows_{table}": row_counts.get(table, 0) for table in minimums}
    if too_small:
        return CheckOutcome(False, f"Volume anormalement bas : {too_small}.", metadata)
    return CheckOutcome(True, "Volumes du GTFS théorique plausibles.", metadata)


def should_rebuild_dbt(status: str | None) -> bool:
    """Une ingestion GTFS justifie-t-elle de reconstruire silver et gold ?

    Args:
        status: Statut de l'ingestion (`ingested`, `not_modified`, `duplicate`).

    Returns:
        Vrai seulement pour une nouvelle version du GTFS.
    """
    return status == "ingested"
