"""Jeu de démonstration : un réseau fictif et son temps réel simulé, chargés en bronze.

Il sert à deux choses :

- la CI : `dbt build` y tourne de bout en bout (bronze → silver → gold, tests
  dbt compris) sans dépendre des flux STAR ;
- une démo locale en quelques minutes, sans attendre des jours de collecte :
  `make demo` puis `make demo-dashboard`.

Le réseau (2 lignes, 13 arrêts, 188 courses) est un GTFS fictif versionné dans
`tests/fixtures/gtfs_demo/`. Le temps réel est simulé course par course : un
retard qui dérive le long du trajet, plus fort aux heures de pointe et sur la
ligne B, quelques incidents, quelques arrêts supprimés. Les instantanés imitent
le flux STAR (seuls les arrêts à venir, un instantané toutes les 30 s au plus)
et passent par les mêmes fonctions que le vrai pipeline (`ingest_gtfs_static`,
`build_batch`, `to_record`, `records_to_parquet`), sans Redpanda.

La simulation est déterministe (graine fixe) et datée sur une semaine fixe :
mêmes chiffres à chaque exécution. Les données vont dans un bucket dédié
(`demo` avec `make demo`) ; le chargement refuse un bucket qui contient déjà le
GTFS réel. Voir `docs/adr/0008-ci-et-jeu-de-demo.md`.

Usage : `python -m star_pulse.demo [--start AAAA-MM-JJ] [--days N] [--seed N]`.
"""

import argparse
import csv
import io
import json
import logging
import random
import zipfile
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from google.transit import gtfs_realtime_pb2

from star_pulse.common.logging import configure_logging
from star_pulse.common.storage import ObjectStore, S3ObjectStore
from star_pulse.config import get_settings
from star_pulse.ingestion.gtfs_rt import Deduplicator, Feed, FeedSource, build_batch
from star_pulse.ingestion.gtfs_static import STATE_KEY, ingest_gtfs_static, read_archive
from star_pulse.streaming.bronze_writer import (
    BronzeRecord,
    ConsumedMessage,
    group_records,
    object_key,
    records_to_parquet,
    to_record,
)

logger = logging.getLogger(__name__)

DEMO_GTFS_DIR = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "gtfs_demo"
DEMO_GTFS_URL = "https://demo.invalid/gtfs_demo.zip"
MARKER_KEY = "_demo.json"

DEMO_START = date(2026, 11, 2)  # un lundi, semaine sans jour férié
DEMO_DAYS = 7
DEMO_SEED = 42

# Cadence des instantanés, comme le producteur réel (un appel toutes les 30 s).
FEED_PERIOD_S = 30
# Une course apparaît dans le flux quelques minutes avant son départ ; ensuite,
# l'instantané qui précède chaque arrêt est émis à son approche.
TRIP_LEAD_S = 300
APPROACH_S = 40

# --- Modèle de retard (secondes). Ordres de grandeur choisis pour donner
# un réseau crédible : ~85 % à l'heure, des pointes marquées, une ligne plus fragile.
START_DELAY = (25.0, 45.0)  # retard au départ : moyenne, écart-type
DRIFT_BASE_S = 8.0  # dérive moyenne par inter-arrêt, hors pointe
DRIFT_PEAK_S = 30.0  # dérive supplémentaire aux heures de pointe (semaine)
DRIFT_SD_S = 20.0
ROUTE_DRIFT_FACTOR = {"DEMO-B": 1.9}  # la ligne B subit plus la circulation
DAY_FACTOR = (0.6, 1.5)  # d'un jour à l'autre (météo, travaux…)
PEAK_HOURS = frozenset({7, 8, 16, 17, 18})
AHEAD_RATE, AHEAD_DRIFT_S = 0.06, -18.0  # conducteur en avance
INCIDENT_RATE, INCIDENT_S = 0.06, (180.0, 600.0)
SKIP_RATE = 0.01  # arrêt intermédiaire non desservi
MIN_DELAY_S = -180


class DemoTargetError(RuntimeError):
    """Le bucket cible contient déjà des données réelles."""


@dataclass(frozen=True)
class Trip:
    """Une course du GTFS : ses arrêts et leurs horaires (secondes depuis le jour de service)."""

    trip_id: str
    route_id: str
    direction_id: int
    service_id: str
    stops: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class Calendar:
    """Jours de circulation d'un service (calendar.txt)."""

    weekdays: tuple[bool, ...]
    start: date
    end: date


@dataclass(frozen=True)
class Network:
    """Ce dont la simulation a besoin du GTFS."""

    timezone: ZoneInfo
    trips: tuple[Trip, ...]
    calendars: Mapping[str, Calendar]
    exceptions: Mapping[tuple[str, date], int]
    stop_coords: Mapping[str, tuple[float, float]]

    def active_services(self, day: date) -> set[str]:
        """Services qui circulent ce jour-là (calendar.txt + calendar_dates.txt)."""
        active = {
            service_id
            for service_id, cal in self.calendars.items()
            if cal.start <= day <= cal.end and cal.weekdays[day.weekday()]
        }
        for (service_id, exception_day), kind in self.exceptions.items():
            if exception_day != day:
                continue
            if kind == 1:
                active.add(service_id)
            else:
                active.discard(service_id)
        return active


@dataclass(frozen=True)
class Passage:
    """Passage simulé d'une course à un arrêt (la « vérité terrain »)."""

    stop_id: str
    stop_sequence: int
    scheduled_at: int  # epoch, secondes
    delay_s: int
    skipped: bool

    @property
    def actual_at(self) -> int:
        """Heure réelle du passage (epoch, secondes)."""
        return self.scheduled_at + self.delay_s


@dataclass(frozen=True)
class DemoResult:
    """Bilan du chargement."""

    days: tuple[date, ...]
    passages: int
    entities: Mapping[Feed, int]
    objects: int


def gtfs_zip(directory: Path = DEMO_GTFS_DIR) -> bytes:
    """Zippe les fichiers du GTFS fictif (contenu et dates fixes : zip reproductible)."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(directory.glob("*.txt")):
            info = zipfile.ZipInfo(path.name, date_time=(2026, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, path.read_bytes())
    return buffer.getvalue()


def _rows(files: Mapping[str, bytes], table: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(files[table].decode("utf-8-sig"))))


def _gtfs_date(value: str) -> date:
    return datetime.strptime(value, "%Y%m%d").date()


def _gtfs_seconds(value: str) -> int:
    hours, minutes, seconds = (int(part) for part in value.split(":"))
    return hours * 3600 + minutes * 60 + seconds


def load_network(files: Mapping[str, bytes]) -> Network:
    """Lit le GTFS (fichiers décompressés, clés sans extension) pour la simulation."""
    stop_times: dict[str, list[tuple[int, str, int]]] = defaultdict(list)
    for row in _rows(files, "stop_times"):
        stop_times[row["trip_id"]].append(
            (int(row["stop_sequence"]), row["stop_id"], _gtfs_seconds(row["arrival_time"]))
        )
    trips = tuple(
        Trip(
            trip_id=row["trip_id"],
            route_id=row["route_id"],
            direction_id=int(row["direction_id"]),
            service_id=row["service_id"],
            stops=tuple((stop, offset) for _, stop, offset in sorted(stop_times[row["trip_id"]])),
        )
        for row in _rows(files, "trips")
    )
    days = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
    calendars = {
        row["service_id"]: Calendar(
            weekdays=tuple(row[d] == "1" for d in days),
            start=_gtfs_date(row["start_date"]),
            end=_gtfs_date(row["end_date"]),
        )
        for row in _rows(files, "calendar")
    }
    exceptions = {
        (row["service_id"], _gtfs_date(row["date"])): int(row["exception_type"])
        for row in _rows(files, "calendar_dates")
    }
    coords = {
        row["stop_id"]: (float(row["stop_lat"]), float(row["stop_lon"]))
        for row in _rows(files, "stops")
    }
    timezone = ZoneInfo(_rows(files, "agency")[0]["agency_timezone"])
    return Network(timezone, trips, calendars, exceptions, coords)


def service_day_start(day: date, tz: ZoneInfo) -> datetime:
    """Origine des horaires GTFS d'un jour : « midi moins 12 h » en heure locale."""
    return datetime.combine(day, time(12), tzinfo=tz) - timedelta(hours=12)


def simulate_trip(
    trip: Trip, day: date, tz: ZoneInfo, rng: random.Random, day_factor: float = 1.0
) -> list[Passage]:
    """Simule les passages d'une course : un retard qui dérive d'arrêt en arrêt."""
    origin = int(service_day_start(day, tz).timestamp())
    departure_hour = (trip.stops[0][1] // 3600) % 24
    peak = day.weekday() < 5 and departure_hour in PEAK_HOURS
    drift = (
        (DRIFT_BASE_S + (DRIFT_PEAK_S if peak else 0.0))
        * ROUTE_DRIFT_FACTOR.get(trip.route_id, 1.0)
        * day_factor
    )
    if rng.random() < AHEAD_RATE:
        drift = AHEAD_DRIFT_S
    incident_at = rng.randrange(1, len(trip.stops)) if rng.random() < INCIDENT_RATE else -1

    delay = rng.gauss(*START_DELAY)
    passages = []
    last = len(trip.stops) - 1
    for index, (stop_id, offset) in enumerate(trip.stops):
        if index:
            delay += rng.gauss(drift, DRIFT_SD_S)
        if index == incident_at:
            delay += rng.uniform(*INCIDENT_S)
        delay = max(delay, MIN_DELAY_S)
        skipped = 0 < index < last and rng.random() < SKIP_RATE
        passages.append(Passage(stop_id, index + 1, origin + offset, round(delay), skipped))
    return passages


def snapshot_plan(passages: Sequence[Passage]) -> list[tuple[int, int]]:
    """Instants des instantanés d'une course et premier arrêt encore à venir dans chacun.

    Le dernier instantané qui cite un arrêt est émis juste avant le passage : sa
    prévision pour cet arrêt est l'heure réelle. Deux arrêts trop proches pour
    tomber dans deux instantanés distincts partagent le même.

    Returns:
        Une liste de `(timestamp de l'instantané, index du prochain arrêt)`.
    """
    plan: list[tuple[int, int]] = []
    for index, passage in enumerate(passages):
        lead = TRIP_LEAD_S if index == 0 else APPROACH_S
        timestamp = (passage.actual_at - lead) // FEED_PERIOD_S * FEED_PERIOD_S
        if plan and timestamp <= plan[-1][0]:
            continue
        plan.append((timestamp, index))
    return plan


def trip_update_entity(
    trip: Trip, passages: Sequence[Passage], next_index: int, timestamp: int
) -> gtfs_realtime_pb2.FeedEntity:
    """TripUpdate d'un instantané : arrêts à venir, retard courant propagé."""
    entity = gtfs_realtime_pb2.FeedEntity(id=trip.trip_id)
    update = entity.trip_update
    update.trip.trip_id = trip.trip_id
    update.trip.route_id = trip.route_id
    update.trip.direction_id = trip.direction_id
    update.timestamp = timestamp
    current_delay = passages[next_index].delay_s
    for index in range(next_index, len(passages)):
        passage = passages[index]
        stu = update.stop_time_update.add(
            stop_id=passage.stop_id, stop_sequence=passage.stop_sequence
        )
        if passage.skipped:
            stu.schedule_relationship = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
            continue
        uncertainty = 0 if index == next_index else 60
        for event in (stu.arrival, stu.departure):
            event.time = passage.scheduled_at + current_delay
            event.delay = current_delay
            event.uncertainty = uncertainty
    return entity


def vehicle_entity(
    trip: Trip,
    passages: Sequence[Passage],
    next_index: int,
    timestamp: int,
    coords: Mapping[str, tuple[float, float]],
) -> gtfs_realtime_pb2.FeedEntity:
    """Position du véhicule au même instant : au dernier arrêt passé, vers le suivant."""
    vehicle_id = f"bus-{trip.trip_id}"
    entity = gtfs_realtime_pb2.FeedEntity(id=vehicle_id)
    position = entity.vehicle
    position.trip.trip_id = trip.trip_id
    position.trip.route_id = trip.route_id
    position.trip.direction_id = trip.direction_id
    position.vehicle.id = vehicle_id
    latitude, longitude = coords[passages[max(next_index - 1, 0)].stop_id]
    position.position.latitude = latitude
    position.position.longitude = longitude
    position.current_stop_sequence = passages[next_index].stop_sequence
    position.stop_id = passages[next_index].stop_id
    position.current_status = gtfs_realtime_pb2.VehiclePosition.IN_TRANSIT_TO
    position.timestamp = timestamp
    return entity


def simulate_feeds(
    network: Network, days: Sequence[date], seed: int
) -> tuple[dict[Feed, list[gtfs_realtime_pb2.FeedMessage]], int]:
    """Simule les flux TripUpdates et VehiclePositions sur les jours demandés.

    Returns:
        Les messages de chaque flux, par instant croissant, et le nombre de passages simulés.
    """
    rng = random.Random(seed)
    entities: dict[Feed, dict[int, list[gtfs_realtime_pb2.FeedEntity]]] = {
        Feed.TRIP_UPDATES: defaultdict(list),
        Feed.VEHICLE_POSITIONS: defaultdict(list),
    }
    n_passages = 0
    for day in days:
        services = network.active_services(day)
        day_factor = rng.uniform(*DAY_FACTOR)
        for trip in network.trips:
            if trip.service_id not in services:
                continue
            passages = simulate_trip(trip, day, network.timezone, rng, day_factor)
            n_passages += len(passages)
            for timestamp, next_index in snapshot_plan(passages):
                entities[Feed.TRIP_UPDATES][timestamp].append(
                    trip_update_entity(trip, passages, next_index, timestamp)
                )
                entities[Feed.VEHICLE_POSITIONS][timestamp].append(
                    vehicle_entity(trip, passages, next_index, timestamp, network.stop_coords)
                )

    feeds: dict[Feed, list[gtfs_realtime_pb2.FeedMessage]] = {}
    for feed, by_time in entities.items():
        messages = []
        for timestamp in sorted(by_time):
            message = gtfs_realtime_pb2.FeedMessage()
            message.header.gtfs_realtime_version = "2.0"
            message.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
            message.header.timestamp = timestamp
            message.entity.extend(by_time[timestamp])
            messages.append(message)
        feeds[feed] = messages
    return feeds, n_passages


def rt_records(
    feeds: Mapping[Feed, Sequence[gtfs_realtime_pb2.FeedMessage]], topics: Mapping[Feed, str]
) -> list[BronzeRecord]:
    """Passe les flux simulés par le chemin du producteur puis du consumer (sans Redpanda)."""
    records = []
    for feed, messages in feeds.items():
        source = FeedSource(feed, f"https://demo.invalid/{feed.value}", topics[feed])
        dedup = Deduplicator()
        offset = 0
        for message in messages:
            fetched_at = datetime.fromtimestamp(message.header.timestamp + 2, tz=UTC)
            batch = build_batch(source, message, dedup, fetched_at)
            if batch is None:
                continue
            dedup.commit(batch)
            ingested_at = fetched_at + timedelta(seconds=3)
            for rt in batch.messages:
                consumed = ConsumedMessage(
                    rt.topic, 0, offset, int(fetched_at.timestamp() * 1000), rt.value
                )
                records.append(to_record(consumed, ingested_at))
                offset += 1
    return records


def check_target(store: ObjectStore) -> None:
    """Refuse un bucket qui contient déjà un GTFS réel (pas de mélange avec la démo).

    Raises:
        DemoTargetError: Si le bucket a un GTFS en bronze sans marqueur de démo.
    """
    if store.exists(STATE_KEY) and not store.exists(MARKER_KEY):
        raise DemoTargetError(
            "Ce bucket contient déjà des données STAR : chargez la démo dans un bucket "
            "dédié (make demo utilise le bucket « demo »)."
        )


def load_demo(
    store: ObjectStore,
    *,
    topics: Mapping[Feed, str],
    start: date = DEMO_START,
    days: int = DEMO_DAYS,
    seed: int = DEMO_SEED,
    gtfs_dir: Path = DEMO_GTFS_DIR,
) -> DemoResult:
    """Écrit le GTFS fictif et le temps réel simulé en bronze.

    Idempotent : les clés et contenus sont déterministes, une relance réécrit
    les mêmes objets.

    Args:
        store: Stockage objet cible (bucket dédié à la démo).
        topics: Topic Kafka de chaque flux, recopié dans les métadonnées bronze.
        start: Premier jour simulé.
        days: Nombre de jours simulés.
        seed: Graine de la simulation.
        gtfs_dir: Dossier du GTFS fictif.

    Returns:
        Le bilan du chargement.

    Raises:
        DemoTargetError: Si le bucket contient déjà des données réelles.
    """
    check_target(store)
    simulated_days = tuple(start + timedelta(days=i) for i in range(days))
    marker = {"start": start.isoformat(), "days": days, "seed": seed}
    store.put_bytes(MARKER_KEY, json.dumps(marker).encode(), content_type="application/json")

    archive = gtfs_zip(gtfs_dir)

    def serve_zip(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=archive)

    # Même chemin que l'ingestion réelle, le téléchargement en moins.
    ingested_at = datetime.combine(start - timedelta(days=1), time(4), tzinfo=UTC)
    with httpx.Client(transport=httpx.MockTransport(serve_zip)) as client:
        ingest_gtfs_static(store, client, DEMO_GTFS_URL, now=ingested_at)

    network = load_network(read_archive(archive))
    feeds, n_passages = simulate_feeds(network, simulated_days, seed)
    records = rt_records(feeds, topics)
    groups = group_records(records)
    for group in sorted(groups):
        rows = groups[group]
        store.put_bytes(
            object_key(group, rows),
            records_to_parquet(rows),
            content_type="application/vnd.apache.parquet",
        )

    entities = {feed: sum(len(m.entity) for m in messages) for feed, messages in feeds.items()}
    return DemoResult(simulated_days, n_passages, entities, len(groups))


def main(argv: Sequence[str] | None = None) -> DemoResult:
    """Point d'entrée CLI : charge la démo dans le bucket de la configuration."""
    parser = argparse.ArgumentParser(description="Charge le jeu de démonstration en bronze.")
    parser.add_argument("--start", type=date.fromisoformat, default=DEMO_START)
    parser.add_argument("--days", type=int, default=DEMO_DAYS)
    parser.add_argument("--seed", type=int, default=DEMO_SEED)
    args = parser.parse_args(argv)
    if args.days < 1:
        parser.error("--days doit être positif")

    settings = get_settings()
    configure_logging(settings.log_level, json_format=settings.log_json)
    store = S3ObjectStore.from_settings(settings)
    store.ensure_bucket()
    topics = {
        Feed.TRIP_UPDATES: settings.topic_trip_updates,
        Feed.VEHICLE_POSITIONS: settings.topic_vehicle_positions,
    }
    try:
        result = load_demo(store, topics=topics, start=args.start, days=args.days, seed=args.seed)
    except DemoTargetError as exc:
        logger.error(str(exc), extra={"bucket": settings.s3_bucket})
        raise SystemExit(1) from exc
    logger.info(
        "Jeu de démo chargé en bronze",
        extra={
            "bucket": settings.s3_bucket,
            "first_day": result.days[0].isoformat(),
            "last_day": result.days[-1].isoformat(),
            "passages": result.passages,
            "trip_updates": result.entities[Feed.TRIP_UPDATES],
            "objects": result.objects,
        },
    )
    return result


if __name__ == "__main__":
    main()
