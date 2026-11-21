"""Producteur GTFS-RT : interroge les flux temps réel STAR et publie sur Redpanda.

À chaque cycle (intervalle ≥ 30 s), pour chacun des trois flux :

1. Téléchargement (User-Agent, retries) puis décodage protobuf.
2. Si le timestamp d'en-tête n'a pas bougé depuis le cycle précédent : rien à publier.
3. Sinon, seules les entités nouvelles ou modifiées sont publiées, une par message,
   en JSON, avec pour clé Kafka l'identifiant de l'entité (ordre garanti par course /
   véhicule).

La mémoire de déduplication vit dans le processus : après un redémarrage, quelques
doublons peuvent être republiés. Ils sont éliminés en aval par la même clé
`(entity_key, entity_version)` — voir `docs/adr/0003-messages-gtfs-rt.md`.

Usage : `python -m star_pulse.ingestion.gtfs_rt [--once]` (ou `make produce-rt`).
"""

import argparse
import hashlib
import json
import logging
import signal
import sys
import threading
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from types import FrameType
from typing import Any, Protocol

import httpx
from confluent_kafka import KafkaError, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

from star_pulse.common.http import build_client, get_with_retry
from star_pulse.common.kafka import BrokerUnavailableError, check_broker
from star_pulse.common.logging import configure_logging
from star_pulse.config import Settings, get_settings

logger = logging.getLogger(__name__)

TOPIC_PARTITIONS = 3
TOPIC_RETENTION_MS = 7 * 24 * 3600 * 1000


class Feed(StrEnum):
    """Flux GTFS-RT collectés."""

    TRIP_UPDATES = "trip_updates"
    VEHICLE_POSITIONS = "vehicle_positions"
    ALERTS = "alerts"


class InvalidFeedError(ValueError):
    """Réponse impossible à décoder en `FeedMessage` GTFS-RT."""


class PublishError(RuntimeError):
    """Au moins un message n'a pas été acquitté par le broker."""


@dataclass(frozen=True)
class FeedSource:
    """Un flux à interroger et le topic où le publier."""

    feed: Feed
    url: str
    topic: str


@dataclass(frozen=True)
class RtMessage:
    """Message prêt à publier."""

    topic: str
    key: str
    value: bytes


def feed_sources(settings: Settings) -> list[FeedSource]:
    """Associe chaque flux de la configuration à son topic."""
    return [
        FeedSource(
            Feed.TRIP_UPDATES, str(settings.gtfs_rt_trip_updates_url), settings.topic_trip_updates
        ),
        FeedSource(
            Feed.VEHICLE_POSITIONS,
            str(settings.gtfs_rt_vehicle_positions_url),
            settings.topic_vehicle_positions,
        ),
        FeedSource(Feed.ALERTS, str(settings.gtfs_rt_alerts_url), settings.topic_alerts),
    ]


def decode_feed(data: bytes) -> gtfs_realtime_pb2.FeedMessage:
    """Décode un flux GTFS-RT.

    Raises:
        InvalidFeedError: Si le contenu n'est pas un `FeedMessage` valide.
    """
    message = gtfs_realtime_pb2.FeedMessage()
    try:
        message.ParseFromString(data)
    except DecodeError as exc:
        raise InvalidFeedError("Flux GTFS-RT illisible") from exc
    if not message.HasField("header"):
        raise InvalidFeedError("Flux GTFS-RT sans en-tête")
    return message


def entity_key(feed: Feed, entity: gtfs_realtime_pb2.FeedEntity) -> str:
    """Identifiant métier stable d'une entité (clé Kafka)."""
    if feed is Feed.TRIP_UPDATES and entity.trip_update.trip.trip_id:
        return entity.trip_update.trip.trip_id
    if feed is Feed.VEHICLE_POSITIONS and entity.vehicle.vehicle.id:
        return entity.vehicle.vehicle.id
    return entity.id


def entity_version(feed: Feed, entity: gtfs_realtime_pb2.FeedEntity) -> str:
    """Version d'une entité : son timestamp, ou à défaut un hash de son contenu."""
    if feed is Feed.TRIP_UPDATES and entity.trip_update.HasField("timestamp"):
        return str(entity.trip_update.timestamp)
    if feed is Feed.VEHICLE_POSITIONS and entity.vehicle.HasField("timestamp"):
        return str(entity.vehicle.timestamp)
    # Les alertes n'ont pas de timestamp : le contenu sérialisé fait office de version.
    return hashlib.sha256(entity.SerializeToString(deterministic=True)).hexdigest()[:16]


@dataclass(frozen=True)
class Batch:
    """Messages d'un flux pour un cycle, et l'instantané à mémoriser une fois publiés."""

    feed: Feed
    header_timestamp: int
    messages: list[RtMessage]
    snapshot: dict[str, str]


@dataclass
class Deduplicator:
    """Mémorise ce qui a déjà été publié, flux par flux.

    La mémoire n'est mise à jour qu'après une publication réussie (`commit`) : un échec
    du broker n'empêche pas de republier les mêmes entités au cycle suivant. Les flux
    étant des `FULL_DATASET`, la mémoire d'un flux est remplacée par l'instantané courant
    et reste bornée à la taille du flux.
    """

    _headers: dict[Feed, int] = field(default_factory=dict)
    _seen: dict[Feed, dict[str, str]] = field(default_factory=dict)

    def header_changed(self, feed: Feed, timestamp: int) -> bool:
        """Vrai si le timestamp d'en-tête diffère du dernier publié pour ce flux."""
        return self._headers.get(feed) != timestamp

    def is_new(self, feed: Feed, key: str, version: str) -> bool:
        """Vrai si cette version de l'entité n'a pas encore été publiée."""
        return self._seen.get(feed, {}).get(key) != version

    def commit(self, batch: Batch) -> None:
        """Enregistre un lot comme publié."""
        self._seen[batch.feed] = batch.snapshot
        self._headers[batch.feed] = batch.header_timestamp


def build_batch(
    source: FeedSource,
    feed_message: gtfs_realtime_pb2.FeedMessage,
    dedup: Deduplicator,
    fetched_at: datetime,
) -> Batch | None:
    """Transforme un flux décodé en messages JSON, après déduplication.

    Returns:
        Le lot à publier, ou `None` si l'en-tête n'a pas changé depuis la dernière publication.
    """
    header_ts = feed_message.header.timestamp
    if not dedup.header_changed(source.feed, header_ts):
        return None

    snapshot: dict[str, str] = {}
    messages = []
    for entity in feed_message.entity:
        if entity.is_deleted:
            continue
        key, version = entity_key(source.feed, entity), entity_version(source.feed, entity)
        snapshot[key] = version
        if not dedup.is_new(source.feed, key, version):
            continue
        envelope: dict[str, Any] = {
            "feed": source.feed.value,
            "feed_timestamp": header_ts,
            "fetched_at": fetched_at.isoformat(),
            "entity_key": key,
            "entity_version": version,
            "entity": MessageToDict(entity, preserving_proto_field_name=True),
        }
        value = json.dumps(envelope, ensure_ascii=False, separators=(",", ":")).encode()
        messages.append(RtMessage(source.topic, key, value))
    return Batch(source.feed, header_ts, messages, snapshot)


class Publisher(Protocol):
    """Destination des messages (Kafka en production, liste en mémoire en test)."""

    def publish(self, messages: Sequence[RtMessage]) -> None:
        """Publie les messages et attend leur acquittement."""
        ...


class KafkaPublisher:
    """Publication sur Redpanda via librdkafka (producteur idempotent)."""

    def __init__(self, bootstrap_servers: str, *, flush_timeout_s: float = 30.0) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "star-pulse-gtfs-rt",
                # Pas de doublon ni de réordonnancement en cas de retry réseau.
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "zstd",
                "linger.ms": 50,
            }
        )
        self._flush_timeout_s = flush_timeout_s

    def publish(self, messages: Sequence[RtMessage]) -> None:
        errors: list[KafkaError] = []

        def on_delivery(err: KafkaError | None, _msg: Message) -> None:
            if err is not None:
                errors.append(err)

        for message in messages:
            self._producer.produce(
                message.topic, key=message.key, value=message.value, on_delivery=on_delivery
            )
            self._producer.poll(0)
        remaining = self._producer.flush(self._flush_timeout_s)
        if errors or remaining:
            raise PublishError(f"{len(errors)} échec(s), {remaining} message(s) non acquitté(s)")


def ensure_topics(bootstrap_servers: str, topics: Sequence[str]) -> None:
    """Crée les topics manquants (idempotent)."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics)
    missing = [t for t in topics if t not in existing]
    if not missing:
        return
    futures = admin.create_topics(
        [
            NewTopic(
                t,
                num_partitions=TOPIC_PARTITIONS,
                replication_factor=1,
                config={"retention.ms": str(TOPIC_RETENTION_MS)},
            )
            for t in missing
        ]
    )
    for topic, future in futures.items():
        try:
            future.result()
            logger.info("Topic créé", extra={"topic": topic})
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def poll_once(
    sources: Sequence[FeedSource],
    client: httpx.Client,
    publisher: Publisher,
    dedup: Deduplicator,
    *,
    now: datetime | None = None,
) -> dict[Feed, int]:
    """Un cycle de collecte : chaque flux est traité indépendamment.

    Returns:
        Le nombre de messages publiés par flux (absent si le flux a échoué).
    """
    fetched_at = now or datetime.now(tz=UTC)
    published: dict[Feed, int] = {}
    for source in sources:
        try:
            response = get_with_retry(client, source.url)
            feed_message = decode_feed(response.content)
            batch = build_batch(source, feed_message, dedup, fetched_at)
            if batch is not None:
                publisher.publish(batch.messages)
                dedup.commit(batch)
        except (httpx.HTTPError, InvalidFeedError, PublishError):
            logger.exception("Échec de collecte, flux ignoré ce cycle", extra={"feed": source.feed})
            continue
        published[source.feed] = len(batch.messages) if batch else 0
        logger.info(
            "Flux collecté",
            extra={
                "feed": source.feed.value,
                "entities": len(feed_message.entity),
                "published": published[source.feed],
                "feed_timestamp": feed_message.header.timestamp,
            },
        )
    return published


def run(settings: Settings, *, once: bool = False, stop: threading.Event | None = None) -> None:
    """Boucle de collecte jusqu'à `stop` (ou un seul cycle si `once`)."""
    stop = stop or threading.Event()
    check_broker(settings.kafka_bootstrap_servers)
    ensure_topics(
        settings.kafka_bootstrap_servers,
        [settings.topic_trip_updates, settings.topic_vehicle_positions, settings.topic_alerts],
    )
    publisher = KafkaPublisher(settings.kafka_bootstrap_servers)
    dedup = Deduplicator()
    sources = feed_sources(settings)
    with build_client(settings) as client:
        while not stop.is_set():
            poll_once(sources, client, publisher, dedup)
            if once:
                break
            stop.wait(settings.poll_interval_s)
    logger.info("Producteur arrêté")


def main(argv: Sequence[str] | None = None) -> None:
    """Point d'entrée CLI."""
    parser = argparse.ArgumentParser(description="Producteur GTFS-RT STAR vers Redpanda")
    parser.add_argument("--once", action="store_true", help="un seul cycle de collecte")
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.log_level, json_format=settings.log_json)
    stop = threading.Event()

    def _handle_signal(signum: int, _frame: FrameType | None) -> None:
        logger.info("Signal reçu, arrêt après le cycle en cours", extra={"signal": signum})
        stop.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    try:
        run(settings, once=args.once, stop=stop)
    except BrokerUnavailableError as exc:
        logger.error(str(exc))
        sys.exit(1)


if __name__ == "__main__":
    main()
