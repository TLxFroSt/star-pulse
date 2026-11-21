"""Consumer Redpanda → couche bronze (Parquet partitionné sur MinIO).

Garanties :

- **At-least-once** : les offsets ne sont commités qu'après l'écriture réussie de tous
  les fichiers du lot. Un crash ou une erreur S3 entraîne une relecture, jamais une perte.
- **Fichiers déterministes** : le nom d'un fichier dépend des offsets qu'il contient ;
  relire exactement les mêmes offsets réécrit le même objet.
- **Partitionnement sur le temps de l'événement** (`feed_timestamp`, UTC) :
  `bronze/gtfs_rt/<feed>/date=YYYY-MM-DD/hour=HH/part-p<partition>-o<début>-<fin>.parquet`.
- Un message illisible n'est pas perdu : il part dans `bronze/gtfs_rt/_rejected/`.

Les doublons restants (relecture partielle, redémarrage du producteur) sont éliminés en
silver sur `(entity_key, entity_version)`. Voir `docs/adr/0004-bronze-gtfs-rt.md`.

Usage : `python -m star_pulse.streaming.bronze_writer [--once]` (ou `make consume-rt`).
"""

import argparse
import json
import logging
import signal
import sys
import tempfile
import threading
import time
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType
from typing import Protocol

import duckdb
from confluent_kafka import Consumer, TopicPartition

from star_pulse.common.kafka import BrokerUnavailableError, check_broker
from star_pulse.common.logging import configure_logging
from star_pulse.common.storage import ObjectStore, S3ObjectStore
from star_pulse.config import get_settings

logger = logging.getLogger(__name__)

BRONZE_PREFIX = "bronze/gtfs_rt"
REJECTED_FEED = "_rejected"

# Schéma Parquet de la couche bronze temps réel (ordre des colonnes conservé).
PARQUET_COLUMNS: dict[str, str] = {
    "feed": "VARCHAR",
    "feed_timestamp": "BIGINT",
    "fetched_at": "TIMESTAMPTZ",
    "entity_key": "VARCHAR",
    "entity_version": "VARCHAR",
    "entity_json": "VARCHAR",
    "kafka_topic": "VARCHAR",
    "kafka_partition": "INTEGER",
    "kafka_offset": "BIGINT",
    "kafka_timestamp_ms": "BIGINT",
    "ingested_at": "TIMESTAMPTZ",
}


@dataclass(frozen=True)
class ConsumedMessage:
    """Message lu sur Redpanda, découplé de la bibliothèque cliente."""

    topic: str
    partition: int
    offset: int
    timestamp_ms: int | None
    value: bytes


@dataclass(frozen=True)
class BronzeRecord:
    """Une ligne de la table bronze."""

    feed: str
    feed_timestamp: int
    fetched_at: str | None
    entity_key: str | None
    entity_version: str | None
    entity_json: str
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    kafka_timestamp_ms: int | None
    ingested_at: str


def to_record(message: ConsumedMessage, ingested_at: datetime) -> BronzeRecord:
    """Convertit un message en ligne bronze ; un message illisible devient un rejet.

    Args:
        message: Message consommé.
        ingested_at: Horodatage d'écriture.

    Returns:
        La ligne bronze (feed `_rejected` si le message est invalide).
    """

    def record(
        feed: str,
        feed_timestamp: int,
        fetched_at: str | None,
        key: str | None,
        version: str | None,
        entity_json: str,
    ) -> BronzeRecord:
        return BronzeRecord(
            feed=feed,
            feed_timestamp=feed_timestamp,
            fetched_at=fetched_at,
            entity_key=key,
            entity_version=version,
            entity_json=entity_json,
            kafka_topic=message.topic,
            kafka_partition=message.partition,
            kafka_offset=message.offset,
            kafka_timestamp_ms=message.timestamp_ms,
            ingested_at=ingested_at.isoformat(),
        )

    try:
        envelope = json.loads(message.value)
        return record(
            str(envelope["feed"]),
            int(envelope["feed_timestamp"]),
            str(envelope["fetched_at"]),
            str(envelope["entity_key"]),
            str(envelope["entity_version"]),
            json.dumps(envelope["entity"], ensure_ascii=False),
        )
    except (ValueError, KeyError, TypeError):
        fallback_ts = (message.timestamp_ms or int(ingested_at.timestamp() * 1000)) // 1000
        raw = message.value.decode("utf-8", errors="replace")
        return record(REJECTED_FEED, fallback_ts, None, None, None, raw)


@dataclass(frozen=True, order=True)
class FileGroup:
    """Ensemble de lignes destinées à un même fichier Parquet."""

    feed: str
    date: str
    hour: str
    topic: str
    partition: int


def group_records(records: Sequence[BronzeRecord]) -> dict[FileGroup, list[BronzeRecord]]:
    """Répartit les lignes par flux, date et heure (UTC) de l'événement, et partition Kafka."""
    groups: dict[FileGroup, list[BronzeRecord]] = defaultdict(list)
    for record in records:
        event_time = datetime.fromtimestamp(record.feed_timestamp, tz=UTC)
        group = FileGroup(
            feed=record.feed,
            date=event_time.strftime("%Y-%m-%d"),
            hour=event_time.strftime("%H"),
            topic=record.kafka_topic,
            partition=record.kafka_partition,
        )
        groups[group].append(record)
    return dict(groups)


def object_key(group: FileGroup, records: Sequence[BronzeRecord]) -> str:
    """Clé S3 déterministe : dépend uniquement des offsets contenus."""
    first = min(r.kafka_offset for r in records)
    last = max(r.kafka_offset for r in records)
    return (
        f"{BRONZE_PREFIX}/{group.feed}/date={group.date}/hour={group.hour}/"
        f"part-p{group.partition}-o{first:012d}-{last:012d}.parquet"
    )


def records_to_parquet(records: Sequence[BronzeRecord]) -> bytes:
    """Sérialise des lignes bronze en Parquet (zstd) avec le schéma `PARQUET_COLUMNS`."""
    with tempfile.TemporaryDirectory() as tmp:
        src, dst = Path(tmp, "batch.ndjson"), Path(tmp, "batch.parquet")
        with src.open("w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(asdict(record), ensure_ascii=False) + "\n")
        columns = ", ".join(f"'{name}': '{dtype}'" for name, dtype in PARQUET_COLUMNS.items())
        order = ", ".join(PARQUET_COLUMNS)
        # Chemins issus d'un répertoire temporaire maîtrisé : interpolation sans risque.
        with duckdb.connect() as con:
            con.execute(
                f"COPY (SELECT {order} FROM read_json('{src}', format = 'newline_delimited', "
                f"columns = {{{columns}}}) ORDER BY kafka_offset) "
                f"TO '{dst}' (FORMAT parquet, COMPRESSION zstd)"
            )
        return dst.read_bytes()


class MessageSource(Protocol):
    """Source de messages (Redpanda en production, liste en mémoire en test)."""

    def poll(self, timeout_s: float) -> ConsumedMessage | None:
        """Renvoie le prochain message, ou `None` après `timeout_s` sans message."""
        ...

    def commit(self, next_offsets: dict[tuple[str, int], int]) -> None:
        """Commite, par (topic, partition), le prochain offset à lire."""
        ...

    def close(self) -> None:
        """Libère la connexion."""
        ...


class KafkaMessageSource:
    """Adaptateur `confluent_kafka.Consumer` → `MessageSource`, commits manuels."""

    def __init__(self, bootstrap_servers: str, group_id: str, topics: Sequence[str]) -> None:
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "client.id": "star-pulse-bronze-writer",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self._consumer.subscribe(list(topics))

    def poll(self, timeout_s: float) -> ConsumedMessage | None:
        msg = self._consumer.poll(timeout_s)
        if msg is None:
            return None
        error = msg.error()
        if error is not None:
            logger.warning("Erreur de lecture Redpanda", extra={"error": str(error)})
            return None
        topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
        if topic is None or partition is None or offset is None:
            return None
        ts_type, ts = msg.timestamp()
        return ConsumedMessage(
            topic=topic,
            partition=partition,
            offset=offset,
            timestamp_ms=ts if ts_type else None,
            value=msg.value() or b"",
        )

    def commit(self, next_offsets: dict[tuple[str, int], int]) -> None:
        self._consumer.commit(
            offsets=[TopicPartition(t, p, o) for (t, p), o in next_offsets.items()],
            asynchronous=False,
        )

    def close(self) -> None:
        self._consumer.close()


@dataclass
class FlushResult:
    """Bilan d'une écriture de lot."""

    records: int
    files: list[str]


class BronzeWriter:
    """Accumule les messages et les écrit par lots en Parquet, puis commite."""

    def __init__(
        self,
        source: MessageSource,
        store: ObjectStore,
        *,
        max_records: int = 5000,
        max_age_s: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
    ) -> None:
        self._source = source
        self._store = store
        self._max_records = max_records
        self._max_age_s = max_age_s
        self._clock = clock
        self._now = now
        self._buffer: list[BronzeRecord] = []
        self._buffer_started: float | None = None

    def add(self, message: ConsumedMessage) -> None:
        """Ajoute un message au lot courant."""
        if not self._buffer:
            self._buffer_started = self._clock()
        self._buffer.append(to_record(message, self._now()))

    def should_flush(self) -> bool:
        """Vrai si le lot a atteint sa taille ou son âge maximal."""
        if not self._buffer or self._buffer_started is None:
            return False
        too_old = self._clock() - self._buffer_started >= self._max_age_s
        return len(self._buffer) >= self._max_records or too_old

    def flush(self) -> FlushResult:
        """Écrit le lot sur le stockage objet, puis commite les offsets.

        Si une écriture échoue, l'exception remonte sans commit : le lot sera relu.
        """
        if not self._buffer:
            return FlushResult(0, [])
        groups = group_records(self._buffer)
        files = []
        for group in sorted(groups):
            rows = groups[group]
            key = object_key(group, rows)
            self._store.put_bytes(
                key, records_to_parquet(rows), content_type="application/vnd.apache.parquet"
            )
            files.append(key)

        next_offsets: dict[tuple[str, int], int] = {}
        for record in self._buffer:
            tp = (record.kafka_topic, record.kafka_partition)
            next_offsets[tp] = max(next_offsets.get(tp, 0), record.kafka_offset + 1)
        self._source.commit(next_offsets)

        result = FlushResult(len(self._buffer), files)
        self._buffer, self._buffer_started = [], None
        logger.info("Lot écrit en bronze", extra={"records": result.records, "files": len(files)})
        return result

    def run(
        self, stop: threading.Event, *, once: bool = False, idle_polls_before_exit: int = 5
    ) -> None:
        """Consomme jusqu'à `stop`. En mode `once`, s'arrête quand il n'y a plus rien à lire."""
        idle = 0
        try:
            while not stop.is_set():
                message = self._source.poll(1.0)
                if message is None:
                    idle += 1
                    if once and idle >= idle_polls_before_exit:
                        break
                else:
                    idle = 0
                    self.add(message)
                if self.should_flush():
                    self.flush()
            self.flush()
        finally:
            self._source.close()


def main(argv: Sequence[str] | None = None) -> None:
    """Point d'entrée CLI."""
    parser = argparse.ArgumentParser(description="Consumer Redpanda vers bronze (MinIO)")
    parser.add_argument("--once", action="store_true", help="vide les topics puis s'arrête")
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.log_level, json_format=settings.log_json)
    try:
        check_broker(settings.kafka_bootstrap_servers)
    except BrokerUnavailableError as exc:
        logger.error(str(exc))
        sys.exit(1)

    stop = threading.Event()

    def _handle_signal(signum: int, _frame: FrameType | None) -> None:
        logger.info("Signal reçu, écriture du lot en cours puis arrêt", extra={"signal": signum})
        stop.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    source = KafkaMessageSource(
        settings.kafka_bootstrap_servers,
        settings.kafka_consumer_group,
        [settings.topic_trip_updates, settings.topic_vehicle_positions, settings.topic_alerts],
    )
    writer = BronzeWriter(
        source,
        S3ObjectStore.from_settings(settings),
        max_records=settings.bronze_batch_max_records,
        max_age_s=settings.bronze_batch_max_age_s,
    )
    writer.run(stop, once=args.once)
    logger.info("Consumer arrêté")


if __name__ == "__main__":
    main()
