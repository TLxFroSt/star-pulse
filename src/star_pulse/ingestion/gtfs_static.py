"""Ingestion du GTFS théorique STAR vers la couche bronze.

Déroulé d'une exécution :

1. Téléchargement conditionnel (`If-None-Match`) : si l'ETag n'a pas changé, arrêt.
2. Validation du zip (fichiers GTFS obligatoires).
3. Checksum du *contenu* : le zip est regénéré chaque nuit même sans changement d'horaires,
   donc on hache les fichiers et non l'archive. Une version déjà connue n'est pas réécrite.
4. Écriture bronze : zip brut + une table Parquet par fichier (colonnes en texte ; le
   typage est fait dans les modèles dbt `stg_*`).
5. Écriture du manifeste en dernier : il sert de marqueur de « commit ». Une exécution
   interrompue avant lui sera simplement rejouée, sur les mêmes clés.

Usage : `python -m star_pulse.ingestion.gtfs_static` (ou `make ingest-gtfs`).
"""

import csv
import hashlib
import io
import json
import logging
import tempfile
import zipfile
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path, PurePosixPath

import duckdb
import httpx

from star_pulse.common.http import build_client, get_with_retry
from star_pulse.common.logging import configure_logging
from star_pulse.common.storage import ObjectStore, S3ObjectStore
from star_pulse.config import Settings, get_settings

logger = logging.getLogger(__name__)

BRONZE_PREFIX = "bronze/gtfs_static"
VERSIONS_PREFIX = f"{BRONZE_PREFIX}/_versions"
STATE_KEY = f"{BRONZE_PREFIX}/_state/latest.json"

# Fichiers obligatoires selon la spécification GTFS (calendar et/ou calendar_dates en plus).
REQUIRED_FILES = frozenset({"agency", "stops", "routes", "trips", "stop_times"})
CALENDAR_FILES = frozenset({"calendar", "calendar_dates"})


class InvalidGtfsError(ValueError):
    """Archive GTFS illisible ou incomplète."""


class IngestionStatus(StrEnum):
    """Issue d'une exécution d'ingestion."""

    INGESTED = "ingested"
    NOT_MODIFIED = "not_modified"  # ETag inchangé, rien téléchargé
    DUPLICATE = "duplicate"  # contenu déjà présent en bronze


@dataclass(frozen=True)
class FeedInfo:
    """Extrait de `feed_info.txt`."""

    publisher: str
    version: str
    start_date: str
    end_date: str


@dataclass(frozen=True)
class Manifest:
    """Métadonnées d'une version GTFS écrite en bronze."""

    content_sha256: str
    ingested_at: str
    ingestion_date: str
    source_url: str
    etag: str | None
    feed: FeedInfo | None
    row_counts: dict[str, int]
    objects: list[str]


@dataclass(frozen=True)
class IngestionResult:
    """Résultat renvoyé par `ingest_gtfs_static`."""

    status: IngestionStatus
    content_sha256: str | None = None
    manifest_key: str | None = None


def read_archive(data: bytes) -> dict[str, bytes]:
    """Lit une archive GTFS et renvoie ses fichiers `.txt` par nom de table.

    Args:
        data: Contenu brut du zip.

    Returns:
        Dictionnaire `{"stops": b"...", ...}`.

    Raises:
        InvalidGtfsError: Si l'archive n'est pas un zip valide ou si un fichier requis manque.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            files = {
                PurePosixPath(info.filename).stem: archive.read(info)
                for info in archive.infolist()
                if not info.is_dir() and info.filename.endswith(".txt")
            }
    except zipfile.BadZipFile as exc:
        raise InvalidGtfsError("L'archive GTFS n'est pas un zip valide") from exc

    missing = REQUIRED_FILES - files.keys()
    if missing:
        raise InvalidGtfsError(f"Fichiers GTFS manquants : {sorted(missing)}")
    if not CALENDAR_FILES & files.keys():
        raise InvalidGtfsError("Ni calendar.txt ni calendar_dates.txt dans l'archive")
    return files


def content_sha256(files: dict[str, bytes]) -> str:
    """Empreinte du contenu GTFS, indépendante de l'archive (ordre, dates, compression).

    Args:
        files: Fichiers par nom de table.

    Returns:
        Le sha256 hexadécimal.
    """
    digest = hashlib.sha256()
    for name in sorted(files):
        digest.update(f"{name}\0{hashlib.sha256(files[name]).hexdigest()}\n".encode())
    return digest.hexdigest()


def parse_feed_info(files: dict[str, bytes]) -> FeedInfo | None:
    """Lit la première ligne de `feed_info.txt` si le fichier est présent."""
    raw = files.get("feed_info")
    if raw is None:
        return None
    rows = list(csv.DictReader(io.StringIO(raw.decode("utf-8-sig"))))
    if not rows:
        return None
    row = rows[0]
    return FeedInfo(
        publisher=row.get("feed_publisher_name", ""),
        version=row.get("feed_version", ""),
        start_date=row.get("feed_start_date", ""),
        end_date=row.get("feed_end_date", ""),
    )


def csv_to_parquet(data: bytes) -> tuple[bytes, int]:
    """Convertit un fichier GTFS (CSV) en Parquet, toutes colonnes en texte.

    Garder du texte en bronze évite toute perte à l'ingestion (heures > 24:00:00,
    zéros initiaux des identifiants...) ; le typage se fait en silver.

    Args:
        data: Contenu CSV.

    Returns:
        Le Parquet (zstd) et son nombre de lignes.
    """
    with tempfile.TemporaryDirectory() as tmp:
        src, dst = Path(tmp, "in.csv"), Path(tmp, "out.parquet")
        src.write_bytes(data)
        # Chemins issus d'un répertoire temporaire maîtrisé : interpolation sans risque.
        with duckdb.connect() as con:
            con.execute(
                f"COPY (SELECT * FROM read_csv('{src}', header = true, all_varchar = true)) "
                f"TO '{dst}' (FORMAT parquet, COMPRESSION zstd)"
            )
            row = con.execute("SELECT count(*) FROM read_parquet(?)", [str(dst)]).fetchone()
        return dst.read_bytes(), int(row[0]) if row else 0


def _version_dir(table: str, ingestion_date: str, sha: str) -> str:
    return f"{BRONZE_PREFIX}/{table}/ingestion_date={ingestion_date}/version={sha[:12]}"


def _read_state(store: ObjectStore) -> dict[str, str]:
    if not store.exists(STATE_KEY):
        return {}
    state: dict[str, str] = json.loads(store.get_bytes(STATE_KEY))
    return state


def ingest_gtfs_static(
    store: ObjectStore,
    client: httpx.Client,
    url: str,
    *,
    now: datetime | None = None,
) -> IngestionResult:
    """Télécharge le GTFS et l'écrit en bronze s'il s'agit d'une nouvelle version.

    Args:
        store: Stockage objet cible.
        client: Client HTTP.
        url: URL du zip GTFS.
        now: Horodatage d'ingestion (injectable pour les tests).

    Returns:
        Le statut de l'exécution et, le cas échéant, la version concernée.
    """
    now = now or datetime.now(tz=UTC)
    state = _read_state(store)

    headers = {"If-None-Match": state["etag"]} if state.get("etag") else None
    response = get_with_retry(client, url, headers=headers)
    if response.status_code == httpx.codes.NOT_MODIFIED:
        logger.info("GTFS inchangé (ETag)", extra={"etag": state.get("etag")})
        return IngestionResult(IngestionStatus.NOT_MODIFIED, state.get("content_sha256"))

    etag = response.headers.get("ETag")
    files = read_archive(response.content)
    sha = content_sha256(files)
    manifest_key = f"{VERSIONS_PREFIX}/{sha}.json"

    if store.exists(manifest_key):
        logger.info("Version GTFS déjà en bronze", extra={"content_sha256": sha})
        result = IngestionResult(IngestionStatus.DUPLICATE, sha, manifest_key)
    else:
        ingestion_date = now.date().isoformat()
        objects: list[str] = []
        row_counts: dict[str, int] = {}

        raw_key = f"{_version_dir('_raw', ingestion_date, sha)}/gtfs.zip"
        store.put_bytes(raw_key, response.content, content_type="application/zip")
        objects.append(raw_key)

        for table in sorted(files):
            parquet, rows = csv_to_parquet(files[table])
            key = f"{_version_dir(table, ingestion_date, sha)}/{table}.parquet"
            store.put_bytes(key, parquet, content_type="application/vnd.apache.parquet")
            objects.append(key)
            row_counts[table] = rows

        manifest = Manifest(
            content_sha256=sha,
            ingested_at=now.isoformat(),
            ingestion_date=ingestion_date,
            source_url=url,
            etag=etag,
            feed=parse_feed_info(files),
            row_counts=row_counts,
            objects=objects,
        )
        # Écrit en dernier : sa présence garantit que la version est complète.
        store.put_bytes(
            manifest_key,
            json.dumps(asdict(manifest), indent=2, ensure_ascii=False).encode(),
            content_type="application/json",
        )
        logger.info(
            "Nouvelle version GTFS écrite en bronze",
            extra={
                "content_sha256": sha,
                "feed_version": manifest.feed.version if manifest.feed else None,
                "rows_stop_times": row_counts.get("stop_times"),
            },
        )
        result = IngestionResult(IngestionStatus.INGESTED, sha, manifest_key)

    store.put_bytes(
        STATE_KEY,
        json.dumps({"etag": etag or "", "content_sha256": sha}).encode(),
        content_type="application/json",
    )
    return result


def main(settings: Settings | None = None) -> IngestionResult:
    """Point d'entrée CLI : ingestion avec la configuration de l'environnement."""
    settings = settings or get_settings()
    configure_logging(settings.log_level, json_format=settings.log_json)
    store = S3ObjectStore.from_settings(settings)
    with build_client(settings) as client:
        return ingest_gtfs_static(store, client, str(settings.gtfs_static_url))


if __name__ == "__main__":
    main()
