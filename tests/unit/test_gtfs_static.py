"""Tests de l'ingestion du GTFS théorique vers bronze."""

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import httpx
import pytest

from star_pulse.common.storage import InMemoryObjectStore
from star_pulse.ingestion.gtfs_static import (
    STATE_KEY,
    IngestionStatus,
    InvalidGtfsError,
    content_sha256,
    csv_to_parquet,
    ingest_gtfs_static,
    parse_feed_info,
    read_archive,
)
from tests.conftest import ZipBuilder

URL = "https://example.test/GTFS_STAR_BUS_METRO_EN_COURS.zip"
NOW = datetime(2026, 11, 21, 4, 0, tzinfo=UTC)


class FakeGtfsServer:
    """Serveur HTTP simulé : sert un zip avec ETag et gère `If-None-Match`."""

    def __init__(self, body: bytes, etag: str = '"v1"') -> None:
        self.body = body
        self.etag = etag
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.headers.get("If-None-Match") == self.etag:
            return httpx.Response(304)
        return httpx.Response(200, content=self.body, headers={"ETag": self.etag})

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self))


# --- Lecture et validation


def test_read_archive_returns_tables(gtfs_zip: bytes) -> None:
    files = read_archive(gtfs_zip)
    assert {"agency", "stops", "routes", "trips", "stop_times", "calendar"} <= files.keys()


def test_read_archive_rejects_non_zip() -> None:
    with pytest.raises(InvalidGtfsError, match="zip valide"):
        read_archive(b"<html>maintenance</html>")


def test_read_archive_rejects_missing_required_file(zip_builder: ZipBuilder) -> None:
    with pytest.raises(InvalidGtfsError, match="stop_times"):
        read_archive(zip_builder(exclude=frozenset({"stop_times"})))


def test_read_archive_requires_a_calendar(zip_builder: ZipBuilder) -> None:
    with pytest.raises(InvalidGtfsError, match="calendar"):
        read_archive(zip_builder(exclude=frozenset({"calendar", "calendar_dates"})))


def test_parse_feed_info(gtfs_zip: bytes) -> None:
    feed = parse_feed_info(read_archive(gtfs_zip))
    assert feed is not None
    assert feed.publisher == "Keolis Rennes"
    assert feed.start_date <= feed.end_date


# --- Checksum


def test_checksum_ignores_rezipping(zip_builder: ZipBuilder) -> None:
    original = zip_builder()
    rezipped = zip_builder(date_time=(2026, 11, 21, 23, 50, 0), reverse_order=True)

    assert original != rezipped
    assert content_sha256(read_archive(original)) == content_sha256(read_archive(rezipped))


def test_checksum_changes_with_content(gtfs_zip: bytes) -> None:
    files = read_archive(gtfs_zip)
    modified = {**files, "stops": files["stops"] + b"\n"}
    assert content_sha256(files) != content_sha256(modified)


# --- Conversion Parquet


def test_csv_to_parquet_keeps_values_as_text(tmp_path: Path) -> None:
    csv_data = (
        b"trip_id,arrival_time,stop_sequence\n00000044847,25:10:00,1\n00000044847,25:12:30,2\n"
    )

    parquet, rows = csv_to_parquet(csv_data)

    path = tmp_path / "t.parquet"
    path.write_bytes(parquet)
    with duckdb.connect() as con:
        con.execute("CREATE TABLE t AS SELECT * FROM read_parquet(?)", [str(path)])
        types = {name: dtype for name, dtype, *_ in con.execute("DESCRIBE t").fetchall()}
        first = con.execute("SELECT trip_id, arrival_time FROM t LIMIT 1").fetchone()
    assert rows == 2
    assert set(types.values()) == {"VARCHAR"}
    assert first == ("00000044847", "25:10:00")  # zéros initiaux et heures > 24h préservés


# --- Ingestion de bout en bout (stockage en mémoire)


def test_first_run_writes_raw_tables_and_manifest(gtfs_zip: bytes) -> None:
    store, server = InMemoryObjectStore(), FakeGtfsServer(gtfs_zip)

    with server.client() as client:
        result = ingest_gtfs_static(store, client, URL, now=NOW)

    assert result.status is IngestionStatus.INGESTED
    assert result.manifest_key is not None
    manifest = json.loads(store.get_bytes(result.manifest_key))
    sha12 = manifest["content_sha256"][:12]

    partition = f"ingestion_date=2026-11-21/version={sha12}"
    assert store.get_bytes(f"bronze/gtfs_static/_raw/{partition}/gtfs.zip") == gtfs_zip
    stop_times_key = f"bronze/gtfs_static/stop_times/{partition}/stop_times.parquet"
    assert stop_times_key in manifest["objects"]
    assert all(store.exists(k) for k in manifest["objects"])
    assert manifest["row_counts"]["trips"] == 8
    assert manifest["etag"] == '"v1"'
    assert manifest["feed"]["publisher"] == "Keolis Rennes"


def test_second_run_with_same_etag_downloads_nothing(gtfs_zip: bytes) -> None:
    store, server = InMemoryObjectStore(), FakeGtfsServer(gtfs_zip)

    with server.client() as client:
        ingest_gtfs_static(store, client, URL, now=NOW)
        keys_before = set(store.objects)
        result = ingest_gtfs_static(store, client, URL, now=NOW)

    assert result.status is IngestionStatus.NOT_MODIFIED
    assert server.requests[-1].headers["If-None-Match"] == '"v1"'
    assert set(store.objects) == keys_before


def test_rezipped_archive_is_detected_as_duplicate(zip_builder: ZipBuilder) -> None:
    store = InMemoryObjectStore()
    with FakeGtfsServer(zip_builder(), etag='"v1"').client() as client:
        first = ingest_gtfs_static(store, client, URL, now=NOW)

    # Le lendemain : nouvel ETag, nouveau zip, mais horaires identiques.
    rezipped = zip_builder(date_time=(2026, 11, 21, 23, 50, 0), reverse_order=True)
    next_day = datetime(2026, 11, 22, 4, 0, tzinfo=UTC)
    with FakeGtfsServer(rezipped, etag='"v2"').client() as client:
        keys_before = set(store.objects) - {STATE_KEY}
        second = ingest_gtfs_static(store, client, URL, now=next_day)

    assert second.status is IngestionStatus.DUPLICATE
    assert second.content_sha256 == first.content_sha256
    assert set(store.objects) - {STATE_KEY} == keys_before
    assert json.loads(store.get_bytes(STATE_KEY))["etag"] == '"v2"'


def test_interrupted_run_is_replayed(gtfs_zip: bytes) -> None:
    """Sans manifeste (crash en cours d'écriture), la version est réécrite entièrement."""
    store, server = InMemoryObjectStore(), FakeGtfsServer(gtfs_zip)
    with server.client() as client:
        result = ingest_gtfs_static(store, client, URL, now=NOW)
    assert result.manifest_key is not None
    del store.objects[result.manifest_key]
    del store.objects[STATE_KEY]

    with server.client() as client:
        replay = ingest_gtfs_static(store, client, URL, now=NOW)

    assert replay.status is IngestionStatus.INGESTED
    assert store.exists(result.manifest_key)


def test_invalid_download_writes_nothing() -> None:
    store = InMemoryObjectStore()
    with FakeGtfsServer(b"not a zip").client() as client, pytest.raises(InvalidGtfsError):
        ingest_gtfs_static(store, client, URL, now=NOW)
    assert store.objects == {}


def test_manifest_is_valid_json_utf8(gtfs_zip: bytes) -> None:
    store = InMemoryObjectStore()
    with FakeGtfsServer(gtfs_zip).client() as client:
        result = ingest_gtfs_static(store, client, URL, now=NOW)
    assert result.manifest_key is not None
    text = store.get_bytes(result.manifest_key).decode("utf-8")
    assert json.loads(text)["ingested_at"] == NOW.isoformat()
