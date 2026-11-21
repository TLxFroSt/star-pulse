"""Ingestion GTFS contre le vrai MinIO local (`make up` requis)."""

import json
import uuid

import httpx
import pytest

from star_pulse.common.storage import S3ObjectStore
from star_pulse.config import Settings
from star_pulse.ingestion.gtfs_static import IngestionStatus, ingest_gtfs_static

pytestmark = pytest.mark.integration


TEST_BUCKET = "lake-tests"


@pytest.fixture
def store() -> S3ObjectStore:
    """Store sur un bucket dédié aux tests, pour ne jamais polluer `lake`."""
    settings = Settings().model_copy(update={"s3_bucket": TEST_BUCKET})
    try:
        httpx.get(f"{settings.s3_endpoint_url}minio/health/live", timeout=2).raise_for_status()
    except httpx.HTTPError:
        pytest.skip("MinIO indisponible : lancer `make up`")
    store = S3ObjectStore.from_settings(settings)
    store.ensure_bucket()
    return store


def test_ingestion_roundtrip_on_minio(store: S3ObjectStore, gtfs_zip: bytes) -> None:
    etag = f'"{uuid.uuid4()}"'  # unique : indépendant des exécutions précédentes
    transport = httpx.MockTransport(
        lambda _: httpx.Response(200, content=gtfs_zip, headers={"ETag": etag})
    )
    with httpx.Client(transport=transport) as client:
        result = ingest_gtfs_static(store, client, "https://example.test/gtfs.zip")

    assert result.status in {IngestionStatus.INGESTED, IngestionStatus.DUPLICATE}
    assert result.manifest_key is not None
    manifest = json.loads(store.get_bytes(result.manifest_key))
    assert all(store.exists(key) for key in manifest["objects"])
