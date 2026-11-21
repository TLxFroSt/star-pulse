"""Stockage objet contre le vrai MinIO local (`make up` requis)."""

import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from star_pulse.common.storage import S3ObjectStore
from star_pulse.config import Settings

pytestmark = pytest.mark.integration


def test_latest_modified_on_minio() -> None:
    settings = Settings().model_copy(update={"s3_bucket": "lake-tests"})
    try:
        httpx.get(f"{settings.s3_endpoint_url}minio/health/live", timeout=2).raise_for_status()
    except httpx.HTTPError:
        pytest.skip("MinIO indisponible : lancer `make up`")
    store = S3ObjectStore.from_settings(settings)
    store.ensure_bucket()
    prefix = f"tests/{uuid.uuid4().hex}/"

    assert store.latest_modified(prefix) is None

    store.put_bytes(f"{prefix}a.txt", b"a")
    latest = store.latest_modified(prefix)

    assert latest is not None
    assert abs(datetime.now(UTC) - latest) < timedelta(minutes=5)
