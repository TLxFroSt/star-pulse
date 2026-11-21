"""Accès au stockage objet du lakehouse (MinIO / S3), derrière une interface testable."""

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from star_pulse.config import Settings

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


class ObjectStore(Protocol):
    """Opérations minimales dont le pipeline a besoin sur un stockage objet."""

    def exists(self, key: str) -> bool:
        """Indique si l'objet `key` existe."""
        ...

    def get_bytes(self, key: str) -> bytes:
        """Lit le contenu de l'objet `key`."""
        ...

    def put_bytes(
        self, key: str, data: bytes, *, content_type: str = "application/octet-stream"
    ) -> None:
        """Écrit (ou remplace) l'objet `key`."""
        ...

    def list_keys(self, prefix: str) -> list[str]:
        """Liste les clés commençant par `prefix`, triées."""
        ...

    def latest_modified(self, prefix: str) -> datetime | None:
        """Date de dernière écriture (UTC) parmi les objets sous `prefix`, ou None."""
        ...


class InMemoryObjectStore:
    """Implémentation en mémoire, pour les tests unitaires."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.modified: dict[str, datetime] = {}

    def exists(self, key: str) -> bool:
        return key in self.objects

    def get_bytes(self, key: str) -> bytes:
        return self.objects[key]

    def put_bytes(
        self, key: str, data: bytes, *, content_type: str = "application/octet-stream"
    ) -> None:
        self.objects[key] = data
        self.modified[key] = datetime.now(UTC)

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(k for k in self.objects if k.startswith(prefix))

    def latest_modified(self, prefix: str) -> datetime | None:
        dates = [d for k, d in self.modified.items() if k.startswith(prefix)]
        return max(dates, default=None)


class S3ObjectStore:
    """Implémentation S3 (MinIO en local), via boto3."""

    def __init__(self, client: "S3Client", bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    @classmethod
    def from_settings(cls, settings: Settings) -> "S3ObjectStore":
        """Crée le store à partir de la configuration (endpoint, identifiants, bucket)."""
        client = boto3.client(
            "s3",
            endpoint_url=str(settings.s3_endpoint_url),
            aws_access_key_id=settings.s3_access_key.get_secret_value(),
            aws_secret_access_key=settings.s3_secret_key.get_secret_value(),
            region_name=settings.s3_region,
            # MinIO attend des URL de style « path » (endpoint/bucket/clé).
            config=Config(s3={"addressing_style": "path"}, retries={"mode": "standard"}),
        )
        return cls(client, settings.s3_bucket)

    def ensure_bucket(self) -> None:
        """Crée le bucket s'il n'existe pas (utile pour les tests d'intégration)."""
        try:
            self._client.head_bucket(Bucket=self._bucket)
        except ClientError:
            self._client.create_bucket(Bucket=self._bucket)

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True

    def get_bytes(self, key: str) -> bytes:
        return self._client.get_object(Bucket=self._bucket, Key=key)["Body"].read()

    def put_bytes(
        self, key: str, data: bytes, *, content_type: str = "application/octet-stream"
    ) -> None:
        self._client.put_object(Bucket=self._bucket, Key=key, Body=data, ContentType=content_type)

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []) if "Key" in obj)
        return sorted(keys)

    def latest_modified(self, prefix: str) -> datetime | None:
        latest: datetime | None = None
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                modified = obj.get("LastModified")
                if modified is not None and (latest is None or modified > latest):
                    latest = modified
        return latest
