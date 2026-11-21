"""Configuration de l'application, lue depuis l'environnement et le fichier `.env`.

Toutes les variables sont préfixées par `STAR_PULSE_`. Les valeurs par défaut
correspondent à l'infra locale lancée par `make up`.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field, HttpUrl, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class Settings(BaseSettings):
    """Paramètres du pipeline STAR Pulse."""

    model_config = SettingsConfigDict(
        env_prefix="STAR_PULSE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    # --- Général
    environment: Literal["local", "ci", "prod"] = "local"
    log_level: LogLevel = "INFO"
    log_json: bool = False

    # --- Redpanda (API Kafka)
    kafka_bootstrap_servers: str = "localhost:19092"
    topic_trip_updates: str = "gtfs-rt.trip-updates"
    topic_vehicle_positions: str = "gtfs-rt.vehicle-positions"
    topic_alerts: str = "gtfs-rt.alerts"
    kafka_consumer_group: str = "star-pulse-bronze"
    # Un lot est écrit dès qu'il atteint l'un des deux seuils.
    bronze_batch_max_records: int = Field(default=5000, gt=0)
    bronze_batch_max_age_s: float = Field(default=60.0, gt=0)

    # --- MinIO / S3
    s3_endpoint_url: HttpUrl = HttpUrl("http://localhost:9000")
    s3_access_key: SecretStr = SecretStr("minioadmin")
    s3_secret_key: SecretStr = SecretStr("minioadmin")
    s3_region: str = "us-east-1"
    s3_bucket: str = "lake"

    # --- Sources open data
    gtfs_static_url: HttpUrl = HttpUrl(
        "https://eu.ftp.opendatasoft.com/star/gtfs/GTFS_STAR_BUS_METRO_EN_COURS.zip"
    )
    gtfs_rt_trip_updates_url: HttpUrl = HttpUrl(
        "https://proxy.transport.data.gouv.fr/resource/star-rennes-integration-gtfs-rt-trip-update"
    )
    gtfs_rt_vehicle_positions_url: HttpUrl = HttpUrl(
        "https://proxy.transport.data.gouv.fr/resource/star-rennes-integration-gtfs-rt-vehicle-position"
    )
    gtfs_rt_alerts_url: HttpUrl = HttpUrl(
        "https://proxy.transport.data.gouv.fr/resource/star-rennes-integration-gtfs-rt-alerts"
    )
    http_user_agent: str = "star-pulse/0.1 (projet portfolio open data)"
    http_timeout_s: float = Field(default=60.0, gt=0)
    # Respect des sources : jamais plus d'un appel toutes les 30 s par flux.
    poll_interval_s: int = Field(default=30, ge=30)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Retourne l'instance de configuration (mise en cache).

    Returns:
        Les paramètres chargés depuis l'environnement et `.env`.
    """
    return Settings()
