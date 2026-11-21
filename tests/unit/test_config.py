"""Smoke tests : la configuration se charge et respecte ses contraintes."""

import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from star_pulse.config import Settings, get_settings


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Isole chaque test d'un éventuel `.env` local et des variables du shell."""
    monkeypatch.chdir(tmp_path)
    for key in list(os.environ):
        if key.startswith("STAR_PULSE_"):
            monkeypatch.delenv(key)
    get_settings.cache_clear()


def test_settings_load_with_defaults() -> None:
    settings = get_settings()

    assert settings.s3_bucket == "lake"
    assert settings.kafka_bootstrap_servers == "localhost:19092"
    assert settings.poll_interval_s >= 30


def test_env_variable_overrides_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STAR_PULSE_S3_BUCKET", "autre-lake")

    assert Settings().s3_bucket == "autre-lake"


def test_dotenv_file_is_read(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("STAR_PULSE_LOG_LEVEL=DEBUG\n", encoding="utf-8")

    assert Settings().log_level == "DEBUG"


def test_secrets_are_masked() -> None:
    assert "minioadmin" not in repr(Settings())


def test_poll_interval_below_30s_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STAR_PULSE_POLL_INTERVAL_S", "5")

    with pytest.raises(ValidationError):
        Settings()
