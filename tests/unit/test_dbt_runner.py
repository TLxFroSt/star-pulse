from pathlib import Path

from pydantic import HttpUrl, SecretStr

from star_pulse.config import Settings
from star_pulse.transform.dbt_runner import DBT_PROJECT_DIR, dbt_args, dbt_env


def test_dbt_env_maps_local_minio(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        s3_endpoint_url=HttpUrl("http://localhost:9000"),
        s3_access_key=SecretStr("key"),
        s3_secret_key=SecretStr("secret"),
        s3_bucket="lake",
        duckdb_path=tmp_path / "db.duckdb",
    )

    env = dbt_env(settings)

    assert env["STAR_PULSE_DBT_S3_ENDPOINT"] == "localhost:9000"
    assert env["STAR_PULSE_DBT_S3_USE_SSL"] == "False"
    assert env["DBT_ENV_SECRET_S3_SECRET_KEY"] == "secret"
    assert env["STAR_PULSE_S3_ACCESS_KEY"] == "key"
    assert env["STAR_PULSE_S3_BUCKET"] == "lake"
    assert env["STAR_PULSE_DUCKDB_PATH"] == str(tmp_path / "db.duckdb")


def test_dbt_env_https_uses_ssl_and_default_port() -> None:
    settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        s3_endpoint_url=HttpUrl("https://s3.example.org"),
    )

    env = dbt_env(settings)

    assert env["STAR_PULSE_DBT_S3_ENDPOINT"] == "s3.example.org:443"
    assert env["STAR_PULSE_DBT_S3_USE_SSL"] == "True"


def test_dbt_args_defaults_to_build() -> None:
    assert dbt_args([], Path("p")) == ["build", "--project-dir", "p", "--profiles-dir", "p"]


def test_dbt_args_keeps_command() -> None:
    args = dbt_args(["source", "freshness"])
    assert args[:2] == ["source", "freshness"]
    assert args[args.index("--project-dir") + 1] == str(DBT_PROJECT_DIR)


def test_dbt_project_exists() -> None:
    assert (DBT_PROJECT_DIR / "dbt_project.yml").is_file()
