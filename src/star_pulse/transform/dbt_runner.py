"""Lance dbt avec la configuration de l'application.

dbt ne lit pas `.env` : ce wrapper charge `Settings` (variables d'environnement
puis `.env`) et les traduit en variables lues par `dbt/profiles.yml`.

Exemples :
    python -m star_pulse.transform.dbt_runner build
    python -m star_pulse.transform.dbt_runner source freshness
"""

import logging
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from dbt.cli.main import dbtRunner

from star_pulse.common.logging import configure_logging
from star_pulse.config import Settings, get_settings

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = Path(__file__).resolve().parents[3] / "dbt"


def dbt_env(settings: Settings) -> dict[str, str]:
    """Traduit la configuration en variables d'environnement pour dbt.

    Args:
        settings: Configuration de l'application.

    Returns:
        Les variables attendues par `dbt/profiles.yml` et les sources. Le secret
        S3 est préfixé `DBT_ENV_SECRET_` pour que dbt le masque dans ses logs.
    """
    endpoint = settings.s3_endpoint_url
    return {
        "STAR_PULSE_DUCKDB_PATH": str(settings.duckdb_path.resolve()),
        "STAR_PULSE_S3_ACCESS_KEY": settings.s3_access_key.get_secret_value(),
        "DBT_ENV_SECRET_S3_SECRET_KEY": settings.s3_secret_key.get_secret_value(),
        "STAR_PULSE_S3_REGION": settings.s3_region,
        "STAR_PULSE_S3_BUCKET": settings.s3_bucket,
        "STAR_PULSE_DBT_S3_ENDPOINT": f"{endpoint.host}:{endpoint.port}",
        "STAR_PULSE_DBT_S3_USE_SSL": str(endpoint.scheme == "https"),
    }


def dbt_args(args: Sequence[str], project_dir: Path = DBT_PROJECT_DIR) -> list[str]:
    """Complète les arguments dbt avec les dossiers du projet et du profil.

    Args:
        args: Commande dbt (`build` par défaut si vide).
        project_dir: Dossier du projet dbt.

    Returns:
        La liste d'arguments à passer à dbt.
    """
    command = list(args) or ["build"]
    return [*command, "--project-dir", str(project_dir), "--profiles-dir", str(project_dir)]


def main(argv: Sequence[str] | None = None) -> int:
    """Point d'entrée : lance la commande dbt demandée.

    Args:
        argv: Arguments de la commande dbt (par défaut ceux de la ligne de commande).

    Returns:
        Code de sortie : 0 si dbt a réussi, 1 sinon.
    """
    settings = get_settings()
    configure_logging(settings.log_level, json_format=settings.log_json)
    os.environ.update(dbt_env(settings))
    settings.duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    args = dbt_args(sys.argv[1:] if argv is None else argv)
    logger.info("lancement de dbt", extra={"dbt_args": " ".join(args)})
    result = dbtRunner().invoke(args)
    return 0 if result.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
