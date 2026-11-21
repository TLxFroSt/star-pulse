"""Configuration du logging applicatif (texte lisible ou JSON structuré)."""

import json
import logging
from datetime import UTC, datetime
from typing import Any

# Attributs standards d'un LogRecord : tout le reste vient de `extra=` et part dans le JSON.
_RESERVED = frozenset(vars(logging.makeLogRecord({})).keys()) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    """Formate chaque enregistrement en une ligne JSON, champs `extra` inclus."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update({k: v for k, v in vars(record).items() if k not in _RESERVED})
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    """Format texte lisible, suivi des champs `extra` en `clé=valeur`."""

    def __init__(self) -> None:
        super().__init__("%(asctime)s %(levelname)-7s %(name)s - %(message)s")

    def format(self, record: logging.LogRecord) -> str:
        extras = {k: v for k, v in vars(record).items() if k not in _RESERVED}
        line = super().format(record)
        if extras:
            line += " " + " ".join(f"{k}={v}" for k, v in extras.items())
        return line


def configure_logging(level: str = "INFO", *, json_format: bool = False) -> None:
    """Configure le logger racine.

    Args:
        level: Niveau minimal (`DEBUG`, `INFO`...).
        json_format: Sortie JSON (une ligne par événement) au lieu du texte.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter() if json_format else TextFormatter())
    logging.basicConfig(level=level, handlers=[handler], force=True)
