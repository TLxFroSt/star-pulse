"""Client HTTP partagé : User-Agent explicite, timeout et retries avec backoff."""

import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass

import httpx

from star_pulse.config import Settings

logger = logging.getLogger(__name__)

RETRYABLE_STATUS = frozenset({408, 429, 500, 502, 503, 504})


@dataclass(frozen=True)
class RetryPolicy:
    """Politique de retry à backoff exponentiel.

    Attributes:
        max_attempts: Nombre total de tentatives (première incluse).
        base_delay_s: Délai avant le premier retry ; doublé à chaque tentative.
        max_delay_s: Plafond du délai entre deux tentatives.
    """

    max_attempts: int = 4
    base_delay_s: float = 2.0
    max_delay_s: float = 30.0

    def delay(self, attempt: int) -> float:
        """Délai à attendre après l'échec de la tentative `attempt` (1-indexée)."""
        return float(min(self.base_delay_s * 2 ** (attempt - 1), self.max_delay_s))


def build_client(
    settings: Settings, *, transport: httpx.BaseTransport | None = None
) -> httpx.Client:
    """Construit le client HTTP configuré pour les sources open data.

    Args:
        settings: Configuration de l'application.
        transport: Transport à injecter (tests : `httpx.MockTransport`).

    Returns:
        Un `httpx.Client` à fermer par l'appelant.
    """
    return httpx.Client(
        headers={"User-Agent": settings.http_user_agent},
        timeout=settings.http_timeout_s,
        follow_redirects=True,
        transport=transport,
    )


def get_with_retry(
    client: httpx.Client,
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    policy: RetryPolicy = RetryPolicy(),
    sleep: Callable[[float], None] = time.sleep,
) -> httpx.Response:
    """GET avec retries sur erreurs réseau et statuts transitoires.

    Un 304 est renvoyé tel quel (requête conditionnelle). Les autres erreurs 4xx
    ne sont pas retentées.

    Args:
        client: Client HTTP.
        url: URL à appeler.
        headers: En-têtes supplémentaires (ex. `If-None-Match`).
        policy: Politique de retry.
        sleep: Fonction d'attente, injectable pour les tests.

    Returns:
        La réponse (statut 2xx ou 304).

    Raises:
        httpx.HTTPError: Après épuisement des tentatives, ou sur une erreur 4xx.
    """
    for attempt in range(1, policy.max_attempts + 1):
        try:
            response = client.get(url, headers=headers)
        except httpx.TransportError as exc:
            if attempt == policy.max_attempts:
                raise
            reason = type(exc).__name__
        else:
            if response.status_code not in RETRYABLE_STATUS:
                if response.status_code != httpx.codes.NOT_MODIFIED:
                    response.raise_for_status()
                return response
            if attempt == policy.max_attempts:
                response.raise_for_status()
            reason = f"HTTP {response.status_code}"

        delay = policy.delay(attempt)
        logger.warning(
            "Échec de la requête, nouvel essai",
            extra={"url": url, "attempt": attempt, "reason": reason, "retry_in_s": delay},
        )
        sleep(delay)

    raise AssertionError("unreachable")  # pragma: no cover
