"""Utilitaires Kafka / Redpanda partagés par le producteur et les consumers."""

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient


class BrokerUnavailableError(RuntimeError):
    """Le broker Redpanda ne répond pas."""


def check_broker(bootstrap_servers: str, *, timeout_s: float = 5.0) -> None:
    """Vérifie que le broker répond, avec un message d'erreur exploitable sinon.

    Args:
        bootstrap_servers: Adresse(s) du broker.
        timeout_s: Délai maximal d'attente.

    Raises:
        BrokerUnavailableError: Si le broker est injoignable.
    """
    # log_level=0 : pas de traces librdkafka en rafale pendant la vérification.
    admin = AdminClient({"bootstrap.servers": bootstrap_servers, "log_level": 0})
    try:
        admin.list_topics(timeout=timeout_s)
    except KafkaException as exc:
        raise BrokerUnavailableError(
            f"Redpanda injoignable sur {bootstrap_servers} : lancer `make up`"
        ) from exc
