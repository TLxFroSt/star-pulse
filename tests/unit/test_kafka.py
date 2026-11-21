"""Tests de la vérification du broker."""

import pytest

from star_pulse.common.kafka import BrokerUnavailableError, check_broker


def test_unreachable_broker_gives_actionable_message() -> None:
    with pytest.raises(BrokerUnavailableError, match="make up"):
        check_broker("127.0.0.1:1", timeout_s=1.0)
