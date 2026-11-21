"""Réglages des tests d'intégration."""

import os
from collections.abc import Generator

import pytest


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[None]
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    """En CI, un test d'intégration sauté faute d'infra devient un échec.

    En local, ces tests se sautent quand `make up` n'a pas été lancé. En CI
    (variable `CI` posée par GitHub Actions), l'infra est censée tourner : un
    saut voudrait dire que la CI est verte sans avoir rien testé.
    """
    report = yield
    if os.environ.get("CI") and report.skipped and item.get_closest_marker("integration"):
        # Pour un saut, longrepr vaut (fichier, ligne, raison).
        reason = report.longrepr[2] if isinstance(report.longrepr, tuple) else report.longrepr
        report.outcome = "failed"
        report.longrepr = f"Test d'intégration sauté en CI, l'infra doit tourner. {reason}"
    return report
