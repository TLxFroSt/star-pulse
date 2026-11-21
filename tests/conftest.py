"""Fixtures partagées."""

import io
import zipfile
from collections.abc import Callable
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"
GTFS_SAMPLE_DIR = FIXTURES / "gtfs_star_sample"

ZipBuilder = Callable[..., bytes]


def build_gtfs_zip(
    source: Path = GTFS_SAMPLE_DIR,
    *,
    date_time: tuple[int, int, int, int, int, int] = (2026, 11, 20, 23, 39, 0),
    exclude: frozenset[str] = frozenset(),
    reverse_order: bool = False,
) -> bytes:
    """Construit un zip GTFS à partir des fichiers texte de la fixture.

    Les paramètres permettent de simuler un ré-zippage (dates, ordre) ou un fichier manquant.
    """
    paths = sorted(p for p in source.glob("*.txt") if p.stem not in exclude)
    if reverse_order:
        paths.reverse()
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in paths:
            info = zipfile.ZipInfo(path.name, date_time=date_time)
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, path.read_bytes())
    return buffer.getvalue()


@pytest.fixture
def gtfs_zip() -> bytes:
    """Archive GTFS d'exemple (extrait anonymisé du GTFS STAR réel)."""
    return build_gtfs_zip()


@pytest.fixture
def zip_builder() -> ZipBuilder:
    """Constructeur d'archives GTFS paramétrable."""
    return build_gtfs_zip
