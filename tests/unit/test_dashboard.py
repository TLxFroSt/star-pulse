from datetime import date
from pathlib import Path

import duckdb
import pytest
from streamlit.testing.v1 import AppTest

from star_pulse.dashboard.data import Filters, GoldReader
from star_pulse.dashboard.page import (
    format_delay,
    format_pct,
    punctuality_color,
)

DAY = date(2026, 11, 21)


@pytest.fixture
def gold_dir(tmp_path: Path) -> Path:
    """Gold minimal : 2 lignes, 3 arrêts, 6 passages (dont 1 terminus, 1 supprimé)."""
    con = duckdb.connect()
    root = tmp_path.as_posix()
    con.execute(
        f"""
        copy (select * from (values
            ('r1', 'C1', 'Ligne 1', '#E2001A', 1),
            ('r2', '12', 'Ligne 12', '#0072BC', 2)
        ) t(route_id, route_short_name, route_long_name, route_color, route_sort_order))
        to '{root}/dim_routes.parquet'
        """
    )
    con.execute(
        f"""
        copy (select * from (values
            ('s1', 'République', 'Rennes', 48.11, -1.68),
            ('s2', 'Gares', 'Rennes', 48.10, -1.67),
            ('s3', 'Atalante', 'Cesson-Sévigné', 48.12, -1.63)
        ) t(stop_id, stop_name, stop_city, stop_lat, stop_lon))
        to '{root}/dim_stops.parquet'
        """
    )
    con.execute(
        f"""
        copy (
            select
                passage_id, route_id, stop_id, scheduled_hour, is_terminus_arrival,
                passage_status, punctuality_class,
                cast(delay_s as integer) as delay_s,
                date '2026-11-21' as service_date,
                'samedi' as service_day_type
            from (values
                ('p1', 'r1', 's1', 8, false, 'observed', 'on_time', 30),
                ('p2', 'r1', 's2', 8, false, 'observed', 'late', 400),
                ('p3', 'r1', 's3', 9, false, 'observed', 'early', -120),
                ('p4', 'r2', 's1', 9, false, 'observed', 'on_time', 0),
                ('p5', 'r2', 's2', 9, false, 'skipped', 'skipped', null),
                ('p6', 'r2', 's3', 9, true, 'observed', 'late', 900)
            ) t(passage_id, route_id, stop_id, scheduled_hour, is_terminus_arrival,
                passage_status, punctuality_class, delay_s)
        ) to '{root}/fct_stop_passages.parquet'
        """
    )
    return tmp_path


@pytest.fixture
def reader(gold_dir: Path) -> GoldReader:
    return GoldReader(duckdb.connect(), gold_dir.as_posix())


def test_date_range(reader: GoldReader) -> None:
    assert reader.date_range() == (DAY, DAY)


def test_date_range_none_when_gold_missing(tmp_path: Path) -> None:
    assert GoldReader(duckdb.connect(), tmp_path.as_posix()).date_range() is None


def test_summary_excludes_terminus_and_skipped(reader: GoldReader) -> None:
    summary = reader.summary(Filters(DAY, DAY))
    assert summary["n_observed"] == 4  # p6 (terminus) exclu, p5 supprimé
    assert summary["n_skipped"] == 1
    assert summary["pct_on_time"] == pytest.approx(0.5)
    assert summary["pct_late"] == pytest.approx(0.25)
    assert summary["pct_early"] == pytest.approx(0.25)


def test_filters_by_route_and_day_type(reader: GoldReader) -> None:
    assert reader.summary(Filters(DAY, DAY, route_ids=("r2",)))["n_observed"] == 1
    assert reader.summary(Filters(DAY, DAY, day_types=("semaine",)))["n_observed"] == 0


def test_by_route_sorted_worst_first(reader: GoldReader) -> None:
    df = reader.by_route(Filters(DAY, DAY), min_observed=1)
    assert list(df["route_short_name"]) == ["C1", "12"]
    assert df["pct_on_time"].iloc[0] == pytest.approx(1 / 3)


def test_by_hour_and_by_stop(reader: GoldReader) -> None:
    filters = Filters(DAY, DAY)
    assert list(reader.by_hour(filters)["scheduled_hour"]) == [8, 9]
    stops = reader.by_stop(filters, min_observed=1)
    assert set(stops["stop_id"]) == {"s1", "s2", "s3"}
    assert {"stop_lat", "stop_lon", "pct_on_time"} <= set(stops.columns)


def test_formatters() -> None:
    assert format_delay(83) == "+1 min 23 s"
    assert format_delay(-40) == "-40 s"
    assert format_delay(None) == "—"
    assert format_pct(0.7812) == "78,1 %"


def test_punctuality_color_scale() -> None:
    assert punctuality_color(0.3)[:3] == [214, 39, 40]
    assert punctuality_color(0.95)[:3] == [44, 160, 44]
    assert punctuality_color(0.7)[:3] == [255, 160, 0]


def _app(root: str) -> None:
    import duckdb as ddb

    from star_pulse.dashboard.data import GoldReader as Reader
    from star_pulse.dashboard.page import render

    render(Reader(ddb.connect(), root))


def test_page_renders(gold_dir: Path) -> None:
    app = AppTest.from_function(_app, args=(gold_dir.as_posix(),)).run(timeout=30)
    assert not app.exception
    assert [m.label for m in app.metric][:1] == ["À l'heure"]
    assert app.metric[0].value == "50,0 %"


def test_page_shows_help_when_gold_empty(tmp_path: Path) -> None:
    app = AppTest.from_function(_app, args=(tmp_path.as_posix(),)).run(timeout=30)
    assert not app.exception
    assert "make dbt-build" in app.info[0].value
