from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from streamlit.testing.v1 import AppTest

from star_pulse.dashboard.data import Filters, GoldReader
from star_pulse.dashboard.page import (
    CRITICAL,
    GOOD,
    NBSP,
    STOPS_KEY,
    day_label,
    format_count,
    format_delay,
    format_pct,
    format_points,
    freshness,
    hour_rank,
    period_label,
    punctuality_band,
    recent_passages_table,
    split_bar_html,
    stop_label,
)

DAY = date(2026, 11, 21)
DAY_BEFORE = date(2026, 11, 20)
LAST_OBSERVED = datetime(2026, 11, 21, 13, 0, tzinfo=UTC)


@pytest.fixture
def gold_dir(tmp_path: Path) -> Path:
    """Gold minimal : 2 lignes, 4 arrêts (dont 2 « République »), 7 passages sur 2 jours.

    Le 21/11 : 6 passages dont 1 terminus et 1 supprimé. Le 20/11 : 1 passage supprimé.
    """
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
            ('s3', 'Atalante', 'Cesson-Sévigné', 48.12, -1.63),
            ('s4', 'République', 'Rennes', 48.1102, -1.6798)
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
                service_date,
                case when isodow(service_date) = 6 then 'samedi' else 'semaine' end
                    as service_day_type,
                cast(
                    cast(service_date as timestamp)
                    + to_hours(scheduled_hour) + to_minutes(minute) as timestamptz
                ) as scheduled_at,
                timestamptz '2026-11-21 13:00:00+00' as observed_at
            from (values
                ('p1', 'r1', 's1', 8, 5, false, 'observed', 'on_time', 30, date '2026-11-21'),
                ('p2', 'r1', 's2', 8, 10, false, 'observed', 'late', 400, date '2026-11-21'),
                ('p3', 'r1', 's3', 9, 0, false, 'observed', 'early', -120, date '2026-11-21'),
                ('p4', 'r2', 's1', 9, 20, false, 'observed', 'on_time', 0, date '2026-11-21'),
                ('p5', 'r2', 's2', 9, 25, false, 'skipped', 'skipped', null, date '2026-11-21'),
                ('p6', 'r2', 's3', 9, 30, true, 'observed', 'late', 900, date '2026-11-21'),
                ('p7', 'r2', 's4', 9, 0, false, 'skipped', 'skipped', null, date '2026-11-20')
            ) t(passage_id, route_id, stop_id, scheduled_hour, minute, is_terminus_arrival,
                passage_status, punctuality_class, delay_s, service_date)
        ) to '{root}/fct_stop_passages.parquet'
        """
    )
    return tmp_path


@pytest.fixture
def reader(gold_dir: Path) -> GoldReader:
    return GoldReader(duckdb.connect(), gold_dir.as_posix())


def test_date_range(reader: GoldReader) -> None:
    assert reader.date_range() == (DAY_BEFORE, DAY)


def test_date_range_none_when_gold_missing(tmp_path: Path) -> None:
    reader = GoldReader(duckdb.connect(), tmp_path.as_posix())
    assert reader.date_range() is None
    assert reader.last_observed() is None


def test_last_observed(reader: GoldReader) -> None:
    assert reader.last_observed() == LAST_OBSERVED


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


def test_stops_grouped_by_name_and_filtered_by_route(reader: GoldReader) -> None:
    stops = reader.stops()
    groups = dict(zip(stops["stop_name"], stops["stop_ids"].map(list), strict=True))
    assert groups == {"Atalante": ["s3"], "Gares": ["s2"], "République": ["s1", "s4"]}
    r1 = reader.stops(("r1",))
    assert list(r1.loc[r1["stop_name"] == "République", "stop_ids"].iloc[0]) == ["s1"]


def test_stop_filter(reader: GoldReader) -> None:
    summary = reader.summary(Filters(DAY, DAY, stop_ids=("s1",)))
    assert summary["n_observed"] == 2
    assert summary["pct_on_time"] == pytest.approx(1.0)


def test_by_stop_groups_directions(reader: GoldReader) -> None:
    stops = reader.by_stop(Filters(DAY_BEFORE, DAY), min_observed=1)
    republique = stops[stops["stop_name"] == "République"].iloc[0]
    assert list(republique["stop_ids"]) == ["s1", "s4"]
    assert republique["n_observed"] == 2


def test_by_day_hour(reader: GoldReader) -> None:
    grid = reader.by_day_hour(Filters(DAY_BEFORE, DAY))
    assert len(grid) == 3  # (20/11, 9 h), (21/11, 8 h), (21/11, 9 h)
    assert {"service_date", "scheduled_hour", "pct_late"} <= set(grid.columns)


def test_recent_passages(reader: GoldReader) -> None:
    recent = reader.recent_passages(Filters(DAY, DAY), limit=2)
    assert list(recent["route_short_name"]) == ["12", "C1"]  # p4 (9 h 20) puis p3 (9 h 00)
    assert list(recent["stop_name"]) == ["République", "Atalante"]
    table = recent_passages_table(recent, with_day=False, with_stop=True)
    assert list(table.columns) == ["Prévu", "Arrêt", "Ligne", "Retard"]
    assert list(table["Retard"]) == [f"+0{NBSP}s", f"-2{NBSP}min{NBSP}00{NBSP}s"]


def test_by_day_hour_and_stop(reader: GoldReader) -> None:
    filters = Filters(DAY, DAY)
    daily = reader.by_day(filters)
    assert list(daily["service_date"].dt.date) == [DAY]
    assert daily["pct_on_time"].iloc[0] == pytest.approx(0.5)
    hours = reader.by_hour(filters)
    assert list(hours["scheduled_hour"]) == [8, 9]
    assert {"pct_late", "pct_early", "median_delay_s"} <= set(hours.columns)
    stops = reader.by_stop(filters, min_observed=1)
    assert set(stops["stop_name"]) == {"République", "Gares", "Atalante"}
    assert {"stop_lat", "stop_lon", "pct_on_time"} <= set(stops.columns)


def test_formatters() -> None:
    def nbsp(text: str) -> str:
        return text.replace(" ", NBSP)

    assert format_delay(83) == nbsp("+1 min 23 s")
    assert format_delay(-40) == nbsp("-40 s")
    assert format_delay(None) == "—"
    assert format_pct(0.7812) == nbsp("78,1 %")
    assert format_pct(0.7812, digits=0) == nbsp("78 %")
    assert format_points(0.021) == nbsp("+2,1 pts")
    assert format_count(11741) == "11\u202f741"


def test_labels() -> None:
    assert day_label(DAY) == "sam. 21/11"
    assert stop_label("République", "Rennes") == "République (Rennes)"
    assert stop_label("Gares", None) == "Gares"
    assert sorted([0, 23, 4, 8], key=hour_rank) == [4, 8, 23, 0]


def test_period_label() -> None:
    assert period_label(DAY, DAY) == "le 21/11/2026"
    assert period_label(date(2026, 11, 15), DAY) == "du 15/11 au 21/11/2026"
    assert period_label(date(2025, 12, 30), date(2026, 1, 2)) == "du 30/12/2025 au 02/01/2026"


def test_freshness() -> None:
    assert freshness(LAST_OBSERVED, LAST_OBSERVED + timedelta(minutes=12)) == (
        f"il y a 12{NBSP}min",
        True,
    )
    text, fresh = freshness(LAST_OBSERVED, LAST_OBSERVED + timedelta(hours=3))
    assert (text, fresh) == (f"il y a 3{NBSP}h", False)


def test_punctuality_band() -> None:
    assert punctuality_band(0.95).color == GOOD
    assert punctuality_band(0.85).color == GOOD
    assert punctuality_band(0.3).color == CRITICAL
    assert [punctuality_band(p).floor for p in (0.8, 0.6)] == [0.7, 0.55]


def test_split_bar_html_labels_wide_segments_only() -> None:
    html = split_bar_html({"pct_early": 0.03, "pct_on_time": 0.78, "pct_late": 0.19})
    assert html.count('class="sp-seg"') == 3
    assert f">78,0{NBSP}%</div>" in html  # valeur dans le segment
    assert f"En avance 3,0{NBSP}%" in html  # segment trop étroit : valeur dans la légende


def _app(root: str) -> None:
    import duckdb as ddb

    from star_pulse.dashboard.data import GoldReader as Reader
    from star_pulse.dashboard.page import render

    render(Reader(ddb.connect(), root))


def test_page_renders(gold_dir: Path) -> None:
    app = AppTest.from_function(_app, args=(gold_dir.as_posix(),)).run(timeout=30)
    assert not app.exception
    assert [m.label for m in app.metric] == [
        "À l'heure",
        "Retard médian",
        "Retard p90",
        "Passages observés",
    ]
    assert app.metric[0].value == f"50,0{NBSP}%"
    assert any("Mehdi Mami" in caption.value for caption in app.caption)


def test_page_with_selected_stop(gold_dir: Path) -> None:
    app = AppTest.from_function(_app, args=(gold_dir.as_posix(),))
    app.session_state[STOPS_KEY] = ["République (Rennes)"]
    app.run(timeout=30)
    assert not app.exception
    assert app.metric[0].value == f"100,0{NBSP}%"  # 2 passages à l'heure sur 2
    assert "À l'heure à l'arrêt" in [m.label for m in app.metric]
    app.button[0].click().run(timeout=30)  # « Revenir à tout le réseau »
    assert app.session_state[STOPS_KEY] == []
    assert app.metric[0].value == f"50,0{NBSP}%"


def test_page_shows_help_when_gold_empty(tmp_path: Path) -> None:
    app = AppTest.from_function(_app, args=(tmp_path.as_posix(),)).run(timeout=30)
    assert not app.exception
    assert "make dbt-build" in app.info[0].value
