"""Page Streamlit : KPI de ponctualité, par heure, par ligne et carte des arrêts."""

from datetime import date, timedelta
from typing import Any

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st

from star_pulse.config import get_settings
from star_pulse.dashboard.data import DAY_TYPES, Filters, GoldReader

CACHE_TTL_S = 300  # le gold est reconstruit toutes les 15 min par Dagster
RENNES_CENTER = (48.111, -1.68)
EMPTY_MESSAGE = (
    "Pas encore de données gold. Lancer `make up`, `make produce-rt`, `make consume-rt`, "
    "attendre quelques minutes puis `make dbt-build` (ou `make dagster`)."
)


def format_delay(seconds: float | None) -> str:
    """Formate un retard en « +1 min 05 s » / « -40 s »."""
    if seconds is None or pd.isna(seconds):
        return "—"
    sign = "-" if seconds < 0 else "+"
    total = round(abs(seconds))
    minutes, secs = divmod(total, 60)
    return f"{sign}{minutes} min {secs:02d} s" if minutes else f"{sign}{secs} s"


def format_pct(value: float | None) -> str:
    """Formate une part en pourcentage français (« 78,1 % »)."""
    if value is None or pd.isna(value):
        return "—"
    return f"{value * 100:.1f} %".replace(".", ",")


def punctuality_color(pct_on_time: float) -> list[int]:
    """Couleur RGBA d'un arrêt : rouge (≤ 50 %) → orange → vert (≥ 90 %)."""
    t = min(max((pct_on_time - 0.5) / 0.4, 0.0), 1.0)
    red, orange, green = (214, 39, 40), (255, 160, 0), (44, 160, 44)
    start, end, u = (red, orange, t * 2) if t < 0.5 else (orange, green, (t - 0.5) * 2)
    r, g, b = (round(a + (c - a) * u) for a, c in zip(start, end, strict=True))
    return [r, g, b, 200]


@st.cache_resource
def _reader() -> GoldReader:
    return GoldReader.from_settings(get_settings())


@st.cache_data(ttl=CACHE_TTL_S, show_spinner=False)
def _cached(_reader: GoldReader, root: str, method: str, *args: Any) -> Any:
    return getattr(_reader, method)(*args)


def _sidebar(reader: GoldReader, first: date, last: date) -> tuple[Filters, int]:
    st.sidebar.header("Filtres")
    default_start = max(first, last - timedelta(days=6))
    period = st.sidebar.date_input(
        "Période (jours de service)",
        value=(default_start, last),
        min_value=first,
        max_value=last,
    )
    start, end = (period[0], period[-1]) if isinstance(period, tuple) and period else (first, last)
    day_types = st.sidebar.multiselect(
        "Type de jour", DAY_TYPES, default=list(DAY_TYPES), placeholder="Tous"
    )
    routes: pd.DataFrame = _cached(reader, reader.root, "routes")
    labels = dict(zip(routes["route_id"], routes["route_short_name"], strict=True))
    route_ids = st.sidebar.multiselect(
        "Lignes",
        list(labels),
        format_func=lambda r: labels.get(r, r),
        placeholder="Toutes les lignes",
    )
    min_passages = st.sidebar.slider("Carte : passages minimum par arrêt", 1, 100, 10)
    filters = Filters(start, end, tuple(day_types) or DAY_TYPES, tuple(route_ids))
    return filters, min_passages


def _kpis(summary: dict[str, Any]) -> None:
    cols = [*st.columns(3), *st.columns(3)]
    cols[0].metric(
        "À l'heure", format_pct(summary["pct_on_time"]), help="Retard entre -1 et +3 min"
    )
    cols[1].metric("En retard", format_pct(summary["pct_late"]), help="Retard > 3 min")
    cols[2].metric("En avance", format_pct(summary["pct_early"]), help="Avance > 1 min")
    cols[3].metric("Retard médian", format_delay(summary["median_delay_s"]))
    cols[4].metric(
        "Retard p90",
        format_delay(summary["p90_delay_s"]),
        help="90 % des passages ont un retard inférieur",
    )
    cols[5].metric(
        "Passages observés",
        f"{int(summary['n_observed']):,}".replace(",", " "),
        help=f"{int(summary['n_skipped'])} arrêts supprimés",
    )


def _hour_chart(df: pd.DataFrame) -> alt.Chart:
    chart: alt.Chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("scheduled_hour:O", title="Heure théorique"),
            y=alt.Y("pct_on_time:Q", title="À l'heure", axis=alt.Axis(format="%")),
            color=alt.Color(
                "pct_on_time:Q",
                scale=alt.Scale(domain=[0.5, 0.7, 0.9], range=["#d62728", "#ffa000", "#2ca02c"]),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("scheduled_hour:O", title="Heure"),
                alt.Tooltip("pct_on_time:Q", title="À l'heure", format=".1%"),
                alt.Tooltip("p90_delay_s:Q", title="Retard p90 (s)", format=".0f"),
                alt.Tooltip("n_observed:Q", title="Passages"),
            ],
        )
    )
    return chart


def _route_chart(df: pd.DataFrame) -> alt.Chart:
    chart: alt.Chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            y=alt.Y(
                "route_short_name:N",
                sort=None,
                title="Ligne",
                axis=alt.Axis(labelOverlap=False),
            ),
            x=alt.X("pct_on_time:Q", title="À l'heure", axis=alt.Axis(format="%")),
            color=alt.Color("route_color:N", scale=None, legend=None),
            tooltip=[
                alt.Tooltip("route_short_name:N", title="Ligne"),
                alt.Tooltip("route_long_name:N", title="Trajet"),
                alt.Tooltip("pct_on_time:Q", title="À l'heure", format=".1%"),
                alt.Tooltip("median_delay_s:Q", title="Retard médian (s)", format=".0f"),
                alt.Tooltip("p90_delay_s:Q", title="Retard p90 (s)", format=".0f"),
                alt.Tooltip("n_observed:Q", title="Passages"),
            ],
        )
        .properties(height=max(200, 22 * len(df)))
    )
    return chart


def _stop_map(df: pd.DataFrame) -> pdk.Deck:
    data = df.assign(
        color=df["pct_on_time"].map(punctuality_color),
        pct_label=df["pct_on_time"].map(format_pct),
        p90_label=df["p90_delay_s"].map(format_delay),
    )
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=data,
        get_position="[stop_lon, stop_lat]",
        get_fill_color="color",
        get_radius=60,
        radius_min_pixels=3,
        pickable=True,
    )
    return pdk.Deck(
        layers=[layer],
        initial_view_state=pdk.ViewState(
            latitude=RENNES_CENTER[0], longitude=RENNES_CENTER[1], zoom=11.5
        ),
        tooltip={
            "html": "<b>{stop_name}</b> ({stop_city})<br/>À l'heure : {pct_label}"
            "<br/>Retard p90 : {p90_label}<br/>{n_observed} passages",
        },
    )


def render(reader: GoldReader) -> None:
    """Construit la page à partir d'un lecteur gold."""
    st.title("STAR Pulse — ponctualité du réseau STAR (Rennes)")
    bounds = _cached(reader, reader.root, "date_range")
    if bounds is None:
        st.info(EMPTY_MESSAGE)
        return
    filters, min_passages = _sidebar(reader, *bounds)

    summary = _cached(reader, reader.root, "summary", filters)
    if not summary["n_observed"]:
        st.warning("Aucun passage pour ces filtres.")
        return
    _kpis(summary)

    left, right = st.columns(2)
    with left:
        st.subheader("Ponctualité par heure")
        st.altair_chart(_hour_chart(_cached(reader, reader.root, "by_hour", filters)))
    with right:
        st.subheader("Lignes, de la moins à la plus ponctuelle")
        st.altair_chart(_route_chart(_cached(reader, reader.root, "by_route", filters)))

    st.subheader("Carte des arrêts")
    stops = _cached(reader, reader.root, "by_stop", filters, min_passages)
    st.pydeck_chart(_stop_map(stops))
    st.caption(
        "À l'heure = retard entre -1 et +3 min par rapport à l'horaire théorique. "
        "Arrivées au terminus exclues, arrêts supprimés comptés à part. "
        "Source : GTFS et GTFS-RT STAR (open data Rennes Métropole)."
    )


def main() -> None:
    """Point d'entrée de `streamlit run dashboard/app.py`."""
    st.set_page_config(page_title="STAR Pulse", page_icon="🚌", layout="wide")
    render(_reader())
