"""Page Streamlit : KPI de ponctualité, historique, lignes et carte interactive des arrêts.

Partis pris graphiques : une seule couleur d'accent (bleu) pour les mesures ;
les couleurs de statut (vert, ambre, orange, rouge) sont réservées au bon ou
mauvais et toujours accompagnées d'un libellé ; grilles et axes discrets.
"""

import math
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from functools import partial
from html import escape
from typing import Any
from zoneinfo import ZoneInfo

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st

from star_pulse.config import get_settings
from star_pulse.dashboard.data import DAY_TYPES, Filters, GoldReader

CACHE_TTL_S = 300  # le gold est reconstruit toutes les 15 min par Dagster
LOCAL_TZ = ZoneInfo("Europe/Paris")
FRESHNESS_LIMIT = timedelta(minutes=30)
TOP_ROUTES = 15
MIN_HOUR_PASSAGES = 20  # en dessous, la part horaire est trop bruitée
STOPS_KEY = "stops"  # arrêts choisis, dans la barre latérale ou par clic sur la carte
MAP_LAYER_ID = "stops"
WEEKDAYS = ("lun.", "mar.", "mer.", "jeu.", "ven.", "sam.", "dim.")
SERVICE_DAY_START_HOUR = 4  # une journée de service va de 4 h à 3 h le lendemain
AUTHOR = "Mehdi Mami"
AUTHOR_URL = "https://github.com/TLxFroSt"
REPO_URL = "https://github.com/TLxFroSt/star-pulse"
EMPTY_MESSAGE = (
    "Pas encore de données gold. Lancer `make up`, `make produce-rt`, `make consume-rt`, "
    "attendre quelques minutes puis `make dbt-build` (ou `make dagster`)."
)

NBSP = "\u00a0"  # espace insécable avant les unités
THIN_NBSP = "\u202f"  # séparateur de milliers
SEP = " \u00b7 "

# --- Palette ----------------------------------------------------------------
ACCENT = "#2a78d6"
ACCENT_HOVER = "#1c5cab"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
GOOD = "#0ca30c"
WARNING = "#fab219"
SERIOUS = "#ec835a"
CRITICAL = "#d03b3b"
SEQUENTIAL = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
FONT = "Inter, 'Source Sans 3', sans-serif"
PCT_LABEL = f"replace(format(datum.value, '.0%'), '%', '{NBSP}%')"

CSS = f"""
<style>
[data-testid="stMainBlockContainer"] {{ padding-top: 2.5rem; padding-bottom: 3rem; }}
.sp-subtitle {{ color: {INK_SECONDARY}; font-size: 1.02rem; margin: -0.5rem 0 0; }}
.sp-byline {{ color: {INK_MUTED}; font-size: 0.9rem; margin: 0.35rem 0 0; }}
.sp-byline a {{ color: {INK_SECONDARY}; font-weight: 600; text-decoration: none; }}
.sp-byline a:hover {{ color: {ACCENT}; text-decoration: underline; }}
.sp-card-title {{ color: {INK}; font-size: 1.05rem; font-weight: 600; }}
.sp-card-sub {{ color: {INK_SECONDARY}; font-size: 0.875rem; margin-top: 0.1rem; }}
.sp-split {{ display: flex; gap: 2px; height: 28px; margin: 0.6rem 0 0.7rem; }}
.sp-seg {{
  display: flex; align-items: center; padding: 0 8px; border-radius: 4px;
  font-size: 0.82rem; font-weight: 600; white-space: nowrap; overflow: hidden;
}}
.sp-legend {{
  display: flex; flex-wrap: wrap; gap: 0.35rem 1.4rem;
  color: {INK_SECONDARY}; font-size: 0.85rem;
}}
.sp-legend > span {{ display: inline-flex; align-items: center; gap: 0.45rem; }}
.sp-key {{ width: 10px; height: 10px; border-radius: 2px; }}
.sp-dot {{ width: 11px; height: 11px; border-radius: 50%; box-shadow: 0 0 0 1px #fff; }}
.sp-muted {{ color: {INK_MUTED}; }}
.sp-hint {{
  color: {INK_SECONDARY}; font-size: 0.9rem; padding: 1rem;
  border: 1px dashed {AXIS}; border-radius: 0.5rem; background: #fafaf8;
}}
.sp-chips {{ display: flex; flex-wrap: wrap; gap: 0.3rem; margin-top: 0.4rem; }}
.sp-chip {{
  font-size: 0.78rem; font-weight: 600; color: {INK}; background: #f0efec;
  border-radius: 0.35rem; padding: 0.1rem 0.45rem;
}}
</style>
"""


# --- Formatage --------------------------------------------------------------


def format_delay(seconds: float | None) -> str:
    """Formate un retard en « +1 min 05 s » / « -40 s »."""
    if seconds is None or pd.isna(seconds):
        return "—"
    sign = "-" if seconds < 0 else "+"
    minutes, secs = divmod(round(abs(seconds)), 60)
    if minutes:
        return f"{sign}{minutes}{NBSP}min{NBSP}{secs:02d}{NBSP}s"
    return f"{sign}{secs}{NBSP}s"


def format_pct(value: float | None, digits: int = 1) -> str:
    """Formate une part en pourcentage français (« 78,1 % »)."""
    if value is None or pd.isna(value):
        return "—"
    return f"{value * 100:.{digits}f}{NBSP}%".replace(".", ",")


def format_points(delta: float) -> str:
    """Écart entre deux parts, en points de pourcentage (« +2,1 pts »)."""
    return f"{delta * 100:+.1f}{NBSP}pts".replace(".", ",")


def format_count(value: float) -> str:
    """Entier avec séparateur de milliers français (« 11 741 »)."""
    return f"{int(value):,}".replace(",", THIN_NBSP)


def period_label(start: date, end: date) -> str:
    """« le 21/11/2026 » ou « du 15/11 au 21/11/2026 »."""
    if start == end:
        return f"le {start:%d/%m/%Y}"
    first = f"{start:%d/%m}" if start.year == end.year else f"{start:%d/%m/%Y}"
    return f"du {first} au {end:%d/%m/%Y}"


def day_label(day: date) -> str:
    """« sam. 21/11 »."""
    return f"{WEEKDAYS[day.weekday()]} {day:%d/%m}"


def hour_rank(hour: int) -> int:
    """Rang d'une heure dans la journée de service (4 h en premier, 3 h en dernier)."""
    return (int(hour) - SERVICE_DAY_START_HOUR) % 24


def stop_label(name: str, city: str | None) -> str:
    """Nom affiché d'un arrêt : « République (Rennes) »."""
    return f"{name} ({city})" if city else name


def freshness(last_observed: datetime, now: datetime) -> tuple[str, bool]:
    """Âge des données (« il y a 12 min ») et indicateur « à jour »."""
    age = now - last_observed
    minutes = max(int(age.total_seconds() // 60), 0)
    if minutes < 1:
        text = "à l'instant"
    elif minutes < 60:
        text = f"il y a {minutes}{NBSP}min"
    elif minutes < 48 * 60:
        text = f"il y a {minutes // 60}{NBSP}h"
    else:
        text = f"le {last_observed.astimezone(LOCAL_TZ):%d/%m/%Y}"
    return text, age <= FRESHNESS_LIMIT


@dataclass(frozen=True)
class Band:
    """Classe de ponctualité d'un arrêt sur la carte."""

    label: str
    color: str
    floor: float


STOP_BANDS = (
    Band(f"85{NBSP}% et plus", GOOD, 0.85),
    Band(f"70 à 85{NBSP}%", WARNING, 0.70),
    Band(f"55 à 70{NBSP}%", SERIOUS, 0.55),
    Band(f"moins de 55{NBSP}%", CRITICAL, 0.0),
)


def punctuality_band(pct_on_time: float) -> Band:
    """Classe d'un arrêt selon sa part de passages à l'heure."""
    return next((band for band in STOP_BANDS if pct_on_time >= band.floor), STOP_BANDS[-1])


def _rgba(hex_color: str, alpha: int = 235) -> list[int]:
    return [int(hex_color[i : i + 2], 16) for i in (1, 3, 5)] + [alpha]


# --- Cache ------------------------------------------------------------------


@st.cache_resource
def _reader() -> GoldReader:
    return GoldReader.from_settings(get_settings())


@st.cache_data(ttl=CACHE_TTL_S, show_spinner=False)
def _cached(_reader: GoldReader, root: str, method: str, *args: Any) -> Any:
    return getattr(_reader, method)(*args)


# --- Mise en page -----------------------------------------------------------


def _card_title(title: str, subtitle: str = "") -> None:
    sub = f'<div class="sp-card-sub">{escape(subtitle)}</div>' if subtitle else ""
    st.html(f'<div class="sp-card-title">{escape(title)}</div>{sub}')


def _header(last_observed: datetime | None) -> None:
    title_col, status_col = st.columns([5, 2], vertical_alignment="center")
    with title_col:
        st.title("STAR Pulse", anchor=False)
        st.html(
            '<p class="sp-subtitle">Ponctualité des bus et du métro de Rennes (réseau STAR), '
            "mesurée en continu à partir des flux temps réel GTFS-RT.</p>"
            f'<p class="sp-byline">Un projet de <a href="{AUTHOR_URL}" target="_blank">'
            f"{escape(AUTHOR)}</a>{SEP}"
            f'<a href="{REPO_URL}" target="_blank">code source</a></p>'
        )
    if last_observed is None:
        return
    text, fresh = freshness(last_observed, datetime.now(UTC))
    with status_col.container(horizontal_alignment="right"):
        st.badge(
            f"Mis à jour {text}",
            icon=":material/check_circle:" if fresh else ":material/schedule:",
            color="green" if fresh else "orange",
            help=(
                "Dernier passage observé le "
                f"{last_observed.astimezone(LOCAL_TZ):%d/%m/%Y à %H:%M} (heure de Paris)"
            ),
        )


def _sidebar(
    reader: GoldReader, first: date, last: date
) -> tuple[Filters, dict[str, str], list[str]]:
    routes: pd.DataFrame = _cached(reader, reader.root, "routes")
    labels = dict(zip(routes["route_id"], routes["route_short_name"], strict=True))
    with st.sidebar:
        st.subheader("Filtres", anchor=False)
        period = st.date_input(
            "Période",
            value=(max(first, last - timedelta(days=6)), last),
            min_value=first,
            max_value=last,
            format="DD/MM/YYYY",
            help="Jours de service (un trajet de 0 h 30 compte pour la veille).",
        )
        bounds = (period[0], period[-1]) if isinstance(period, tuple) and period else None
        start, end = bounds or (first, last)
        selected = st.pills(
            "Type de jour",
            DAY_TYPES,
            selection_mode="multi",
            default=list(DAY_TYPES),
            format_func=str.capitalize,
        )
        route_ids = st.multiselect(
            "Lignes",
            list(labels),
            format_func=lambda r: labels.get(r, r),
            placeholder="Toutes les lignes",
        )
        stops: pd.DataFrame = _cached(reader, reader.root, "stops", tuple(route_ids))
        groups = {
            stop_label(name, city): tuple(ids)
            for name, city, ids in zip(
                stops["stop_name"], stops["stop_city"], stops["stop_ids"], strict=True
            )
        }
        # Après un changement de lignes, on ne garde que les arrêts encore proposés.
        st.session_state[STOPS_KEY] = [
            name for name in st.session_state.get(STOPS_KEY, []) if name in groups
        ]
        stop_names: list[str] = st.multiselect(
            "Arrêts",
            list(groups),
            key=STOPS_KEY,
            placeholder="Tous les arrêts",
            help="Recherche par nom. Un clic sur un arrêt de la carte le sélectionne aussi.",
        )
        st.divider()
        st.caption(
            "**À l'heure** : entre 1 min d'avance et 3 min de retard sur l'horaire "
            "théorique. Arrivées au terminus exclues, arrêts supprimés comptés à part.\n\n"
            "**Sources** : GTFS et GTFS-RT du réseau STAR (open data Rennes Métropole).\n\n"
            f"Projet réalisé par [{AUTHOR}]({AUTHOR_URL}){SEP}[code source sur GitHub]({REPO_URL})"
        )
    day_types = tuple(selected) if isinstance(selected, list) and selected else DAY_TYPES
    stop_ids = tuple(sorted({stop_id for name in stop_names for stop_id in groups[name]}))
    return Filters(start, end, day_types, tuple(route_ids), stop_ids), labels, stop_names


def _selection_label(filters: Filters, labels: dict[str, str], stop_names: list[str]) -> str:
    parts = [f"Jours de service {period_label(filters.start, filters.end)}"]
    if filters.route_ids:
        names = ", ".join(labels.get(r, r) for r in filters.route_ids)
        parts.append(f"ligne{'s' if len(filters.route_ids) > 1 else ''} {names}")
    else:
        parts.append("toutes les lignes")
    if len(stop_names) == 1:
        parts.append(f"arrêt {stop_names[0]}")
    elif stop_names:
        parts.append(f"{len(stop_names)} arrêts")
    if set(filters.day_types) != set(DAY_TYPES):
        parts.append(", ".join(filters.day_types))
    return SEP.join(parts)


def _previous_filters(filters: Filters, first: date) -> Filters | None:
    """Même durée juste avant la période choisie, si elle est disponible."""
    end = filters.start - timedelta(days=1)
    start = end - (filters.end - filters.start)
    return replace(filters, start=start, end=end) if start >= first else None


# --- Indicateurs ------------------------------------------------------------


def _kpis(
    summary: dict[str, Any], previous: dict[str, Any] | None, daily: pd.DataFrame, days: int
) -> None:
    def trend(column: str, scale: float = 1.0) -> list[float] | None:
        if len(daily) < 2:
            return None
        return [round(float(v) * scale, 1) for v in daily[column].fillna(0)]

    def diff(key: str) -> float | None:
        if previous is None or pd.isna(previous[key]) or pd.isna(summary[key]):
            return None
        return float(summary[key]) - float(previous[key])

    compared = "vs la veille" if days == 1 else f"vs les {days} jours précédents"
    on_time, median, p90 = diff("pct_on_time"), diff("median_delay_s"), diff("p90_delay_s")
    cols = st.columns(4)
    cols[0].metric(
        "À l'heure",
        format_pct(summary["pct_on_time"]),
        delta=None if on_time is None else format_points(on_time),
        delta_description=compared if on_time is not None else None,
        help="Part des passages entre 1 min d'avance et 3 min de retard",
        border=True,
        chart_data=trend("pct_on_time", 100),
    )
    cols[1].metric(
        "Retard médian",
        format_delay(summary["median_delay_s"]),
        delta=None if median is None else format_delay(median),
        delta_color="inverse",
        delta_description=compared if median is not None else None,
        help="La moitié des passages ont un retard inférieur",
        border=True,
        chart_data=trend("median_delay_s"),
    )
    cols[2].metric(
        "Retard p90",
        format_delay(summary["p90_delay_s"]),
        delta=None if p90 is None else format_delay(p90),
        delta_color="inverse",
        delta_description=compared if p90 is not None else None,
        help="90 % des passages ont un retard inférieur",
        border=True,
        chart_data=trend("p90_delay_s"),
    )
    cols[3].metric(
        "Passages observés",
        format_count(summary["n_observed"]),
        help=(
            f"{format_count(summary['n_skipped'])} arrêts supprimés (non desservis), comptés à part"
        ),
        border=True,
        chart_data=trend("n_observed"),
        chart_type="bar",
    )


def split_bar_html(summary: dict[str, Any]) -> str:
    """Barre segmentée avance / à l'heure / retard, avec sa légende."""
    parts = [
        ("En avance", "plus d'1 min d'avance", summary["pct_early"], WARNING, INK),
        ("À l'heure", "de -1 à +3 min", summary["pct_on_time"], GOOD, "#ffffff"),
        ("En retard", "plus de 3 min de retard", summary["pct_late"], CRITICAL, "#ffffff"),
    ]
    blocks, keys, described = [], [], []
    for name, rule, value, fill, ink in parts:
        share = 0.0 if value is None or pd.isna(value) else float(value)
        described.append(f"{name.lower()} {format_pct(share)}")
        inside = share >= 0.06  # assez large pour afficher la valeur
        if share > 0:
            blocks.append(
                f'<div class="sp-seg" style="flex: {share:.4f} 1 0; background: {fill}; '
                f'color: {ink}" title="{name} : {format_pct(share)}">'
                f"{format_pct(share) if inside else ''}</div>"
            )
        value_in_legend = "" if inside else f" {format_pct(share)}"
        keys.append(
            f'<span><i class="sp-key" style="background: {fill}"></i>'
            f'{name}{value_in_legend} <span class="sp-muted">({rule})</span></span>'
        )
    label = escape("Répartition des passages : " + ", ".join(described))
    return (
        f'<div class="sp-split" role="img" aria-label="{label}">{"".join(blocks)}</div>'
        f'<div class="sp-legend">{"".join(keys)}</div>'
    )


# --- Graphiques -------------------------------------------------------------


def _style(chart: alt.LayerChart, height: int) -> alt.LayerChart:
    styled: alt.LayerChart = (
        chart.properties(height=height, background="transparent", padding={"top": 8, "left": 12})
        .configure(font=FONT)
        .configure_view(stroke=None)
        .configure_axis(
            labelColor=INK_MUTED,
            labelFontSize=12,
            titleColor=INK_SECONDARY,
            gridColor=GRID,
            gridWidth=1,
            domainColor=AXIS,
            tickColor=AXIS,
            labelPadding=6,
        )
    )
    return styled


def _tooltips(*extra: alt.Tooltip) -> list[alt.Tooltip]:
    return [
        *extra,
        alt.Tooltip("on_time:N", title="À l'heure"),
        alt.Tooltip("late:N", title="En retard"),
        alt.Tooltip("early:N", title="En avance"),
        alt.Tooltip("median:N", title="Retard médian"),
        alt.Tooltip("p90:N", title="Retard p90"),
        alt.Tooltip("n:N", title="Passages observés"),
    ]


def _labelled(df: pd.DataFrame) -> pd.DataFrame:
    """Ajoute les libellés français utilisés dans les infobulles."""
    return df.assign(
        on_time=df["pct_on_time"].map(format_pct),
        late=df["pct_late"].map(format_pct),
        early=df["pct_early"].map(format_pct),
        median=df["median_delay_s"].map(format_delay),
        p90=df["p90_delay_s"].map(format_delay),
        n=df["n_observed"].map(format_count),
    )


def _hour_chart(df: pd.DataFrame, average: float, height: int) -> alt.LayerChart:
    data = _labelled(df[df["n_observed"] >= MIN_HOUR_PASSAGES])
    data = data.assign(
        hour=data["scheduled_hour"].map(lambda h: f"{int(h)}h"),
        rank=data["scheduled_hour"].map(hour_rank),
    ).sort_values("rank")
    hours = list(data["hour"])
    many = len(hours) > 12  # une étiquette sur deux, barres proportionnelles
    width: Any = {"band": 0.72} if many else 20
    hover = alt.selection_point(on="pointerover", clear="pointerout", fields=["hour"], empty=False)
    bars = (
        alt.Chart(data)
        .mark_bar(width=width, cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X(
                "hour:O",
                sort=None,
                title=None,
                axis=alt.Axis(
                    labelAngle=0,
                    ticks=False,
                    labelPadding=8,
                    values=hours[::2] if many else hours,
                ),
            ),
            y=alt.Y(
                "pct_on_time:Q",
                title=None,
                scale=alt.Scale(domain=[0, 1]),
                axis=alt.Axis(labelExpr=PCT_LABEL, tickCount=5, domain=False, ticks=False),
            ),
            color=alt.when(hover).then(alt.value(ACCENT_HOVER)).otherwise(alt.value(ACCENT)),
            tooltip=_tooltips(alt.Tooltip("hour:N", title="Heure théorique")),
        )
        .add_params(hover)
    )
    rule = (
        alt.Chart(pd.DataFrame({"average": [average]}))
        .mark_rule(color=INK_SECONDARY, strokeWidth=1.5)
        .encode(y="average:Q")
    )
    return _style(alt.LayerChart(layer=[bars, rule]), height=height)


def _day_chart(df: pd.DataFrame, average: float) -> alt.LayerChart:
    data = _labelled(df).assign(day=df["service_date"].map(day_label))
    hover = alt.selection_point(on="pointerover", clear="pointerout", fields=["day"], empty=False)
    width: Any = 20 if len(data) <= 14 else {"band": 0.7}
    bars = (
        alt.Chart(data)
        .mark_bar(width=width, cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X(
                "day:O",
                sort=None,
                title=None,
                axis=alt.Axis(
                    labelAngle=0,
                    ticks=False,
                    labelPadding=8,
                    labelExpr="split(datum.label, ' ')",  # « sam. » / « 21/11 »
                    labelLineHeight=14,
                ),
            ),
            y=alt.Y(
                "pct_on_time:Q",
                title=None,
                scale=alt.Scale(domain=[0, 1]),
                axis=alt.Axis(labelExpr=PCT_LABEL, tickCount=5, domain=False, ticks=False),
            ),
            color=alt.when(hover).then(alt.value(ACCENT_HOVER)).otherwise(alt.value(ACCENT)),
            tooltip=_tooltips(alt.Tooltip("day:N", title="Jour de service")),
        )
        .add_params(hover)
    )
    rule = (
        alt.Chart(pd.DataFrame({"average": [average]}))
        .mark_rule(color=INK_SECONDARY, strokeWidth=1.5)
        .encode(y="average:Q")
    )
    return _style(alt.LayerChart(layer=[bars, rule]), height=260)


def _heatmap(df: pd.DataFrame) -> alt.LayerChart:
    """Grille jour x heure de la part des passages en retard."""
    data = _labelled(df[df["n_observed"] >= MIN_HOUR_PASSAGES])
    data = data.assign(
        day=data["service_date"].map(day_label),
        hour=data["scheduled_hour"].map(lambda h: f"{int(h)}h"),
        rank=data["scheduled_hour"].map(hour_rank),
    ).sort_values(["service_date", "rank"])
    hours = list(dict.fromkeys(data.sort_values("rank")["hour"]))
    days = list(dict.fromkeys(data["day"]))
    top = max(float(data["pct_late"].max()), 0.05) if not data.empty else 1.0
    cells = (
        alt.Chart(data)
        .mark_rect(cornerRadius=3, stroke="#ffffff", strokeWidth=2)
        .encode(
            x=alt.X(
                "hour:O",
                sort=hours,
                title=None,
                axis=alt.Axis(labelAngle=0, ticks=False, domain=False, grid=False),
            ),
            y=alt.Y(
                "day:O",
                sort=days,
                title=None,
                axis=alt.Axis(ticks=False, domain=False, grid=False, labelPadding=8),
            ),
            color=alt.Color(
                "pct_late:Q",
                title="En retard",
                scale=alt.Scale(domain=[0, top], range=SEQUENTIAL, nice=False),
                legend=alt.Legend(
                    orient="top",
                    direction="horizontal",
                    labelExpr=PCT_LABEL,
                    gradientLength=180,
                    gradientThickness=8,
                    titleColor=INK_SECONDARY,
                    labelColor=INK_MUTED,
                    tickCount=4,
                ),
            ),
            tooltip=_tooltips(
                alt.Tooltip("day:N", title="Jour de service"),
                alt.Tooltip("hour:N", title="Heure théorique"),
            ),
        )
    )
    return _style(alt.LayerChart(layer=[cells]), height=max(160, 34 * len(days)))


def _route_chart_height(n_routes: int) -> int:
    return max(240, 26 * min(n_routes, TOP_ROUTES))


def _route_chart(df: pd.DataFrame) -> alt.LayerChart:
    data = _labelled(df.head(TOP_ROUTES))
    data = data.assign(label=data["pct_on_time"].map(format_pct))
    hover = alt.selection_point(
        on="pointerover", clear="pointerout", fields=["route_id"], empty=False
    )
    y = alt.Y(
        "route_short_name:N",
        sort=None,
        title=None,
        axis=alt.Axis(
            ticks=False,
            domain=False,
            labelColor=INK,
            labelFontWeight=600,
            labelPadding=10,
            labelOverlap=False,
        ),
    )
    x = alt.X("pct_on_time:Q", title=None, axis=None, scale=alt.Scale(domain=[0, 1.12]))
    bars = (
        alt.Chart(data)
        .mark_bar(size=16, cornerRadiusEnd=4)
        .encode(
            x=x,
            y=y,
            color=alt.when(hover).then(alt.value(ACCENT_HOVER)).otherwise(alt.value(ACCENT)),
            tooltip=_tooltips(
                alt.Tooltip("route_short_name:N", title="Ligne"),
                alt.Tooltip("route_long_name:N", title="Trajet"),
            ),
        )
        .add_params(hover)
    )
    labels = (
        alt.Chart(data)
        .mark_text(align="left", baseline="middle", dx=6, fontSize=12, color=INK_SECONDARY)
        .encode(x=x, y=y, text="label:N")
    )
    return _style(alt.LayerChart(layer=[bars, labels]), height=_route_chart_height(len(data)))


# --- Carte ------------------------------------------------------------------


def _legend_html(min_passages: int) -> str:
    dots = "".join(
        f'<span><i class="sp-dot" style="background: {band.color}"></i>{band.label}</span>'
        for band in STOP_BANDS
    )
    return (
        f'<div class="sp-legend"><span class="sp-muted">À l\'heure :</span>{dots}'
        f'<span class="sp-muted">Taille du point : nombre de passages '
        f"(au moins {min_passages})</span></div>"
    )


def _stop_map(df: pd.DataFrame, selected: set[str]) -> pdk.Deck:
    max_n = max(float(df["n_observed"].max()), 1.0)
    bands = df["pct_on_time"].map(punctuality_band)
    labels = [stop_label(n, c) for n, c in zip(df["stop_name"], df["stop_city"], strict=True)]
    is_selected = pd.Series([label in selected for label in labels], index=df.index)
    data = (
        df.drop(columns=["stop_ids"])
        .assign(
            stop_label=labels,
            color=bands.map(lambda b: _rgba(b.color)),
            band=bands.map(lambda b: b.label),
            radius=df["n_observed"].map(lambda n: 3 + 6 * math.sqrt(n / max_n)) + is_selected * 3,
            line_color=is_selected.map(lambda sel: _rgba(INK if sel else "#ffffff", 255)),
            line_width=is_selected.map(lambda sel: 3 if sel else 1),
            selected=is_selected,
            pct_label=df["pct_on_time"].map(format_pct),
            p90_label=df["p90_delay_s"].map(format_delay),
            n_label=df["n_observed"].map(format_count),
        )
        # les arrêts en difficulté et l'arrêt choisi sont dessinés par-dessus
        .sort_values(["selected", "pct_on_time"], ascending=[True, False])
    )
    layer = pdk.Layer(
        "ScatterplotLayer",
        id=MAP_LAYER_ID,
        data=data,
        get_position="[stop_lon, stop_lat]",
        get_fill_color="color",
        get_radius="radius",
        radius_units="'pixels'",  # chaîne littérale : pydeck évalue les str
        stroked=True,
        get_line_color="line_color",
        get_line_width="line_width",
        line_width_units="'pixels'",
        pickable=True,
        auto_highlight=True,
        highlight_color=[11, 11, 11, 60],
    )
    focus = data[data["selected"]]
    if len(focus) == 1:  # un seul arrêt choisi : on zoome dessus
        view = pdk.ViewState(
            latitude=float(focus["stop_lat"].iloc[0]),
            longitude=float(focus["stop_lon"].iloc[0]),
            zoom=13.5,
        )
    else:
        view = pdk.ViewState(
            latitude=float(df["stop_lat"].median()),
            longitude=float(df["stop_lon"].median()),
            zoom=11.0,
        )
    tooltip = {
        "html": (
            "<b>{stop_name}</b><br/><span style='color:#52514e'>{stop_city}</span><br/>"
            "À l'heure : <b>{pct_label}</b><br/>Retard p90 : {p90_label}<br/>"
            "{n_label} passages observés<br/>"
            "<span style='color:#898781'>Clic : fiche de l'arrêt</span>"
        ),
        "style": {
            "backgroundColor": "#ffffff",
            "color": INK,
            "fontFamily": FONT,
            "fontSize": "13px",
            "lineHeight": "1.45",
            "padding": "8px 10px",
            "border": f"1px solid {GRID}",
            "borderRadius": "6px",
            "boxShadow": "0 2px 8px rgba(0, 0, 0, 0.08)",
        },
    }
    return pdk.Deck(
        layers=[layer],
        initial_view_state=view,
        map_provider="carto",
        map_style=pdk.map_styles.CARTO_LIGHT,
        tooltip=tooltip,
    )


def _on_map_select(map_key: str) -> None:
    """Un clic sur un arrêt de la carte remplace la sélection d'arrêts."""
    state = st.session_state.get(map_key) or {}
    objects = state.get("selection", {}).get("objects", {}).get(MAP_LAYER_ID, [])
    st.session_state[STOPS_KEY] = [objects[0]["stop_label"]] if objects else []


def _clear_stops() -> None:
    st.session_state[STOPS_KEY] = []
    # nouvelle clé de carte : efface aussi la sélection affichée sur la carte
    st.session_state["map_version"] = st.session_state.get("map_version", 0) + 1


def recent_passages_table(recent: pd.DataFrame, with_day: bool, with_stop: bool) -> pd.DataFrame:
    """Derniers passages mis en forme pour la fiche d'arrêt."""
    scheduled = pd.to_datetime(recent["scheduled_at"], utc=True).dt.tz_convert(LOCAL_TZ)
    table = pd.DataFrame(
        {
            "Prévu": scheduled.dt.strftime("%d/%m %H:%M" if with_day else "%H:%M"),
            "Ligne": recent["route_short_name"],
            "Retard": recent["delay_s"].map(format_delay),
        }
    )
    if with_stop:
        table.insert(1, "Arrêt", recent["stop_name"])
    return table


def _stop_panel(
    reader: GoldReader, filters: Filters, stop_names: list[str], summary: dict[str, Any]
) -> None:
    if not stop_names:
        st.html(
            '<div class="sp-hint"><b>Clique sur un arrêt</b> de la carte pour afficher sa '
            "fiche : ponctualité, lignes qui le desservent et derniers passages. "
            "Toute la page se filtre alors sur cet arrêt.</div>"
        )
        return
    title = stop_names[0] if len(stop_names) == 1 else f"{len(stop_names)} arrêts sélectionnés"
    lines = _cached(reader, reader.root, "by_route", filters, 1)
    chips = "".join(
        f'<span class="sp-chip">{escape(str(name))}</span>'
        for name in sorted(lines["route_short_name"].dropna(), key=lambda n: (len(n), n))
    )
    st.html(f'<div class="sp-card-title">{escape(title)}</div><div class="sp-chips">{chips}</div>')
    on_time_col, median_col = st.columns(2)
    on_time_col.metric("À l'heure à l'arrêt", format_pct(summary["pct_on_time"]))
    median_col.metric("Retard médian à l'arrêt", format_delay(summary["median_delay_s"]))
    recent = _cached(reader, reader.root, "recent_passages", filters, 12)
    st.caption("Derniers passages observés")
    st.dataframe(
        recent_passages_table(recent, filters.start != filters.end, len(stop_names) > 1),
        hide_index=True,
        height=300,
    )
    st.button(
        "Revenir à tout le réseau",
        icon=":material/close:",
        on_click=_clear_stops,
        width="stretch",
    )


def _route_table(df: pd.DataFrame) -> None:
    """Tableau de toutes les lignes, parts en % avec une décimale."""
    shares = ["pct_on_time", "pct_late", "pct_early"]
    pct = "%.1f %%"
    st.dataframe(
        df.assign(**{column: df[column] * 100 for column in shares}),
        hide_index=True,
        column_order=[
            "route_short_name",
            "route_long_name",
            "pct_on_time",
            "pct_late",
            "pct_early",
            "median_delay_s",
            "p90_delay_s",
            "n_observed",
        ],
        column_config={
            "route_short_name": st.column_config.TextColumn("Ligne", width="small"),
            "route_long_name": st.column_config.TextColumn("Trajet", width="medium"),
            "pct_on_time": st.column_config.ProgressColumn(
                "À l'heure", format=pct, min_value=0, max_value=100
            ),
            "pct_late": st.column_config.NumberColumn("En retard", format=pct),
            "pct_early": st.column_config.NumberColumn("En avance", format=pct),
            "median_delay_s": st.column_config.NumberColumn("Retard médian", format="%d s"),
            "p90_delay_s": st.column_config.NumberColumn("Retard p90", format="%d s"),
            "n_observed": st.column_config.NumberColumn("Passages", format="localized"),
        },
    )


# --- Page -------------------------------------------------------------------


def _history(reader: GoldReader, filters: Filters, daily: pd.DataFrame, average: float) -> None:
    with st.container(border=True):
        _card_title(
            "Évolution jour par jour",
            "À gauche, la part des passages à l'heure par jour de service (trait gris : "
            "moyenne). À droite, la part des passages en retard par jour et par heure.",
        )
        if len(daily) < 2:
            st.caption(
                "L'historique s'affiche dès que la période choisie contient au moins 2 jours "
                "de données. Il se construit tant que la collecte temps réel tourne "
                "(`make produce-rt` et `make consume-rt`)."
            )
            return
        left, right = st.columns([2, 3], gap="large")
        with left:
            st.altair_chart(_day_chart(daily, average), theme=None)
        with right:
            grid = _cached(reader, reader.root, "by_day_hour", filters)
            st.altair_chart(_heatmap(grid), theme=None)


def render(reader: GoldReader) -> None:
    """Construit la page à partir d'un lecteur gold."""
    st.html(CSS)
    bounds = _cached(reader, reader.root, "date_range")
    _header(_cached(reader, reader.root, "last_observed") if bounds else None)
    if bounds is None:
        st.info(EMPTY_MESSAGE, icon=":material/info:")
        return
    filters, labels, stop_names = _sidebar(reader, *bounds)
    st.caption(_selection_label(filters, labels, stop_names))

    summary = _cached(reader, reader.root, "summary", filters)
    if not summary["n_observed"]:
        st.warning("Aucun passage observé pour ces filtres.", icon=":material/filter_alt_off:")
        return
    average = float(summary["pct_on_time"])
    previous_filters = _previous_filters(filters, bounds[0])
    previous = (
        _cached(reader, reader.root, "summary", previous_filters) if previous_filters else None
    )
    daily = _cached(reader, reader.root, "by_day", filters)
    days = (filters.end - filters.start).days + 1
    _kpis(summary, previous if previous and previous["n_observed"] else None, daily, days)

    with st.container(border=True):
        _card_title("Répartition des passages observés")
        st.html(split_bar_html(summary))

    _history(reader, filters, daily, average)

    routes = _cached(reader, reader.root, "by_route", filters)
    left, right = st.columns(2, gap="medium")
    with left.container(border=True):
        _card_title(
            "Ponctualité selon l'heure",
            f"Part des passages à l'heure par heure théorique. Trait gris : moyenne "
            f"de la sélection ({format_pct(average)}).",
        )
        hours = _cached(reader, reader.root, "by_hour", filters)
        st.altair_chart(_hour_chart(hours, average, _route_chart_height(len(routes))), theme=None)
    with right.container(border=True):
        _card_title(
            "Lignes les moins ponctuelles",
            f"Part des passages à l'heure, {min(TOP_ROUTES, len(routes))} lignes "
            "les plus en difficulté (au moins 20 passages).",
        )
        if routes.empty:
            st.caption("Pas assez de passages par ligne sur cette sélection.")
        else:
            st.altair_chart(_route_chart(routes), theme=None)

    with st.container(border=True):
        title_col, slider_col = st.columns([3, 1], vertical_alignment="bottom")
        with title_col:
            _card_title(
                "Ponctualité par arrêt",
                "Chaque point est un arrêt, coloré selon sa part de passages à l'heure. "
                "Clique sur un arrêt pour filtrer la page.",
            )
        min_passages = slider_col.slider("Passages minimum par arrêt", 1, 100, 10)
        # la carte montre tout le réseau, l'arrêt choisi y est mis en évidence
        stops = _cached(reader, reader.root, "by_stop", replace(filters, stop_ids=()), min_passages)
        st.html(_legend_html(min_passages))
        map_col, panel_col = st.columns([3, 2], gap="medium")
        with map_col:
            if stops.empty:
                st.caption("Aucun arrêt avec assez de passages.")
            else:
                map_key = f"stop_map_{st.session_state.get('map_version', 0)}"
                st.pydeck_chart(
                    _stop_map(stops, set(stop_names)),
                    height=560,
                    key=map_key,
                    on_select=partial(_on_map_select, map_key),
                    selection_mode="single-object",
                )
        with panel_col:
            _stop_panel(reader, filters, stop_names, summary)

    with st.expander("Détail de toutes les lignes", icon=":material/table_rows:"):
        _route_table(routes)

    st.caption(
        f"STAR Pulse, un projet de {AUTHOR}. "
        "Pipeline : flux GTFS-RT, Redpanda, Parquet sur MinIO, modèles dbt + DuckDB, "
        "orchestration Dagster. Données : réseau STAR, open data Rennes Métropole."
    )


def main() -> None:
    """Point d'entrée de `streamlit run dashboard/app.py`."""
    st.set_page_config(
        page_title="STAR Pulse, ponctualité du réseau STAR",
        page_icon=":material/directions_bus:",
        layout="wide",
    )
    render(_reader())
