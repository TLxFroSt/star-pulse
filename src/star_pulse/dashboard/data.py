"""Lecture des tables gold pour le dashboard, via DuckDB.

Les indicateurs sont recalculés depuis `fct_stop_passages` (grain passage) : quels
que soient les filtres, les pourcentages viennent des compteurs et les centiles sont
exacts, sans moyenne de pourcentages.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

import duckdb
import pandas as pd

from star_pulse.config import Settings

DAY_TYPES = ("semaine", "samedi", "dimanche")

# Indicateurs communs à toutes les agrégations (grain : passage).
_METRICS = """
    count(*) filter (where passage_status = 'observed') as n_observed,
    count(*) filter (where punctuality_class = 'skipped') as n_skipped,
    count(*) filter (where punctuality_class = 'on_time')
        / nullif(count(*) filter (where passage_status = 'observed'), 0) as pct_on_time,
    count(*) filter (where punctuality_class = 'late')
        / nullif(count(*) filter (where passage_status = 'observed'), 0) as pct_late,
    count(*) filter (where punctuality_class = 'early')
        / nullif(count(*) filter (where passage_status = 'observed'), 0) as pct_early,
    median(delay_s) as median_delay_s,
    quantile_cont(delay_s, 0.9) as p90_delay_s
"""


@dataclass(frozen=True)
class Filters:
    """Filtres de la page (hashables, pour le cache Streamlit)."""

    start: date
    end: date
    day_types: tuple[str, ...] = DAY_TYPES
    route_ids: tuple[str, ...] = field(default_factory=tuple)  # vide = toutes les lignes
    stop_ids: tuple[str, ...] = field(default_factory=tuple)  # vide = tous les arrêts


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class GoldReader:
    """Requêtes du dashboard sur les Parquet gold (MinIO ou dossier local)."""

    def __init__(self, con: duckdb.DuckDBPyConnection, root: str) -> None:
        self._con = con
        self._root = root.rstrip("/")

    @classmethod
    def from_settings(cls, settings: Settings) -> "GoldReader":
        """Connexion DuckDB en mémoire sur `s3://<bucket>/gold` (MinIO local)."""
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        endpoint = settings.s3_endpoint_url
        con.execute(
            f"""
            CREATE SECRET lake (
                TYPE s3,
                KEY_ID {_sql_literal(settings.s3_access_key.get_secret_value())},
                SECRET {_sql_literal(settings.s3_secret_key.get_secret_value())},
                REGION {_sql_literal(settings.s3_region)},
                ENDPOINT {_sql_literal(f"{endpoint.host}:{endpoint.port}")},
                USE_SSL {str(endpoint.scheme == "https").lower()},
                URL_STYLE 'path'
            )
            """
        )
        return cls(con, f"s3://{settings.s3_bucket}/gold")

    @property
    def root(self) -> str:
        """Dossier des Parquet gold (sert aussi de clé de cache)."""
        return self._root

    def _table(self, name: str) -> str:
        return f"read_parquet({_sql_literal(f'{self._root}/{name}.parquet')})"

    def _query(self, sql: str, params: list[Any] | None = None) -> pd.DataFrame:
        return self._con.execute(sql, params or []).df()

    @staticmethod
    def _where(filters: Filters) -> tuple[str, list[Any]]:
        clause = """
            service_date between ? and ?
            and list_contains(?, service_day_type)
            and (len(?) = 0 or list_contains(?, route_id))
            and (len(?) = 0 or list_contains(?, stop_id))
        """
        routes, stops = list(filters.route_ids), list(filters.stop_ids)
        return clause, [
            filters.start,
            filters.end,
            list(filters.day_types),
            routes,
            routes,
            stops,
            stops,
        ]

    # --- Référentiels -------------------------------------------------------

    def date_range(self) -> tuple[date, date] | None:
        """Premier et dernier jour de service disponibles, ou None si le gold est vide."""
        try:
            table = self._table("fct_stop_passages")
            row = self._con.execute(
                f"select min(service_date), max(service_date) from {table}"
            ).fetchone()
        except duckdb.Error:
            return None
        if row is None or row[0] is None:
            return None
        return row[0], row[1]

    def routes(self) -> pd.DataFrame:
        """Lignes (identifiant, nom court, couleur), triées comme sur le réseau."""
        return self._query(
            f"""
            select route_id, route_short_name, route_long_name, route_color
            from {self._table("dim_routes")}
            order by route_sort_order nulls last, route_short_name
            """
        )

    def stops(self, route_ids: tuple[str, ...] = ()) -> pd.DataFrame:
        """Arrêts regroupés par nom et commune (les deux sens d'un même arrêt
        ont des identifiants différents), limités aux lignes données."""
        routes = list(route_ids)
        return self._query(
            f"""
            select s.stop_name, s.stop_city, list(distinct s.stop_id order by s.stop_id) as stop_ids
            from {self._table("dim_stops")} as s
            where s.stop_id in (
                select stop_id from {self._table("fct_stop_passages")}
                where len(?) = 0 or list_contains(?, route_id)
            )
            group by all
            order by s.stop_name, s.stop_city
            """,
            [routes, routes],
        )

    def last_observed(self) -> datetime | None:
        """Horodatage du dernier passage observé (fraîcheur des données)."""
        try:
            row = self._con.execute(
                f"select max(observed_at) from {self._table('fct_stop_passages')}"
            ).fetchone()
        except duckdb.Error:
            return None
        value: datetime | None = row[0] if row else None
        return value

    # --- Indicateurs --------------------------------------------------------

    def _passages(self, filters: Filters) -> tuple[str, list[Any]]:
        where, params = self._where(filters)
        sql = f"""
            select * from {self._table("fct_stop_passages")}
            where not is_terminus_arrival and {where}
        """
        return sql, params

    def summary(self, filters: Filters) -> dict[str, Any]:
        """KPI globaux de la sélection."""
        passages, params = self._passages(filters)
        df = self._query(f"select {_METRICS} from ({passages})", params)
        return {str(k): v for k, v in df.iloc[0].to_dict().items()}

    def by_day(self, filters: Filters) -> pd.DataFrame:
        """KPI par jour de service (tendances des tuiles)."""
        passages, params = self._passages(filters)
        return self._query(
            f"""
            select service_date, {_METRICS}
            from ({passages})
            group by service_date
            order by service_date
            """,
            params,
        )

    def by_day_hour(self, filters: Filters) -> pd.DataFrame:
        """KPI par jour de service et heure théorique (grille de la semaine)."""
        passages, params = self._passages(filters)
        return self._query(
            f"""
            select service_date, scheduled_hour, {_METRICS}
            from ({passages})
            group by service_date, scheduled_hour
            order by service_date, scheduled_hour
            """,
            params,
        )

    def by_hour(self, filters: Filters) -> pd.DataFrame:
        """Ponctualité par heure théorique locale."""
        passages, params = self._passages(filters)
        return self._query(
            f"""
            select scheduled_hour, {_METRICS}
            from ({passages})
            group by scheduled_hour
            order by scheduled_hour
            """,
            params,
        )

    def by_route(self, filters: Filters, min_observed: int = 20) -> pd.DataFrame:
        """Ponctualité par ligne, de la moins à la plus ponctuelle."""
        passages, params = self._passages(filters)
        return self._query(
            f"""
            select
                p.route_id,
                r.route_short_name,
                r.route_long_name,
                r.route_color,
                {_METRICS}
            from ({passages}) as p
            left join {self._table("dim_routes")} as r using (route_id)
            group by all
            having count(*) filter (where passage_status = 'observed') >= ?
            order by pct_on_time, p90_delay_s desc
            """,
            [*params, min_observed],
        )

    def by_stop(self, filters: Filters, min_observed: int = 10) -> pd.DataFrame:
        """Ponctualité par arrêt (regroupé par nom et commune), pour la carte."""
        passages, params = self._passages(filters)
        return self._query(
            f"""
            select
                s.stop_name,
                s.stop_city,
                list(distinct p.stop_id order by p.stop_id) as stop_ids,
                avg(s.stop_lat) as stop_lat,
                avg(s.stop_lon) as stop_lon,
                {_METRICS}
            from ({passages}) as p
            inner join {self._table("dim_stops")} as s using (stop_id)
            group by s.stop_name, s.stop_city
            having count(*) filter (where passage_status = 'observed') >= ?
            """,
            [*params, min_observed],
        )

    def recent_passages(self, filters: Filters, limit: int = 20) -> pd.DataFrame:
        """Derniers passages observés de la sélection, du plus récent au plus ancien."""
        passages, params = self._passages(filters)
        return self._query(
            f"""
            select
                p.scheduled_at,
                p.observed_at,
                p.delay_s,
                p.punctuality_class,
                r.route_short_name,
                s.stop_name
            from ({passages}) as p
            left join {self._table("dim_routes")} as r using (route_id)
            left join {self._table("dim_stops")} as s using (stop_id)
            where p.passage_status = 'observed'
            order by p.scheduled_at desc
            limit ?
            """,
            [*params, limit],
        )
