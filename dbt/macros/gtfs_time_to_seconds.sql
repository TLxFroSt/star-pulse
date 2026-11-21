{#
  Convertit une heure GTFS « HH:MM:SS » en secondes depuis le début du jour de
  service. Les heures peuvent dépasser 24 (ex. 25:30:00 = 1 h 30 le lendemain).
#}
{% macro gtfs_time_to_seconds(column) -%}
    case
        when nullif(trim({{ column }}), '') is null then null
        else cast(split_part(trim({{ column }}), ':', 1) as integer) * 3600
            + cast(split_part(trim({{ column }}), ':', 2) as integer) * 60
            + cast(split_part(trim({{ column }}), ':', 3) as integer)
    end
{%- endmacro %}
