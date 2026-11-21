{# Convertit une date GTFS « AAAAMMJJ » en DATE. #}
{% macro gtfs_date(column) -%}
    cast(strptime(nullif(trim({{ column }}), ''), '%Y%m%d') as date)
{%- endmacro %}
