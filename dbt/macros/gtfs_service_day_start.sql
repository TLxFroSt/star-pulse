{#
  Instant de référence d'un jour de service GTFS : « midi moins 12 h » en heure
  locale. Égal à minuit sauf les jours de changement d'heure, où c'est la
  convention de la spécification GTFS pour calculer les horaires.
#}
{% macro gtfs_service_day_start(service_date) -%}
    (timezone('{{ var("gtfs_timezone") }}', cast({{ service_date }} as timestamp) + interval 12 hour)
        - interval 12 hour)
{%- endmacro %}
