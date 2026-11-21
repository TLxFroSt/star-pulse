{{ config(location=gold_location('fct_punctuality_stop_day')) }}

-- KPI de ponctualité par arrêt et par jour de service, pour la carte
-- (arrivées au terminus exclues).
select
    stop_id,
    service_date,
    any_value(service_day_type) as service_day_type,
    count(distinct route_id) as n_routes,
    {{ punctuality_metrics() }}
from {{ ref('fct_stop_passages') }}
where not is_terminus_arrival
group by stop_id, service_date
