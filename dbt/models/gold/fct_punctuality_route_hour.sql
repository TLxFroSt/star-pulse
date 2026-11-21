{{ config(location=gold_location('fct_punctuality_route_hour')) }}

-- KPI de ponctualité par ligne, jour de service et heure théorique locale
-- (arrivées au terminus exclues).
select
    route_id,
    service_date,
    scheduled_hour,
    any_value(service_day_type) as service_day_type,
    {{ punctuality_metrics() }}
from {{ ref('fct_stop_passages') }}
where not is_terminus_arrival
group by route_id, service_date, scheduled_hour
