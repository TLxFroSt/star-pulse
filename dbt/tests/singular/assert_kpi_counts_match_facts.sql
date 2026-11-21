-- Les tables KPI doivent compter exactement les passages de la table de faits
-- (hors arrivées au terminus), sans perte ni double compte.
with facts as (
    select count(*) as n from {{ ref('fct_stop_passages') }} where not is_terminus_arrival
),

kpis as (
    select 'route_day' as kpi, sum(n_passages) as n from {{ ref('fct_punctuality_route_day') }}
    union all
    select 'route_hour', sum(n_passages) from {{ ref('fct_punctuality_route_hour') }}
    union all
    select 'stop_day', sum(n_passages) from {{ ref('fct_punctuality_stop_day') }}
)

select kpis.kpi, kpis.n as kpi_passages, facts.n as fact_passages
from kpis
cross join facts
where kpis.n is distinct from facts.n
