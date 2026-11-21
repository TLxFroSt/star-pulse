{{ config(location=gold_location('fct_stop_passages')) }}

-- Un passage révolu par ligne, classé selon sa ponctualité. L'heure et le jour
-- sont ceux de l'horaire théorique, en heure locale.
with passages as (
    select * from {{ ref('int_stop_passages_matched') }}
),

trip_ends as (
    select gtfs_version, trip_id, max(stop_sequence) as last_stop_sequence
    from {{ ref('stg_gtfs__stop_times') }}
    group by gtfs_version, trip_id
),

enriched as (
    select
        p.*,
        timezone('{{ var("gtfs_timezone") }}', p.scheduled_at) as scheduled_local,
        p.stop_sequence = te.last_stop_sequence as is_terminus_arrival
    from passages as p
    inner join trip_ends as te using (gtfs_version, trip_id)
)

select
    passage_id,
    service_date,
    isodow(service_date) as service_day_of_week,
    case isodow(service_date)
        when 6 then 'samedi'
        when 7 then 'dimanche'
        else 'semaine'
    end as service_day_type,
    hour(scheduled_local) as scheduled_hour,
    route_id,
    direction_id,
    trip_id,
    stop_id,
    stop_sequence,
    is_terminus_arrival,
    passage_status,
    case
        when passage_status = 'skipped' then 'skipped'
        when delay_s < {{ var('on_time_min_delay_s') }} then 'early'
        when delay_s > {{ var('on_time_max_delay_s') }} then 'late'
        else 'on_time'
    end as punctuality_class,
    delay_s,
    scheduled_at,
    observed_at,
    prediction_lead_s,
    gtfs_version
from enriched
