{{ config(materialized='external') }}

-- Table silver centrale : chaque passage temps réel rapproché de son horaire
-- théorique. Le jour de service n'est pas fourni par le flux STAR : on teste le
-- jour local de l'observation et la veille (courses après minuit, horaires
-- > 24 h), on ne garde que les services actifs ce jour-là et on retient le
-- candidat le plus proche de l'horaire théorique. Même règle pour un arrêt
-- desservi deux fois par la course (boucle).
--
-- Seuls les passages révolus sont gardés : heure observée (ou théorique pour
-- un arrêt supprimé) antérieure au dernier instantané reçu.
with passages as (
    select
        *,
        coalesce(observed_at, last_seen_at) as reference_at
    from {{ ref('int_rt__stop_passages') }}
),

candidates as (
    select
        p.*,
        cast(timezone('{{ var("gtfs_timezone") }}', p.reference_at) as date)
            - d.days_back as service_date
    from passages as p
    cross join (values (0), (1)) as d (days_back)
),

scheduled as (
    select
        c.*,
        v.gtfs_version,
        t.route_id as scheduled_route_id,
        t.direction_id as scheduled_direction_id,
        t.service_id,
        st.stop_sequence,
        st.timepoint,
        {{ gtfs_service_day_start('c.service_date') }} + to_seconds(
            case
                when c.event_type = 'departure'
                    then coalesce(st.departure_offset_s, st.arrival_offset_s)
                else coalesce(st.arrival_offset_s, st.departure_offset_s)
            end
        ) as scheduled_at
    from candidates as c
    inner join {{ ref('int_gtfs__versions') }} as v
        on c.service_date >= v.valid_from and c.service_date < v.valid_to
    inner join {{ ref('stg_gtfs__trips') }} as t
        on t.gtfs_version = v.gtfs_version and t.trip_id = c.trip_id
    inner join {{ ref('int_service_dates') }} as sd
        on sd.gtfs_version = v.gtfs_version
        and sd.service_id = t.service_id
        and sd.service_date = c.service_date
    inner join {{ ref('stg_gtfs__stop_times') }} as st
        on st.gtfs_version = v.gtfs_version
        and st.trip_id = c.trip_id
        and st.stop_id = c.stop_id
),

best_match as (
    select *
    from scheduled
    qualify row_number() over (
        partition by rt_passage_key
        order by abs(epoch(reference_at) - epoch(scheduled_at)), stop_sequence
    ) = 1
)

select
    md5(concat_ws('|', service_date, trip_id, stop_sequence)) as passage_id,
    service_date,
    gtfs_version,
    trip_id,
    scheduled_route_id as route_id,
    scheduled_direction_id as direction_id,
    service_id,
    stop_id,
    stop_sequence,
    timepoint = 1 as is_timepoint,
    passage_status,
    event_type,
    scheduled_at,
    observed_at,
    cast(epoch(observed_at) - epoch(scheduled_at) as integer) as delay_s,
    uncertainty_s,
    last_seen_at,
    -- Avance de la dernière prévision sur le passage : faible = mesure fiable.
    cast(epoch(observed_at) - epoch(last_seen_at) as integer) as prediction_lead_s,
    rt_passage_key
from best_match
where
    (passage_status = 'observed' and observed_at <= watermark_at)
    or (passage_status = 'skipped' and scheduled_at <= watermark_at)
