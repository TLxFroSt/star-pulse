-- Dernière prévision connue pour chaque passage (course, arrêt) vu dans le
-- temps réel. Le flux ne liste que les arrêts à venir : la dernière prévision
-- émise avant que l'arrêt disparaisse est la meilleure estimation du passage.
--
-- Un même trip_id peut circuler plusieurs jours : les instantanés d'une course
-- séparés de plus de `rt_run_gap_hours` forment des « runs » distincts.
--
-- Le flux STAR contient deux sortes de prévisions inexploitables, écartées :
-- - courses figées : mise à jour vieille de plusieurs heures, prévisions
--   fausses d'un jour ;
-- - prévisions « plates » : tous les arrêts d'une course à la même heure
--   (vu sur toutes les courses à venir d'un même service de véhicule).
with fresh_updates as (
    select * from {{ ref('stg_gtfs_rt__trip_updates') }}
    where stop_id is not null
        and trip_update_at >= feed_at - interval {{ var('rt_max_update_age_minutes') }} minute
),

flat_versions as (
    select entity_key, entity_version
    from fresh_updates
    where coalesce(predicted_arrival_at, predicted_departure_at) is not null
    group by entity_key, entity_version
    having count(*) >= 3
        and count(distinct coalesce(predicted_arrival_at, predicted_departure_at)) = 1
),

updates as (
    select *
    from fresh_updates
    where (entity_key, entity_version) not in (select (entity_key, entity_version) from flat_versions)
),

watermark as (
    select max(feed_at) as watermark_at from updates
),

trip_snapshots as (
    select distinct trip_id, feed_timestamp from updates
),

flagged as (
    select
        trip_id,
        feed_timestamp,
        case
            when feed_timestamp
                - lag(feed_timestamp) over (partition by trip_id order by feed_timestamp)
                > {{ var('rt_run_gap_hours') }} * 3600
            then 1 else 0
        end as is_new_run
    from trip_snapshots
),

runs as (
    select
        trip_id,
        feed_timestamp,
        sum(is_new_run) over (
            partition by trip_id order by feed_timestamp rows unbounded preceding
        ) as run_number
    from flagged
),

run_starts as (
    select trip_id, run_number, min(feed_timestamp) as run_first_feed_timestamp
    from runs
    group by trip_id, run_number
),

latest as (
    select
        u.*,
        rs.run_first_feed_timestamp
    from updates as u
    inner join runs as r using (trip_id, feed_timestamp)
    inner join run_starts as rs using (trip_id, run_number)
    qualify row_number() over (
        partition by u.trip_id, r.run_number, u.stop_id
        order by u.feed_timestamp desc, u.update_index desc
    ) = 1
)

select
    md5(concat_ws('|', trip_id, run_first_feed_timestamp, stop_id)) as rt_passage_key,
    trip_id,
    route_id,
    direction_id,
    stop_id,
    case
        when stop_schedule_relationship = 'SKIPPED' then 'skipped'
        else 'observed'
    end as passage_status,
    case
        when stop_schedule_relationship = 'SKIPPED' then null
        when predicted_arrival_at is not null then 'arrival'
        else 'departure'
    end as event_type,
    case
        when stop_schedule_relationship = 'SKIPPED' then null
        else coalesce(predicted_arrival_at, predicted_departure_at)
    end as observed_at,
    case
        when stop_schedule_relationship = 'SKIPPED' then null
        else coalesce(arrival_uncertainty_s, departure_uncertainty_s)
    end as uncertainty_s,
    feed_at as last_seen_at,
    watermark_at
from latest
cross join watermark
where stop_schedule_relationship = 'SKIPPED'
    or coalesce(predicted_arrival_at, predicted_departure_at) is not null
