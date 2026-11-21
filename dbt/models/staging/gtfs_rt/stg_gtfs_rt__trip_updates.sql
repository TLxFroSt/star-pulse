-- Une ligne par (version d'une course, arrêt prévu). Le bronze est
-- « au moins une fois » : on dédoublonne sur (entity_key, entity_version).
with source as (
    select * from {{ source('gtfs_rt', 'trip_updates') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by entity_key, entity_version
        order by feed_timestamp, kafka_partition, kafka_offset
    ) = 1
),

parsed as (
    select
        entity_key,
        entity_version,
        cast(feed_timestamp as bigint) as feed_timestamp,
        cast(ingested_at as timestamptz) as ingested_at,
        entity_json ->> '$.trip_update.trip.trip_id' as trip_id,
        entity_json ->> '$.trip_update.trip.route_id' as route_id,
        try_cast(entity_json ->> '$.trip_update.trip.direction_id' as integer) as direction_id,
        coalesce(
            entity_json ->> '$.trip_update.trip.schedule_relationship', 'SCHEDULED'
        ) as trip_schedule_relationship,
        try_cast(entity_json ->> '$.trip_update.timestamp' as bigint) as trip_update_timestamp,
        from_json(
            entity_json -> '$.trip_update.stop_time_update',
            '[{"stop_id": "VARCHAR", "stop_sequence": "INTEGER",
               "schedule_relationship": "VARCHAR",
               "arrival": {"time": "VARCHAR", "delay": "INTEGER", "uncertainty": "INTEGER"},
               "departure": {"time": "VARCHAR", "delay": "INTEGER", "uncertainty": "INTEGER"}}]'
        ) as stop_time_updates
    from deduplicated
),

unnested as (
    select
        * exclude (stop_time_updates),
        unnest(stop_time_updates) as stu,
        generate_subscripts(stop_time_updates, 1) as update_index
    from parsed
)

select
    entity_key,
    entity_version,
    feed_timestamp,
    to_timestamp(feed_timestamp) as feed_at,
    to_timestamp(trip_update_timestamp) as trip_update_at,
    trip_id,
    route_id,
    direction_id,
    trip_schedule_relationship,
    update_index,
    stu.stop_id as stop_id,
    stu.stop_sequence as stop_sequence,
    coalesce(stu.schedule_relationship, 'SCHEDULED') as stop_schedule_relationship,
    to_timestamp(try_cast(stu.arrival.time as bigint)) as predicted_arrival_at,
    stu.arrival.uncertainty as arrival_uncertainty_s,
    to_timestamp(try_cast(stu.departure.time as bigint)) as predicted_departure_at,
    stu.departure.uncertainty as departure_uncertainty_s,
    ingested_at
from unnested
