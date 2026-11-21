-- Une ligne par position de véhicule, dédoublonnée sur (entity_key, entity_version).
with source as (
    select * from {{ source('gtfs_rt', 'vehicle_positions') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by entity_key, entity_version
        order by feed_timestamp, kafka_partition, kafka_offset
    ) = 1
)

select
    entity_key,
    entity_version,
    cast(feed_timestamp as bigint) as feed_timestamp,
    to_timestamp(cast(feed_timestamp as bigint)) as feed_at,
    entity_json ->> '$.vehicle.vehicle.id' as vehicle_id,
    entity_json ->> '$.vehicle.vehicle.label' as vehicle_label,
    entity_json ->> '$.vehicle.trip.trip_id' as trip_id,
    entity_json ->> '$.vehicle.trip.route_id' as route_id,
    try_cast(entity_json ->> '$.vehicle.trip.direction_id' as integer) as direction_id,
    try_cast(entity_json ->> '$.vehicle.position.latitude' as double) as latitude,
    try_cast(entity_json ->> '$.vehicle.position.longitude' as double) as longitude,
    try_cast(entity_json ->> '$.vehicle.position.bearing' as double) as bearing,
    try_cast(entity_json ->> '$.vehicle.position.speed' as double) as speed_m_s,
    entity_json ->> '$.vehicle.current_status' as current_status,
    entity_json ->> '$.vehicle.stop_id' as stop_id,
    try_cast(entity_json ->> '$.vehicle.current_stop_sequence' as integer) as current_stop_sequence,
    to_timestamp(try_cast(entity_json ->> '$.vehicle.timestamp' as bigint)) as position_at,
    cast(ingested_at as timestamptz) as ingested_at
from deduplicated
