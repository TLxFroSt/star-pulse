select
    version as gtfs_version,
    trip_id,
    route_id,
    service_id,
    trip_headsign,
    try_cast(direction_id as integer) as direction_id,
    nullif(block_id, '') as block_id,
    nullif(shape_id, '') as shape_id,
    try_cast(wheelchair_accessible as integer) as wheelchair_accessible
from {{ source('gtfs', 'trips') }}
