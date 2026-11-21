select
    version as gtfs_version,
    stop_id,
    stop_code,
    stop_name,
    -- STAR met la commune dans stop_desc.
    stop_desc as stop_city,
    cast(stop_lat as double) as stop_lat,
    cast(stop_lon as double) as stop_lon,
    coalesce(try_cast(location_type as integer), 0) as location_type,
    nullif(parent_station, '') as parent_station,
    try_cast(wheelchair_boarding as integer) as wheelchair_boarding
from {{ source('gtfs', 'stops') }}
