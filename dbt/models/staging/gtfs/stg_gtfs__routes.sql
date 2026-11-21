select
    version as gtfs_version,
    route_id,
    agency_id,
    route_short_name,
    route_long_name,
    cast(route_type as integer) as route_type,
    route_color,
    route_text_color,
    try_cast(route_sort_order as integer) as route_sort_order
from {{ source('gtfs', 'routes') }}
