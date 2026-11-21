{{ config(location=gold_location('dim_routes')) }}

-- Lignes du réseau, dans leur version GTFS la plus récente.
with routes as (
    select r.*, v.ingested_on
    from {{ ref('stg_gtfs__routes') }} as r
    inner join {{ ref('int_gtfs__versions') }} as v using (gtfs_version)
)

select
    route_id,
    route_short_name,
    route_long_name,
    route_type,
    '#' || coalesce(nullif(route_color, ''), '808080') as route_color,
    '#' || coalesce(nullif(route_text_color, ''), 'FFFFFF') as route_text_color,
    route_sort_order,
    gtfs_version
from routes
qualify row_number() over (partition by route_id order by ingested_on desc, gtfs_version desc) = 1
