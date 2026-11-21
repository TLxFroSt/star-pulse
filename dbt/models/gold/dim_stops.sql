{{ config(location=gold_location('dim_stops')) }}

-- Arrêts du réseau, dans leur version GTFS la plus récente.
with stops as (
    select s.*, v.ingested_on
    from {{ ref('stg_gtfs__stops') }} as s
    inner join {{ ref('int_gtfs__versions') }} as v using (gtfs_version)
)

select
    stop_id,
    stop_code,
    stop_name,
    stop_city,
    stop_lat,
    stop_lon,
    location_type,
    parent_station,
    wheelchair_boarding,
    gtfs_version
from stops
qualify row_number() over (partition by stop_id order by ingested_on desc, gtfs_version desc) = 1
