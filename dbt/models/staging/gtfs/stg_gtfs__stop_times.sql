-- Horaires théoriques, en secondes depuis le début du jour de service
-- (peuvent dépasser 86 400 pour les courses après minuit).
select
    version as gtfs_version,
    trip_id,
    cast(stop_sequence as integer) as stop_sequence,
    stop_id,
    {{ gtfs_time_to_seconds('arrival_time') }} as arrival_offset_s,
    {{ gtfs_time_to_seconds('departure_time') }} as departure_offset_s,
    try_cast(pickup_type as integer) as pickup_type,
    try_cast(drop_off_type as integer) as drop_off_type,
    try_cast(timepoint as integer) as timepoint
from {{ source('gtfs', 'stop_times') }}
