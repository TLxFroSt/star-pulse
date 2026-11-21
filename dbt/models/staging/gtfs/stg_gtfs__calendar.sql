select
    version as gtfs_version,
    service_id,
    monday = '1' as monday,
    tuesday = '1' as tuesday,
    wednesday = '1' as wednesday,
    thursday = '1' as thursday,
    friday = '1' as friday,
    saturday = '1' as saturday,
    sunday = '1' as sunday,
    {{ gtfs_date('start_date') }} as start_date,
    {{ gtfs_date('end_date') }} as end_date
from {{ source('gtfs', 'calendar') }}
