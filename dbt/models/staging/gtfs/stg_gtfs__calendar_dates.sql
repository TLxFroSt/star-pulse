-- Exceptions de calendrier : 1 = service ajouté, 2 = service supprimé.
select
    version as gtfs_version,
    service_id,
    {{ gtfs_date('date') }} as service_date,
    cast(exception_type as integer) as exception_type
from {{ source('gtfs', 'calendar_dates') }}
