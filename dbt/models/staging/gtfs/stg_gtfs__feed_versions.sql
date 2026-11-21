-- Une ligne par version du GTFS théorique ingérée en bronze.
select
    version as gtfs_version,
    min(cast(ingestion_date as date)) as ingested_on,
    any_value(feed_version) as feed_version,
    any_value({{ gtfs_date('feed_start_date') }}) as feed_start_date,
    any_value({{ gtfs_date('feed_end_date') }}) as feed_end_date
from {{ source('gtfs', 'feed_info') }}
group by version
