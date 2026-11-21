-- Période de validité de chaque version du GTFS théorique : une version
-- s'applique aux jours de service à partir de son ingestion et jusqu'à la
-- suivante. La plus ancienne couvre aussi tout ce qui précède.
with versions as (
    select * from {{ ref('stg_gtfs__feed_versions') }}
),

ordered as (
    select
        *,
        row_number() over (order by ingested_on, gtfs_version) as version_rank,
        lead(ingested_on) over (order by ingested_on, gtfs_version) as next_ingested_on
    from versions
)

select
    gtfs_version,
    feed_version,
    ingested_on,
    case when version_rank = 1 then date '1900-01-01' else ingested_on end as valid_from,
    coalesce(next_ingested_on, date '9999-12-31') as valid_to
from ordered
