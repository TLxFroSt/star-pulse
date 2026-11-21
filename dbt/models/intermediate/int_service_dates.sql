-- Jours de circulation de chaque service : calendar.txt déplié jour par jour,
-- plus les ajouts (type 1) et moins les suppressions (type 2) de calendar_dates.txt.
with calendar as (
    select * from {{ ref('stg_gtfs__calendar') }}
),

calendar_days as (
    select
        gtfs_version,
        service_id,
        monday, tuesday, wednesday, thursday, friday, saturday, sunday,
        cast(unnest(generate_series(
            cast(start_date as timestamp), cast(end_date as timestamp), interval 1 day
        )) as date) as service_date
    from calendar
),

regular as (
    select gtfs_version, service_id, service_date
    from calendar_days
    where case isodow(service_date)
        when 1 then monday
        when 2 then tuesday
        when 3 then wednesday
        when 4 then thursday
        when 5 then friday
        when 6 then saturday
        when 7 then sunday
    end
),

exceptions as (
    select * from {{ ref('stg_gtfs__calendar_dates') }}
),

added as (
    select gtfs_version, service_id, service_date from exceptions where exception_type = 1
),

removed as (
    select gtfs_version, service_id, service_date from exceptions where exception_type = 2
),

active as (
    select * from regular
    union
    select * from added
)

select * from active
except
select * from removed
