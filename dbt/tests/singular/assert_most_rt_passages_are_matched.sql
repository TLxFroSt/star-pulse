-- Alerte si plus de 5 % des passages révolus du temps réel ne trouvent pas
-- leur horaire théorique (GTFS périmé, identifiants qui changent…).
{{ config(severity='warn') }}

with past as (
    select rt_passage_key
    from {{ ref('int_rt__stop_passages') }}
    where passage_status = 'observed' and observed_at <= watermark_at
),

unmatched as (
    select rt_passage_key from past
    except
    select rt_passage_key from {{ ref('int_stop_passages_matched') }}
)

select
    (select count(*) from unmatched) as unmatched_passages,
    (select count(*) from past) as past_passages
where (select count(*) from unmatched) > 0.05 * (select count(*) from past)
