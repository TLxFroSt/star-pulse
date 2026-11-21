{#
  Indicateurs de ponctualité communs aux tables KPI. Les arrêts supprimés sont
  comptés à part et exclus du dénominateur des pourcentages.
#}
{% macro punctuality_metrics() -%}
    count(*) as n_passages,
    count(*) filter (where passage_status = 'observed') as n_observed,
    count(*) filter (where punctuality_class = 'on_time') as n_on_time,
    count(*) filter (where punctuality_class = 'early') as n_early,
    count(*) filter (where punctuality_class = 'late') as n_late,
    count(*) filter (where punctuality_class = 'skipped') as n_skipped,
    count(*) filter (where punctuality_class = 'on_time')
        / nullif(count(*) filter (where passage_status = 'observed'), 0) as pct_on_time,
    count(*) filter (where punctuality_class = 'early')
        / nullif(count(*) filter (where passage_status = 'observed'), 0) as pct_early,
    count(*) filter (where punctuality_class = 'late')
        / nullif(count(*) filter (where passage_status = 'observed'), 0) as pct_late,
    round(avg(delay_s), 1) as avg_delay_s,
    median(delay_s) as median_delay_s,
    quantile_cont(delay_s, 0.9) as p90_delay_s
{%- endmacro %}
