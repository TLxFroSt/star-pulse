{# Emplacement Parquet d'un modèle gold : s3://<bucket>/gold/<nom>.parquet #}
{% macro gold_location(model_name) -%}
    s3://{{ env_var('STAR_PULSE_S3_BUCKET', 'lake') }}/gold/{{ model_name }}.parquet
{%- endmacro %}
