{#
  Vérifie qu'une combinaison de colonnes est unique (équivalent minimal de
  dbt_utils.unique_combination_of_columns, sans dépendance à un package).
#}
{% test unique_combination_of_columns(model, combination_of_columns) %}
select {{ combination_of_columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ combination_of_columns | join(', ') }}
having count(*) > 1
{% endtest %}
