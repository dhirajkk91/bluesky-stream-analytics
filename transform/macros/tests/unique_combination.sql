{#- No two rows share the same values in these columns. -#}
{% test unique_combination(model, columns) %}
SELECT {{ columns | join(', ') }}, count(*) AS rows_with_these_values
FROM {{ model }}
GROUP BY {{ columns | join(', ') }}
HAVING count(*) > 1
{% endtest %}
