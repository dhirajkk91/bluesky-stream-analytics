{#-
  The days a model builds: [start, end), as 'yyyy-mm-dd'. A full build starts
  at the beginning of collection; an incremental one redoes the last few days,
  since an hour can be loaded late. `end` is today (UTC) unless set.
-#}
{% macro build_window() %}
  {% set end_var = var('end_date') %}
  {% if end_var %}
    {% set end = modules.datetime.date.fromisoformat(end_var) %}
  {% else %}
    {% set end = run_started_at.astimezone(modules.pytz.utc).date() %}
  {% endif %}
  {% if is_incremental() %}
    {% set start = end - modules.datetime.timedelta(days=var('lookback_days')) %}
  {% else %}
    {% set start = modules.datetime.date.fromisoformat(var('collection_start')) %}
  {% endif %}
  {% do return({'start': start.isoformat(), 'end': end.isoformat()}) %}
{% endmacro %}

{#- Clear the days about to be rebuilt, so a rebuild replaces them instead of adding to them. -#}
{% macro clear_window(column) %}
  {% if is_incremental() %}
    {% set w = build_window() %}
    DELETE FROM {{ this }} WHERE {{ column }} >= DATE '{{ w.start }}' AND {{ column }} < DATE '{{ w.end }}'
  {% endif %}
{% endmacro %}
