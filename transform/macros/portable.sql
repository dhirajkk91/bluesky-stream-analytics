{#- SQL that differs between Athena (Trino) and DuckDB, which the tests run on. -#}

{% macro utc_date(column) -%}
  {{ return(adapter.dispatch('utc_date')(column)) }}
{%- endmacro %}
{% macro athena__utc_date(column) %}date({{ column }}){% endmacro %}
{% macro default__utc_date(column) %}CAST(timezone('UTC', {{ column }}) AS DATE){% endmacro %}

{#- A literal UTC timestamp from 'yyyy-mm-dd'. -#}
{% macro utc_timestamp(day) -%}
  {{ return(adapter.dispatch('utc_timestamp')(day)) }}
{%- endmacro %}
{% macro athena__utc_timestamp(day) %}TIMESTAMP '{{ day }} 00:00:00 UTC'{% endmacro %}
{% macro default__utc_timestamp(day) %}TIMESTAMPTZ '{{ day }} 00:00:00+00'{% endmacro %}

{#- Accounts are keyed by a hash of their DID, so the marts don't carry it. -#}
{% macro account_key(did) -%}
  {{ return(adapter.dispatch('account_key')(did)) }}
{%- endmacro %}
{% macro athena__account_key(did) %}lower(to_hex(sha256(to_utf8({{ did }})))){% endmacro %}
{% macro default__account_key(did) %}sha256({{ did }}){% endmacro %}

{#- One row per day from `start` to `end`, both 'yyyy-mm-dd', end excluded. -#}
{% macro days_between(start, end) -%}
  {{ return(adapter.dispatch('days_between')(start, end)) }}
{%- endmacro %}
{% macro athena__days_between(start, end) -%}
  SELECT d AS date_day FROM UNNEST(sequence(DATE '{{ start }}', date_add('day', -1, DATE '{{ end }}'), INTERVAL '1' DAY)) AS t(d)
{%- endmacro %}
{% macro default__days_between(start, end) -%}
  SELECT CAST(range AS DATE) AS date_day FROM range(DATE '{{ start }}', DATE '{{ end }}', INTERVAL 1 DAY)
{%- endmacro %}

{% macro iso_day_of_week(column) -%}
  {{ return(adapter.dispatch('iso_day_of_week')(column)) }}
{%- endmacro %}
{% macro athena__iso_day_of_week(column) %}day_of_week({{ column }}){% endmacro %}
{% macro default__iso_day_of_week(column) %}isodow({{ column }}){% endmacro %}
