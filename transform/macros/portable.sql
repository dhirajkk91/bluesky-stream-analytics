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

{#- A SHA-256 of a text, as lowercase hex. NULL stays NULL. -#}
{% macro hash_text(text) -%}
  {{ return(adapter.dispatch('hash_text')(text)) }}
{%- endmacro %}
{% macro athena__hash_text(text) %}lower(to_hex(sha256(to_utf8({{ text }})))){% endmacro %}
{% macro default__hash_text(text) %}sha256({{ text }}){% endmacro %}

{#- Accounts are keyed by a hash of their DID, so the marts don't carry it. -#}
{% macro account_key(did) %}{{ hash_text(did) }}{% endmacro %}

{#- Posts are keyed by a hash of their at:// URI, the same whether the post itself or a reply to it was seen. -#}
{% macro post_key(uri) %}{{ hash_text(uri) }}{% endmacro %}

{#- The first language a post was tagged with. -#}
{% macro first_language(langs) -%}
  {{ return(adapter.dispatch('first_language')(langs)) }}
{%- endmacro %}
{% macro athena__first_language(langs) %}element_at({{ langs }}, 1){% endmacro %}
{% macro default__first_language(langs) %}{{ langs }}[1]{% endmacro %}

{#- Seconds from one timestamp to another. -#}
{% macro seconds_between(start, end) -%}
  {{ return(adapter.dispatch('seconds_between')(start, end)) }}
{%- endmacro %}
{% macro athena__seconds_between(start, end) %}(to_unixtime({{ end }}) - to_unixtime({{ start }})){% endmacro %}
{% macro default__seconds_between(start, end) %}(epoch({{ end }}) - epoch({{ start }})){% endmacro %}

{#-
  When a record was created, decoded from its key: a TID is 13 base32-sortable
  characters, and the first 11 are its creation time in microseconds. NULL for
  keys that aren't TIDs. The same decoding as the projections job's.
-#}
{% macro tid_time(rkey) -%}
  {{ return(adapter.dispatch('tid_time')(rkey)) }}
{%- endmacro %}
{% macro athena__tid_time(rkey) -%}
  CASE WHEN regexp_like({{ rkey }}, '^[2-7a-j][2-7a-z]{12}$') THEN from_unixtime(CAST(reduce(
      transform(regexp_extract_all(substr({{ rkey }}, 1, 11), '.'), c -> CAST(strpos('234567abcdefghijklmnopqrstuvwxyz', c) - 1 AS BIGINT)),
      CAST(0 AS BIGINT), (acc, d) -> acc * 32 + d, acc -> acc
  ) AS DOUBLE) / 1e6) END
{%- endmacro %}
{% macro default__tid_time(rkey) -%}
  CASE WHEN regexp_full_match({{ rkey }}, '[2-7a-j][2-7a-z]{12}') THEN make_timestamptz(list_reduce(
      list_transform(string_split({{ rkey }}[1:11], ''), lambda c: CAST(instr('234567abcdefghijklmnopqrstuvwxyz', c) - 1 AS BIGINT)),
      lambda acc, d: acc * 32 + d
  )) END
{%- endmacro %}

{#- One row per day from `start` to `end`, both 'yyyy-mm-dd', end excluded. -#}
{% macro days_between(start, end) -%}
  {{ return(adapter.dispatch('days_between')(start, end)) }}
{%- endmacro %}
{% macro athena__days_between(start, end) -%}
  SELECT CAST(d AS DATE) AS date_day FROM UNNEST(sequence(DATE '{{ start }}', date_add('day', -1, DATE '{{ end }}'), INTERVAL '1' DAY)) AS t(d)
{%- endmacro %}
{% macro default__days_between(start, end) -%}
  SELECT CAST(range AS DATE) AS date_day FROM range(DATE '{{ start }}', DATE '{{ end }}', INTERVAL 1 DAY)
{%- endmacro %}

{% macro iso_day_of_week(column) -%}
  {{ return(adapter.dispatch('iso_day_of_week')(column)) }}
{%- endmacro %}
{% macro athena__iso_day_of_week(column) %}day_of_week({{ column }}){% endmacro %}
{% macro default__iso_day_of_week(column) %}isodow({{ column }}){% endmacro %}
