{#
    Portable title-casing.

    BigQuery ships INITCAP; DuckDB does not. Rather than let that difference
    leak into every model, dispatch on the adapter — this is the mechanism that
    keeps one SQL codebase running against both the local DuckDB target and the
    deployed BigQuery target.
#}

{% macro title_case(column) -%}
    {{ return(adapter.dispatch('title_case', 'lodestar')(column)) }}
{%- endmacro %}


{% macro default__title_case(column) -%}
    initcap(trim({{ column }}))
{%- endmacro %}


{% macro duckdb__title_case(column) -%}
    array_to_string(
        list_transform(
            string_split(lower(trim({{ column }})), ' '),
            s -> case when length(s) = 0 then s else upper(s[1]) || s[2:] end
        ),
        ' '
    )
{%- endmacro %}
