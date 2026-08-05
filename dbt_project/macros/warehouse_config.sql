{#
    Physical-layout config that only some warehouses understand.

    BigQuery takes a dict for `partition_by`; DuckDB rejects it outright, so a
    hard-coded dict makes the project un-runnable locally. Routing the config
    through these helpers keeps one set of model files working against both
    targets, with the physical tuning applied only where it means something.
#}

{% macro partition_config(field, granularity='day', data_type='date') %}
    {% if target.type == 'bigquery' %}
        {{ return({'field': field, 'data_type': data_type, 'granularity': granularity}) }}
    {% else %}
        {{ return(none) }}
    {% endif %}
{% endmacro %}


{% macro cluster_config(columns) %}
    {% if target.type == 'bigquery' %}
        {{ return(columns) }}
    {% else %}
        {{ return(none) }}
    {% endif %}
{% endmacro %}
