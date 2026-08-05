{#
    Schema routing.

    prod  -> bare layer names: `staging`, `intermediate`, `marts`
    other -> prefixed with the target schema: `ci_pr_142_marts`, `lodestar_marts`

    The prefix is what lets every pull request build the full graph into its own
    isolated namespace, and lets that namespace be dropped in one statement when
    the PR closes.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}

    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}

    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
