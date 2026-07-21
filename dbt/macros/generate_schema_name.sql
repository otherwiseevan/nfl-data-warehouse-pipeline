{#
    dbt's default generate_schema_name macro concatenates the custom
    schema (e.g. "staging") with the connection's default schema
    (e.g. "main"), producing "main_staging". That's dbt's standard
    behavior across every adapter, not a DuckDB quirk -- overriding
    it here so models land in clean schemas ("staging", "marts")
    instead of prefixed ones ("main_staging", "main_marts").
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}

        {{ target.schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
