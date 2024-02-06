-- get_custom_schema.sql
-- https://docs.getdbt.com/docs/build/custom-schemas

{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ generate_schema_name_for_env(custom_schema_name, node) }}
{%- endmacro %}
