{% macro check_data_availability() %}
{% set sources_and_models = [] -%}

{% for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") %}
        {%- do sources_and_models.append([node.schema, node.name, node.config.materialized]) -%}
{%- endfor %}

{% for node in graph.sources.values() -%}
  {%- do sources_and_models.append([node.schema, node.name, node.resource_type]) -%}
{%- endfor %}

--{{ log("sources and models: " ~ sources_and_models, info=True) }}

{% set results_table_query %}
drop table if exists sagerx.data_availability CASCADE;
create table sagerx.data_availability(
    schema_name text,
    table_name text,
    has_data boolean,
    materialized text,
    columns_info jsonb
);
{% endset %}

{{ run_query(results_table_query) }}


{% for schema_name, table_name, mat_config in sources_and_models %}
    {% set check_table_exists_query %}
      {{ log("Check table exists query for " ~ schema_name ~ " : " ~ table_name, info=True) }}
              select exists (
                  select 1 
                  from information_schema.tables 
                  where table_schema = '{{ schema_name }}' 
                  and table_name = '{{ table_name }}'
              ) as table_exists
      {% endset %}

    {% set table_exists_result = run_query(check_table_exists_query) %}
    {% if table_exists_result[0]['table_exists'] %}
      {{ log("Table: " ~ table_name ~ " does exist.", info=True) }}

      {% set columns_query %}
        select 
            column_name, 
            data_type, 
            is_nullable
        from 
            information_schema.columns 
        where 
            table_schema = '{{ schema_name }}' 
            and table_name = '{{ table_name }}'
      {% endset %}
      
      {% set columns_result = run_query(columns_query) %}
      {% set columns_info = [] %}
      {% for column in columns_result %}
          {%- do columns_info.append({"column_name": column['column_name'], "data_type": column['data_type'], "is_nullable": column['is_nullable']}) -%}
      {% endfor %}

      {% set row_count_query %}
          select count(*) as row_count from {{schema_name}}.{{table_name}}
      {% endset %}
      {% set row_count_result = run_query(row_count_query) %}

      {% set insert_query %}
          insert into sagerx.data_availability
          (schema_name, table_name, has_data, materialized, columns_info)
          values ('{{ schema_name }}','{{ table_name }}', {{ row_count_result[0]['row_count'] > 0 }}, '{{ mat_config }}', '{{ columns_info | tojson }}');
      {% endset %}
      {{ run_query(insert_query) }}
    {% else %}
        {{ log("No Table: " ~ table_name ~ " does not exist.", info=True) }}
    {% endif %}
    
{% endfor %}
{% endmacro %}
