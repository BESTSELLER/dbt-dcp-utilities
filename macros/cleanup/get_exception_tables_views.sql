{% macro get_exception_tables_views() %}
  
  {# 
  This macro returns a list of all tables and views that should get excluded by the cleanup macros by entry in the cleanup_exceptions.csv file. It makes no difference if an object is declared as view or table in the exceptions file.
  #}

{% set excluded_objects = [] %}

{% set sql_query %}
    select distinct upper(schema_name) || '.' || upper(object_name) as full_name from {{ ref('cleanup_exceptions') }} where lower(object_type) in ('view','table')
{% endset %}
{% set exclude_objects = dbt_utils.get_query_results_as_dict(sql_query) %}

{% if exclude_objects %}
  {% for object in exclude_objects['FULL_NAME'] %}
    {% set msg ='Found view/table in exceptions: ' ~ object %}
    {{ log( msg , info=true) }}
    {% do excluded_objects.append(object) %}
  {% endfor %}
{% endif %}

{{ return(excluded_objects) }}

{% endmacro %}