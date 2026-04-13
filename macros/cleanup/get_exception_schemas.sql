{% macro get_exception_schemas() %}
  
  {# 
  This macro returns a list of all schemas that should get excluded by the cleanup macros by entry in the cleanup_exceptions.csv file.
  #}

  {% set excluded_schemas = [] %}
  {% set exclude_schemas = dbt_utils.get_column_values(table=ref('cleanup_exceptions'), column='schema_name', where="object_type='schema'") %}
  {% if exclude_schemas %}
  {% for schema in exclude_schemas %}
    {% set msg ='Found schema in exceptions: ' ~ schema.upper() %}
    {{ log( msg , info=true) }}
    {% do excluded_schemas.append(schema.upper()) %}
  {% endfor %}
  {% endif %}

  {{ return(excluded_schemas) }}

{% endmacro %}