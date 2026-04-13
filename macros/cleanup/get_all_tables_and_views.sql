{% macro get_all_tables_and_views() %}

  {# 
  This macro returns a list of tables and views from the target databases as well as the schema name
  Snowflake's default metadata schema INFORMATION_SCHEMA is excluded.
  #}

  
  {{ log( 'get_all_tables_and_views: Loading views and tables from database' , info=true) }}
  {% set schema_results = run_query('SHOW SCHEMAS') %}
  {% set all_schemas = schema_results.columns[1].values() %}
  {% set all_tables_and_views = [] %}

  {% for schema in all_schemas %}
  {% if schema != "INFORMATION_SCHEMA" %}
    {% set view_results = run_query('SHOW VIEWS IN ' ~ schema) %}
    {% set views_in_schema = view_results.columns[1].values() %}
    {% for name in views_in_schema %}
      {% set view = {'schema': schema, 'name': name, 'type': 'view'} %}
      {{ log( view , info=true) }}
      {% do all_tables_and_views.append(view) %}
    {% endfor %}

    {% set table_results = run_query('SHOW TABLES IN ' ~ schema) %}
    {% set tables_in_schema = table_results.columns[1].values() %}
    {% for name in tables_in_schema %}
      {% set table = {'schema': schema, 'name': name, 'type': 'table'} %}
      {{ log( table , info=true) }}
      {% do all_tables_and_views.append(table) %}
    {% endfor %}
  {% endif %}
  {% endfor %}

  {{ return(all_tables_and_views) }}
{% endmacro %}
