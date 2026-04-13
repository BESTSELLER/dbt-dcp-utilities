{% macro drop_empty_schemas(dry_run=true) %}
  
  {# 
  This macro checks all current database schemas and drops those that do not 
  contain any views or tables 
  #}

  {{ log( 'drop_empty_schemas: Dropping all schemas from database that contain no table/views' , info=true) }}
  {% set schema_results = run_query('SHOW SCHEMAS') %}
  {% set all_schemas = schema_results.columns[1].values() %}
  {% set excluded_schemas = get_exception_schemas() %}
  {% set all_tables_and_views = [] %}
  {% set empty_schema_counter = [] %}

  {% for schema in all_schemas %}
    
    {% if schema != "INFORMATION_SCHEMA" and schema not in excluded_schemas%}
      {% set view_results = run_query('SHOW VIEWS IN ' ~ schema) %}
      {% set table_results = run_query('SHOW TABLES IN ' ~ schema) %}

      {# Only drop if schema is empty #}
      {% if view_results|length == 0 and table_results|length == 0 %}
        {% set query = 'DROP SCHEMA ' + schema %}
        {{ log( query , info=true) }}
        {% set __ = empty_schema_counter.append(1) %}

        {% if not dry_run %}
          {% do run_query(query) %}
        {% endif %}
      {% endif %}
    {% endif %}

  {% endfor %}

  {# error on this macro during dry-run so we can set up an alert #}
  {% if empty_schema_counter|length > 0 and dry_run %}
  {{ exceptions.raise_compiler_error("The Cleanup Job has found " ~ empty_schema_counter|length ~ " empty schema(s) to be dropped (Dry-Run).") }}
  {% endif %}

{% endmacro %}