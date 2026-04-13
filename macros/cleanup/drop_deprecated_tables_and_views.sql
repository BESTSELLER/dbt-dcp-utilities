{% macro drop_deprecated_tables_and_views(dry_run=true) %}
  
  {# 
  This macro will drop all tables and views from the database that are not referenced by dbt. 
  Note: objects belonging to schemas containing dbt sources will remain untouched by this operation.
  #}
  
  {{ log( 'Dropping unknown tables and views. dry_run: ' ~ dry_run , info=true) }}
  
  {% set all_tables_and_views = dbt_dcp_utilities.get_all_tables_and_views() %}
  {% set dbt_tables_and_views = dbt_dcp_utilities.get_dbt_nodes() %}
  {% set ns = namespace(existsInDbt=false, query='') %}
  {% set dbt_sources_schemas = dbt_dcp_utilities.get_dbt_sources_schemas() %}
  {% set excluded_schemas = dbt_dcp_utilities.get_exception_schemas() %}
  {% set excluded_objects = dbt_dcp_utilities.get_exception_tables_views() %}
  {% set model_counter = [] %}

  {{ log( 'Starting to drop non-dbt tables and views. dry_run: ' ~ dry_run , info=true) }}
  {% for relation in all_tables_and_views %}
    {% if relation.schema not in dbt_sources_schemas and relation.schema not in excluded_schemas %}

      {% set ns.existsInDbt = false %}
      {% for node in dbt_tables_and_views %}
        {% if (relation.schema.upper() == node.schema.upper() and relation.name.upper() == node.name.upper()) or ( relation.schema.upper() ~ '.' ~ relation.name.upper() in excluded_objects) %}
          {% set ns.existsInDbt = true %}
        {% endif %}
      {% endfor %}

      {# Only drop objects that do not exist in dbt  #}
      {% if not ns.existsInDbt %}
        
        {% if relation.type == 'view' %}
          {% set ns.query = 'DROP VIEW IF EXISTS ' + relation.schema + '.' + relation.name %}
        {% else %}
          {% set ns.query = 'DROP TABLE IF EXISTS ' + relation.schema + '.' + relation.name %}
        {% endif %}

        {{ log( ns.query , info=true) }}
        {% set __ = model_counter.append(1) %}

        {% if not dry_run %}
          {% do run_query(ns.query) %}
        {% endif %}

      {% endif %}
    {% endif %}
    
  {% endfor %}

  {# error on this macro during dry-run so we can set up an alert #}
  {% if model_counter|length > 0 and dry_run %}
  {{ exceptions.raise_compiler_error("The Cleanup Job has found " ~ model_counter|length ~ " deprecated table(s) or view(s) to be dropped  (Dry-Run).") }}
  {% endif %}

{% endmacro %}
