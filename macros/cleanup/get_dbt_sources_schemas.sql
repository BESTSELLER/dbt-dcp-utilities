{% macro get_dbt_sources_schemas() %}
  
  {# 
  This macro returns a list of all database.schema names contained within the dbt model graph
  for all sources that exist in the target database.
  #}
  
  {% set dbt_sources_schemas = [] %}
  {% for node in graph.sources.values() %}
    {% if node.schema is not none and node.database.upper() == target.database.upper() %}
      {% if node.schema.upper() not in dbt_sources_schemas %}
        {% set msg ='Found source schema in target database: ' + node.database + '.' + node.schema %}
        {{ log( msg , info=true) }}
        {% do dbt_sources_schemas.append(node.schema.upper()) %}
      {% endif %}      
    {% endif %}
  {% endfor %}

  {{ return(dbt_sources_schemas) }}
{% endmacro %}