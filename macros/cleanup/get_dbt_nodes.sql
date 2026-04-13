{% macro get_dbt_nodes() %}

  {# 
  This macro parses the dbt model graph and returns all tables and views

  Note:
  Only models materialized as view, table or incremental and seeds are considered. 
  Incremental models and seeds are treated as tables. 
  #}

  {{ log( 'get_dbt_nodes: Loading views and tables from dbt graph' , info=true) }}  
  {% set dbt_tables_and_views = [] %}
  {% set table_materializations = ["table", "incremental", "seed"] %}
  {% set all_materializations = ["view", "table", "incremental", "seed", "snapshot"] %} 
  {% set table_resource_types = ["model", "seed", "snapshot"] %}


  {% for node in graph.nodes.values() %}
    {% if node.resource_type in table_resource_types and node.config.get('materialized', 'none') in all_materializations %}
            {# use this is you want the full json: {{ log( node , info=true) }} #}

            {# Set the schema to what we define in the generate_schema_macro #}
            {% set schema_name = generate_schema_name(node.config.get('schema'), node) %}

            {# obtaining version #}
            {% if node.version is number %}
                {% set node_version = ['_V', node.version]|join %}
            {% else %}
                {% set node_version = '' %}
            {% endif %}

            {# Set the table/view name as alias instead of node name if alias exists, account for versioning #}
            {% set node_name = node.config.get('alias') 
                if node.config.get('alias')  
                else [node.name, node_version]|join %}


            {# Set the node_type type as either table or view. #}
            {% if node.config.get('materialized', 'table') in table_materializations %}
                {% set node_type = "table" %}
            {% else %}
                {% set node_type = "view" %}
            {% endif %}

            {% set node = {'schema': schema_name, 'name': node_name, 'type': node_type} %}
            {{ log( node , info=true) }}
            {% do dbt_tables_and_views.append(node) %}
    {% endif %}
  {% endfor %}

  {{ return(dbt_tables_and_views) }}
{% endmacro %}