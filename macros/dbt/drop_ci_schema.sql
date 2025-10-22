-- macros/drop_ci_schema.sql
{% macro drop_ci_schema(schema, database=None) %}
  {# require a schema #}
  {% if not schema %}
    {{ exceptions.raise_compiler_error("schema is required") }}
  {% endif %}

  {# pick database: explicit arg or default to target.database #}
  {% set db = (database if database else target.database) %}

  {# enforce CI naming rules #}
  {% if not schema.startswith('DBT_CI_PR_') %}
    {{ exceptions.raise_compiler_error("schema must start with DBT_CI_PR_") }}
  {% endif %}
  {% if not db.endswith('_TEST_DB') %}
    {{ exceptions.raise_compiler_error("database must end with _TEST_DB") }}
  {% endif %}

  {# find all schemas in db that start with the schema name #}
  {% set find_sql -%}
    select schema_name
    from {{ adapter.quote(db) }}.information_schema.schemata
    where schema_name ilike '{{ schema }}%'
  {%- endset %}

  {% set res = run_query(find_sql) %}

  {# loop and drop each matched schema #}
  {% for row in res.rows %}
    {% set sname = row[0] %}
    {% set drop_sql -%}
      drop schema if exists {{ adapter.quote(db) }}.{{ adapter.quote(sname) }} cascade
    {%- endset %}
    {{ log(drop_sql, info=true) }}
    {% do run_query(drop_sql) %}
  {% endfor %}

{% endmacro %}
