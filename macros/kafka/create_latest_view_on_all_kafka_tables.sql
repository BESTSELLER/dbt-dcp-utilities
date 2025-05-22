{% macro create_latest_view_on_kafka_tables(
    domain,
    env='DEV',
    source_schema='INGEST_KAFKA',
    dest_schema=none,
    tbl_prefix='STG__')
%}
    {% set source_database = domain ~ '_RAW' ~ ('' if env == 'PROD' or env == '' else '_' ~ env) ~ '_DB'%}
    {% set tbls = dbt_utils.get_relations_by_prefix(schema=source_schema, prefix='', database=source_database) %}
    {% set schema_name = generate_schema_name(dest_schema) %}
    {{ log("Generating latest view for all tables in: " ~ source_database ~ '.' ~ source_schema ) }}
    {{ log("Creating latest views in the schema: " ~ schema_name) }}

    CREATE SCHEMA IF NOT EXISTS {{ schema_name }};
    {% for tbl in tbls %}
        CREATE OR REPLACE VIEW {{ schema_name }}.{{ view_prefix }}{{ tbl.identifier }}_latest
            COMMENT='Latest state of {{ tbl }}. This view provides the latest registered value for each Kafka key.\n\nThis view was created automatically by the "create_latest_view" macro which was executed automatically using the on-run-start hook.'
        AS
            {{ get_kafka_table_latest_state(tbl) }}
    {% endfor %}
{% endmacro %}
