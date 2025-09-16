-- This macro avoids redundant schema prefixes for models that already have a custom schema
-- defined in dbt_profiles.yml. It ensures that models with a custom schema do not get
-- prefixed again, keeping schema names clean and consistent.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}
        
    {%- elif env_var('DBT_ENV_NAME', 'dev') == 'prod' -%}

        {{ custom_schema_name | trim }}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}
{%- endmacro %}
