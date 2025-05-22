{% macro get_kafka_table_latest_state(
    input_table
)
%}
    SELECT * EXCLUDE row_num FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY RECORD_METADATA:key ORDER BY RECORD_METADATA:offset DESC) AS row_num
        FROM {{ input_table }}
    )
    WHERE row_num = 1;
{% endmacro %}
