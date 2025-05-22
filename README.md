This DBT package contains general purpose macros.

⚠️ IMPORTANT NOTE: This is a public repository, do not put any sensitive information here !!!

## Table of contents

* [How to use](#howto-use)
* [Kafka](#kafka)
  * [create_latest_view_on_kafka_tables](#create_latest_view_on_kafka_tables)
  * [get_kafka_table_latest_state](#get_kafka_table_latest_state)

## How to use
To use this package, put the following in the `packages.yml` file in your DBT project
```
packages:
  - git: https://github.com/BESTSELLER/dbt-dcp-utilities
    revision: 0.1.0
```
When you have done that, execute `dbt deps` in order to download the dependencies.
This package uses v1.3.0 of dbt-utils - if you run into dependency conflicts you can probably solve this by using the newest version of dbt-utils.

## Kafka

### create_latest_view_on_kafka_tables
Dynamically creates latest views for all Kafka-ingested tables in a given schema.
Each view selects the latest record per Kafka key based on the highest offset.
This macro is useful for building staging views that represent the most recent state
of each entity in a Kafka topic ingestion table.

To create these views automatically on each run/build, you can invoke this macro with
the `on_run_start` hook in dbt_project.yml.

Parameters:
  - `domain`: The domain (e.g., 'DCPA').
  - `env`: The environment to use (e.g., 'PROD', 'DEV', 'TEST').
  - `source_schema`: Schema where raw Kafka tables live.
  - `dest_schema`: Schema where views should be created.
  - `view_prefix`: Prefix to add to each view name.

###  get_kafka_table_latest_state
Calculates the latest state of a single Kafka table. The latest state is defined as
the highest offset for every key.

Parameters:
  - `input_table`: The table to calculate the latest state for.
