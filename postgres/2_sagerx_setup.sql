--Make schemas for sagerx
CREATE SCHEMA sagerx_dev;
CREATE SCHEMA sagerx_lake;
CREATE SCHEMA sagerx;

--Add pg_stat_statements extension for query monitoring
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE TABLE sagerx.data_availability (
    schema_name text,
    table_name text,
    has_data boolean,
    materialized text
);