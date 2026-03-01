-- make schemas for sagerx
create schema sagerx_dev;
create schema sagerx_lake;
create schema sagerx;

-- add pg_stat_statements extension for query monitoring
create extension if not exists pg_stat_statements;

create table sagerx.data_availability (
    schema_name text,
    table_name text,
    has_data boolean,
    materialized text
);
