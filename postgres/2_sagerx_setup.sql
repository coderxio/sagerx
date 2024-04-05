--Make schemas for sagerx
CREATE SCHEMA sagerx_dev;
CREATE SCHEMA sagerx_lake;
CREATE SCHEMA sagerx;

--Add pg_stat_statements extension for query monitoring
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;