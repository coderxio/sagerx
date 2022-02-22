--Make schemas for sagerx
CREATE SCHEMA staging;
CREATE SCHEMA datasource;
CREATE SCHEMA flatfile;

--Add pg_stat_statements extension for query monitoring
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;