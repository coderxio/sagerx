--Build airflow database and user
CREATE USER airflow WITH ENCRYPTED PASSWORD 'airflow';
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

--Make foreign data wrapper to allow sagerx read access to airflow tables
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE SERVER airflow_fdw FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'postgres', port '5432', dbname 'airflow');
CREATE USER MAPPING FOR sagerx SERVER airflow_fdw OPTIONS (user 'airflow', password 'airflow');
GRANT USAGE ON FOREIGN SERVER airflow_fdw TO sagerx;