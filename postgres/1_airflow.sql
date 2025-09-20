-- build airflow database and user
create user airflow with encrypted password 'airflow';
create database airflow;
grant all privileges on database airflow to airflow;

-- make foreign data wrapper to allow sagerx read access to airflow tables
create extension if not exists postgres_fdw;
create server airflow_fdw foreign data wrapper postgres_fdw options (host 'postgres', port '5432', dbname 'airflow');
create user mapping for sagerx server airflow_fdw options (user 'airflow', password 'airflow');
grant usage on foreign server airflow_fdw to sagerx;
