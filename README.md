# SageRx

## Development Quickstart
1. Clone the repo.
2. Add a `.env` file at the root of the repo.
3. Add the ENV var `AIRFLOW_UID=<uid>` to the .env file.
    - UID can be found by running `id -u` on linux systems, typically the first user on the system is `1001`
4. Run `docker-compose up airflow-init`.
5. Run `docker-compose up`

If you get issues on folder permissions

`sudo chmod -R 777 postgres,data,extracts,logs,plugins`

### Using dbt
On docker-compose up a dbt container will be created to be used for cli commands. To enter commands run `docker exec -it dbt /bin/bash`. This will place you into a bash session in the dbt container. Then you can run dbt commands as you normally would.

### Server URLs

- Airflow UI is hosted on `'0.0.0.0:8001'`
- PgAdmin is hosted on `0.0.0.0:8002`
