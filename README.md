# Sagerx

## Development Quickstart
1. Clone the repo.
2. Add a `.env` file at the root of the repo.
3. Add the ENV var `AIRFLOW_UID=<uid>` to the .env file.
    - UID can be found by running `id -u` on linux systems, typically the first user on the system is `1001`
4. Run `docker-compose up airflow-init`.
5. Run `docker-compose up`

### server URLs

- Airflow UI is hosted on `'0.0.0.0:8001'`
- PgAdmin is hosted on `0.0.0.0:8002`
- WebAPI is hosted on `0.0.0.0:8000`
