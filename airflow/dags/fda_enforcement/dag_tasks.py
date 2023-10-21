from airflow.decorators import task

# Task to download data from web location
@task
def extract_load(data_interval_start=None, data_interval_end=None):
    import requests
    import sqlalchemy
    import pandas as pd
    import logging

    start_date = data_interval_start.format("YYYYMMDD")
    end_date = data_interval_end.format("YYYYMMDD")

    url = f"https://api.fda.gov/drug/enforcement.json?search=report_date:[{start_date}+TO+{end_date}]&limit=1000"
    logging.info(url)

    response = requests.get(url)

    json_object = response.json()["results"]

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg_hook.get_sqlalchemy_engine()

    df = pd.DataFrame(json_object)

    df.set_index("recall_number")

    num_rows = df.to_sql(
        "fda_enforcement",
        con=engine,
        schema="datasource",
        if_exists="append",
        dtype={"openfda": sqlalchemy.types.JSON},
    )

    return num_rows