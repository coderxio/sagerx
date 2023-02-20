from pathlib import Path
import pendulum

from sagerx import get_dataset, read_sql_file, get_sql_list, alert_slack_channel

from airflow.decorators import dag, task

from airflow.operators.python import ShortCircuitOperator
from airflow.operators.python import get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.subprocess import SubprocessHook


@dag(
    schedule="0 0 * * 4",
    start_date=pendulum.datetime(2017, 1, 1),
    max_active_runs=1,
)
def fda_enforcement():
    dag_id = "fda_enforcement"

    # Task to download data from web location
    @task
    def extract_load(data_interval_start=None, data_interval_end=None):
        import requests
        import sqlalchemy
        import pandas as pd
        import pendulum
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

        df.to_sql(
            "fda_enforcement",
            con=engine,
            schema="datasource",
            if_exists="append",
            index_label="recall_number",
            dtype={"openfda": sqlalchemy.types.JSON},
        )

        df_rows = df.shape[0]

        return df_rows

    def df_has_data(**context) -> bool :
        df_rows = context['ti'].xcom_pull(task_ids='extract_load')
        if df_rows > 0:
            return True
        else:
            return False

    test_contains_data = ShortCircuitOperator(
        task_id = 'test_contains_data',
        python_callable = df_has_data
    )

    # Task to transform data using dbt
    @task
    def transform():
        subprocess = SubprocessHook()
        result = subprocess.run_command(['dbt', 'run'], cwd='/dbt/sagerx')
        print("Result from dbt:", result)

    extract_load() >> test_contains_data >> transform()

fda_enforcement()
