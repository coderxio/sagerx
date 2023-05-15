from pathlib import Path
import pendulum

from sagerx import get_dataset, read_sql_file, get_sql_list, alert_slack_channel

from airflow.decorators import dag, task

from airflow.operators.python import get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.subprocess import SubprocessHook


@dag(
    schedule="0 4 * * *",
    start_date=pendulum.today(),
    catchup=False,
)
def meps():
    dag_id = "meps"
    ds_url = "https://meps.ahrq.gov/mepsweb/data_files/pufs/h220a/h220axlsx.zip"

    # Task to download data from web location
    @task
    def extract():
        data_folder = Path("/opt/airflow/data") / dag_id
        data_path = get_dataset(ds_url, data_folder)
        return data_path
    
    @task
    def load(data_path):
        import pandas as pd
        import sqlalchemy

        df = pd.read_excel(data_path + '/h220a.xlsx')
        df.columns = df.columns.str.lower()
        print(df.head(10))

        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = pg_hook.get_sqlalchemy_engine()

        df.to_sql(
            "meps",
            con=engine,
            schema="datasource",
            if_exists="replace"
        )

    load(extract())

meps()
