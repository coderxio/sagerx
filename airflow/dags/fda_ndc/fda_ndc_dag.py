from pathlib import Path
import pendulum

from sagerx import get_dataset, read_sql_file, alert_slack_channel

from airflow.decorators import dag, task

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.subprocess import SubprocessHook


@dag(
    schedule="0 4 * * *",
    start_date=pendulum.today(),
    catchup=False,    
)
def fda_ndc():

    dag_id = "fda_ndc"
    ds_url = "https://www.accessdata.fda.gov/cder/ndctext.zip"
    ds_folder = Path("/opt/airflow/dags") / dag_id
    data_folder = Path("/opt/airflow/data") / dag_id

    # Task to download data from web location
    @task
    def extract():
        data_path = get_dataset(ds_url, data_folder)
        return data_path

    # Task to load data into source db schema
    @task
    def load_product():
        sql = "load-fda_ndc_product.sql"
        sql_path = ds_folder / sql
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        pg_hook.run(read_sql_file(sql_path))

    # Task to load data into source db schema
    @task
    def load_package():
        sql = "load-fda_ndc_package.sql"
        sql_path = ds_folder / sql
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        pg_hook.run(read_sql_file(sql_path))

    # Task to transform data using dbt
    @task
    def transform():
        subprocess = SubprocessHook()
        result = subprocess.run_command(['dbt', 'run'], cwd='/dbt/sagerx') 

    extract() >> load_product() >> load_package() >> transform()

fda_ndc()
