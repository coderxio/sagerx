from pathlib import Path
import pendulum

from sagerx import get_dataset, read_sql_file, alert_slack_channel

from airflow.decorators import dag, task

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.subprocess import SubprocessHook


@dag(
    schedule="0 4 * * *",
    start_date=pendulum.today(),
    catchup=False,    
)
def fda_ndc():

    dag_id = "fda_ndc"
    url = "https://www.accessdata.fda.gov/cder/ndctext.zip"
    retrieve_dataset_function = get_dataset
    ds_folder = Path("/opt/airflow/dags") / dag_id
    data_folder = Path("/opt/airflow/data") / dag_id

    # Task to download data from web location
    @task
    def extract():
        PythonOperator(
            task_id=f"get_{dag_id}",
            python_callable=retrieve_dataset_function,
            op_kwargs={"ds_url": url, "data_folder": data_folder},
        )

    # Task to load data into source db schema
    @task
    def load_product():
        sql = "load-fda_ndc_product.sql"
        sql_path = ds_folder / sql
        PostgresOperator(
            task_id=sql,
            postgres_conn_id="postgres_default",
            sql=read_sql_file(sql_path),
        )

    # Task to load data into source db schema
    @task
    def load_package():
        sql = "load-fda_ndc_package.sql"
        sql_path = ds_folder / sql
        PostgresOperator(
            task_id=sql,
            postgres_conn_id="postgres_default",
            sql=read_sql_file(sql_path),
        )

    # Task to transform data using dbt
    @task
    def transform():
        subprocess = SubprocessHook()
        result = subprocess.run_command(['dbt', 'run'], cwd='/dbt/sagerx')        

    extract() >> load_product() >> load_package() >> transform()

fda_ndc()
