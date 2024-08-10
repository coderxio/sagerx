from pathlib import Path
import pendulum

from sagerx import get_dataset, read_sql_file, get_sql_list, alert_slack_channel

from airflow.decorators import dag, task

from airflow.operators.python import get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

from common_dag_tasks import run_subprocess_command


@dag(
    schedule="0 0 10 * *",
    start_date=pendulum.datetime(2005, 1, 1),
    catchup=False,
)
def rxnorm():
    dag_id = "rxnorm"
    ds_url = "https://download.nlm.nih.gov/umls/kss/rxnorm/RxNorm_full_current.zip"

    @task
    def get_tgt():
        import requests
        from airflow.models import Variable

        api_key = Variable.get("umls_api")

        url = "https://utslogin.nlm.nih.gov/cas/v1/api-key"
        param = { "apikey": api_key }
        headers = { "Content-type": "application/x-www-form-urlencoded" }

        tgt_response = requests.post(url, headers=headers, data=param)

        first, second = tgt_response.text.split("api-key/")
        tgt, fourth = second.split('" method')

        return tgt

    @task
    def get_st(tgt: str):
        import requests

        url = f"https://utslogin.nlm.nih.gov/cas/v1/tickets/{tgt}"
        param = { "service": ds_url }
        headers = { "Content-type": "application/x-www-form-urlencoded" }

        st_response = requests.post(url, headers=headers, data=param)
        st = st_response.text

        return st

    # Task to download data from web location
    @task
    def extract(st: str):
        data_folder = Path("/opt/airflow/data") / dag_id
        data_path = get_dataset(f"{ds_url}?ticket={st}", data_folder)
        return data_path

    # Task to load data into source db schema
    load = []
    ds_folder = Path("/opt/airflow/dags") / dag_id
    for sql in get_sql_list("load", ds_folder):
        sql_path = ds_folder / sql
        task_id = sql[:-4]
        load.append(
            PostgresOperator(
                task_id=task_id,
                postgres_conn_id="postgres_default",
                sql=read_sql_file(sql_path),
            )
        )

    # Task to transform data using dbt
    @task
    def transform():
        run_subprocess_command(['docker', 'exec', 'dbt', 'dbt', 'run', '--select', 'models/staging/rxnorm', 'models/intermediate/rxnorm'], cwd='/dbt/sagerx')

    extract(get_st(get_tgt())) >> load >> transform()

rxnorm()
