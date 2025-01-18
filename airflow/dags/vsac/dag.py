from pathlib import Path
import pendulum

from sagerx import get_dataset, read_sql_file, get_sql_list, alert_slack_channel

from airflow.decorators import dag, task

from airflow.operators.python import get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

from common_dag_tasks import run_subprocess_command, extract
from vsac.dag_tasks import main_execution



@dag(
    schedule="0 3 * * *",
    start_date=pendulum.yesterday(),
    catchup=False,
)
def vsac():
    dag_id = "vsac"
    base_url = "https://cts.nlm.nih.gov/fhir"
    ds_url = ""

    extract_load_task = main_execution()

    extract_load_task

vsac()
