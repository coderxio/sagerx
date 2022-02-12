from datetime import date, timedelta
from textwrap import dedent
from pathlib import Path
import calendar

from sagerx import read_sql_file, get_sql_list, alert_slack_channel

ds = {"dag_id": "flat_files", "schedule_interval": "0 0 1 1 *"}  # yearly

########################### DYNAMIC DAG DO NOT TOUCH BELOW HERE #################################

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


def create_dag(dag_args):

    dag_id = dag_args["dag_id"]

    dag = DAG(
        dag_id,
        schedule_interval=dag_args["schedule_interval"],
        default_args=dag_args,
        description=f"Export {dag_id}",
    )

    ds_folder = Path("/opt/airflow/dags") / dag_id

    with dag:

        tl = []
        # Task to load data into source db schema
        for sql in get_sql_list("export-", ds_folder):
            sql_path = ds_folder / sql
            tl.append(
                PostgresOperator(
                    task_id=sql,
                    postgres_conn_id="postgres_default",
                    sql=read_sql_file(sql_path),
                )
            )

        tl

    return dag


# builds a dag for each data set in data_set_list

default_args = {
    "owner": "airflow",
    "start_date": days_ago(0),
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # none airflow common dag elements
    "on_failure_callback": alert_slack_channel,
}

dag_args = {**default_args, **ds}

globals()[dag_args["dag_id"]] = create_dag(dag_args)
