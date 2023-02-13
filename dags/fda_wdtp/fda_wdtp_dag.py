from datetime import date, timedelta
from textwrap import dedent
from pathlib import Path
import calendar

from sagerx import get_dataset, read_sql_file, get_sql_list, alert_slack_channel

import user_macros

ds = {
    "dag_id": "fda_wdtp",
    "schedule_interval": "0 9 * * 5",  # run a 9am every thur
    "url": "https://www.accessdata.fda.gov/cder/wdd_3pl_facilities_report.txt",
}


########################### DYNAMIC DAG DO NOT TOUCH BELOW HERE #################################

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


def create_dag(dag_args):

    dag_id = dag_args["dag_id"]
    url = dag_args["url"]
    retrieve_dataset_function = dag_args["retrieve_dataset_function"]

    dag = DAG(
        dag_id,
        schedule_interval=dag_args["schedule_interval"],
        default_args=dag_args,
        description=f"Processes {dag_id} source",
        user_defined_macros=dag_args.get("user_defined_macros"),
    )

    ds_folder = Path("/opt/airflow/dags") / dag_id
    data_folder = Path("/opt/airflow/data") / dag_id

    with dag:

        # Task to download data from web location
        get_data = PythonOperator(
            task_id=f"get_{dag_id}",
            python_callable=retrieve_dataset_function,
            op_kwargs={"ds_url": url, "data_folder": data_folder},
        )

        tl = [get_data]
        # Task to load data into source db schema
        for sql in get_sql_list("load-", ds_folder):
            sql_path = ds_folder / sql
            tl.append(
                PostgresOperator(
                    task_id=sql,
                    postgres_conn_id="postgres_default",
                    sql=read_sql_file(sql_path),
                )
            )

        for sql in get_sql_list("staging-", ds_folder):
            sql_path = ds_folder / sql
            tl.append(
                PostgresOperator(
                    task_id=sql,
                    postgres_conn_id="postgres_default",
                    sql=read_sql_file(sql_path),
                )
            )

        for sql in get_sql_list("view-", ds_folder):
            sql_path = ds_folder / sql
            tl.append(
                PostgresOperator(
                    task_id=sql,
                    postgres_conn_id="postgres_default",
                    sql=read_sql_file(sql_path),
                )
            )

        for sql in get_sql_list("alter-", ds_folder):
            sql_path = ds_folder / sql
            tl.append(
                PostgresOperator(
                    task_id=sql,
                    postgres_conn_id="postgres_default",
                    sql=read_sql_file(sql_path),
                )
            )

        for i in range(len(tl)):
            if i not in [0]:
                tl[i - 1] >> tl[i]

    return dag


# builds a dag for each data set in data_set_list


default_args = {
    "owner": "airflow",
    "start_date": days_ago(0),
    "depends_on_past": False,
    "email": ["admin@sagerx.io"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # none airflow common dag elements
    "retrieve_dataset_function": get_dataset,
    "on_failure_callback": alert_slack_channel,
}

dag_args = {**default_args, **ds}

globals()[dag_args["dag_id"]] = create_dag(dag_args)
