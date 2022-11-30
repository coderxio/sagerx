from airflow.models import Variable
from datetime import date, datetime, timedelta
from textwrap import dedent
from pathlib import Path

from sagerx import get_dataset, read_sql_file, get_sql_list, alert_slack_channel

ds = {
    "dag_id": "fda_enforcement",
    "schedule_interval": "0 0 * * 4",  # run weekly on Thursday - looks like report dates are usually Wednesdays
}


########################### DYNAMIC DAG DO NOT TOUCH BELOW HERE #################################

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


# builds a dag for each data set in data_set_list
default_args = {
    "owner": "airflow",
    "start_date": datetime(2012, 1, 1),
    "depends_on_past": False,
    "email": ["admin@sagerx.io"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=0.1),
    # none airflow common dag elements
    "retrieve_dataset_function": get_dataset,
    "on_failure_callback": alert_slack_channel,
}

dag_args = {**default_args, **ds}


dag_id = dag_args["dag_id"]
retrieve_dataset_function = dag_args["retrieve_dataset_function"]

dag = DAG(
    dag_id,
    schedule_interval=dag_args["schedule_interval"],
    default_args=dag_args,
    description=f"Processes {dag_id} source",
    user_defined_macros=dag_args.get("user_defined_macros"),
    max_active_runs=1
)

ds_folder = Path("/opt/airflow/dags") / dag_id
data_folder = Path("/opt/airflow/data") / dag_id

with dag:

    @task(task_id="EL_fda_enforcement")
    def extract_load_dataset(data_interval_start=None, data_interval_end=None):
        import requests
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
        df["openfda"] = df["openfda"].astype("str")
        df.to_sql(
            "fda_enforcement", con=engine, schema="datasource", if_exists="replace"
        )

    el_ds = extract_load_dataset()

    tl = [el_ds]
    # Task to load data into source db schema
    for sql in get_sql_list(
        "load-",
        ds_folder,
    ):
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
