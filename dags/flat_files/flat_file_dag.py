from datetime import date, timedelta
from textwrap import dedent
from pathlib import Path
import calendar

from sagerx import read_sql_file, get_sql_list
import sqlalchemy
import os
import pandas as pd

db_conn_string = os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"]
db_conn = sqlalchemy.create_engine(db_conn_string)

ds = {"dag_id": "flat_files", "schedule_interval": "0 0 1 1 *"}  # yearly


def export_table(data_folder, table):

    df = pd.read_sql(f"SELECT * FROM flatfile.{table}", con=db_conn)
    df.to_csv(f"{data_folder}/{table}.txt", sep="|", quotechar='"')


def get_table_list(schema_name: str):
    import pandas as pd

    df = pd.read_sql(
        f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'",
        con=db_conn,
    )
    table_list = df["table_name"].values.tolist()
    return table_list


########################### DYNAMIC DAG DO NOT TOUCH BELOW HERE #################################

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
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
    data_folder = Path("/opt/airflow/data") / dag_id

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
        for table in get_table_list("flatfile"):
            tl.append(
                PythonOperator(
                    task_id=f"export_{dag_id}",
                    python_callable=export_table,
                    op_kwargs={"data_folder": data_folder, "table": table},
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
}

dag_args = {**default_args, **ds}

globals()[dag_args["dag_id"]] = create_dag(dag_args)
