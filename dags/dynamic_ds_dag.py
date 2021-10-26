from datetime import date, timedelta
from textwrap import dedent
from pathlib import Path

from sagerx import get_dataset, read_sql_file, get_sql_list


data_set_list = [
    {
        "dag_id": "nadac",
        "schedule": "0 6 * * 5",  # run a 6am every thur (url marco minuses day to get wed)
        "url": "https://download.medicaid.gov/data/nadac-national-average-drug-acquisition-cost-{{ macros.ds_format( macros.ds_add(ds ,-1), '%Y-%m-%d', '%m-%d-%Y' ) }}.csv",
        #   "url": "https://download.medicaid.gov/data/nadac-national-average-drug-acquisition-cost-10-20-2021.csv"
    },
    {
        "dag_id": "fda_excluded",
        "schedule": "30 4 * * *",  # run a 4:30am every day
        "url": "https://www.accessdata.fda.gov/cder/ndc_excluded.zip",
    },
    {
        "dag_id": "fda_unfinished",
        "schedule": "15 4 * * *",  # run a 4:15am every day
        "url": "https://www.accessdata.fda.gov/cder/ndc_unfinished.zip",
    },
]


########################### DYNAMIC DAG DO NOT TOUCH BELOW HERE #################################

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


def create_dag(dag_id, schedule, url, default_args):

    dag = DAG(
        dag_id,
        default_args=default_args,
        description=f"Processes {dag_id} source",
        schedule_interval=schedule,
    )

    ds_folder = Path("/opt/airflow/dags") / dag_id
    data_folder = Path("/opt/airflow/data") / dag_id

    with dag:

        # Task to download data from web location
        get_data = PythonOperator(
            task_id=f"get_{dag_id}",
            python_callable=get_dataset,
            op_kwargs={"ds_url": url, "data_folder": data_folder},
        )

        tl = [get_data]
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

        for i in range(len(tl)):
            if i not in [0]:
                tl[i - 1] >> tl[i]

    return dag


# builds a dag for each data set in data_set_list
for ds in data_set_list:

    dag_id = ds["dag_id"]
    schedule = ds["schedule"]
    url = ds["url"]

    default_args = {
        "owner": "airflow",
        "start_date": days_ago(0),
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }

    globals()[dag_id] = create_dag(dag_id, schedule, url, default_args)
