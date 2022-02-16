from airflow.models import Variable
from datetime import date, timedelta
from textwrap import dedent
from pathlib import Path

from sagerx import get_dataset, read_sql_file, get_sql_list, alert_slack_channel

download_url = "https://download.nlm.nih.gov/umls/kss/rxnorm/RxNorm_full_current.zip"
apikey =  Variable.get('umls_api')

ds = {
    "dag_id": "rxnorm_full",
    "schedule_interval": "0 0 1 * *",  # run once monthly)
    "url": "https://download.nlm.nih.gov/umls/kss/rxnorm/RxNorm_full_current.zip",
}


def obtain_umls_tgt(apikey: str, ti):
    import requests
    import logging

    url = "https://utslogin.nlm.nih.gov/cas/v1/api-key"
    param = {"apikey": apikey}
    headers = {"Content-type": "application/x-www-form-urlencoded"}

    tgt_response = requests.post(url, headers=headers, data=param)

    first, second = tgt_response.text.split("api-key/")
    tgt_ticket, fourth = second.split('" method')

    # print(TGTTicket)
    logging.info(apikey)
    ti.xcom_push(key="tgt_ticket", value=tgt_ticket)


def obtain_umls_st(download_url: str, ti):
    import requests

    tgt_ticket = ti.xcom_pull(key="tgt_ticket", task_ids=f"obtain_tgt_for_rxnorm_full")
    url = f"https://utslogin.nlm.nih.gov/cas/v1/tickets/{tgt_ticket}"
    param = {"service": download_url}
    headers = {"Content-type": "application/x-www-form-urlencoded"}

    st_response = requests.post(url, headers=headers, data=param)

    # print(STResponse.text)
    ti.xcom_push(key="st_response", value=st_response.text)


########################### DYNAMIC DAG DO NOT TOUCH BELOW HERE #################################

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


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
    get_tgt = PythonOperator(
        task_id=f"obtain_tgt_for_{dag_id}",
        python_callable=obtain_umls_tgt,
        op_kwargs={"apikey": apikey},
    )

    get_tst = PythonOperator(
        task_id=f"obtain_st_for_{dag_id}",
        python_callable=obtain_umls_st,
        op_kwargs={"download_url": download_url},
    )

    get_data = PythonOperator(
        task_id=f"get_{dag_id}",
        python_callable=retrieve_dataset_function,
        op_kwargs={
            "ds_url": download_url
            + "?ticket={{ ti.xcom_pull(key='st_response',task_ids='obtain_st_for_rxnorm_full') }}",
            "data_folder": data_folder,
        },
    )

    tl = [get_tgt, get_tst, get_data]
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
