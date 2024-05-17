import numpy as np
import pandas as pd
from time import sleep
import requests
from requests.adapters import HTTPAdapter, Retry
import sqlalchemy

from pathlib import Path
import pendulum

from sagerx import get_dataset, read_sql_file, get_sql_list, alert_slack_channel, create_path

from airflow.decorators import dag, task

from airflow.operators.python import get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.subprocess import SubprocessHook


from airflow_operator import create_dag
from common_dag_tasks import  get_ds_folder, get_data_folder, transform
from rxnorm_historical.dag_tasks import get_rxcuis, extract_ndc

dag_id = "rxnorm_historical"

dag = create_dag(
    dag_id=dag_id,
    schedule= "0 0 1 1 *",
    max_active_runs=1,
    catchup=False,
)

with dag:
    ds_folder = get_ds_folder(dag_id)
    data_folder = get_data_folder(dag_id)

    rxcuis = get_rxcuis()
    extract_ndc(rxcuis) >> transform(dag_id)