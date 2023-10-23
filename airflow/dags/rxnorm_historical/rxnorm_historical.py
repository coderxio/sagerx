import numpy as np
import pandas as pd
import os
from time import sleep
import requests
from requests.adapters import HTTPAdapter, Retry

from pathlib import Path
import pendulum

from sagerx import get_dataset, read_sql_file, get_sql_list, alert_slack_channel

from airflow.decorators import dag, task

from airflow.operators.python import get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.subprocess import SubprocessHook


@dag(
    schedule="0 4 * * *",
    start_date=pendulum.yesterday(),
    catchup=False,
)
def rxnorm_historical():
    dag_id = "rxnorm_historical"

    def get_atc_from_rxcui(rxcui):
        try:
            s = requests.Session()
            retries = Retry(total=5,
                            backoff_factor=0.5)
            s.mount('https://', HTTPAdapter(max_retries=retries))
            response = s.get(f'https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json?rxcui={rxcui}&relaSource=ATCPROD')
            response.raise_for_status()
            jsonResponse = response.json()
            resp = jsonResponse["rxclassDrugInfoList"]["rxclassDrugInfo"][0]["rxclassMinConceptItem"]
            return resp
        except Exception as e:
            print(e)
            return -1

    # Task to download data from web location
    @task
    def extract():
        # Define list of rxcui

        # For example
        rxcui_list = [198335]

        # Get ATC for full list of RXCUI
        atcs = {}
        failed_atc = []

        for rxcui in rxcui_list:
            sleep(0.1)
            atc = get_atc_from_rxcui(rxcui)
            if atc == -1:
                failed_atc.append(rxcui)
            else:
                atcs[rxcui] = atc

        atc_df = pd.DataFrame.from_dict(atcs, orient='index').reset_index()
        print(atc_df.head(10))

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
        subprocess = SubprocessHook()
        result = subprocess.run_command(['dbt', 'run', '--select', 'models/staging/fda_ndc'], cwd='/dbt/sagerx')
        print("Result from dbt:", result)

    #extract() >> load >> transform()
    extract()

rxnorm_historical()
