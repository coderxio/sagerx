import numpy as np
import pandas as pd
from time import sleep
import requests
from requests.adapters import HTTPAdapter, Retry

from pathlib import Path
import pendulum

from sagerx import get_dataset, read_sql_file, get_sql_list, alert_slack_channel, create_path

from airflow.decorators import dag, task

from airflow.operators.python import get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.subprocess import SubprocessHook


@dag(
    schedule="0 0 1 1 *",
    start_date=pendulum.yesterday(),
    catchup=False,
)
def rxclass_atc_to_product():
    dag_id = "rxclass_atc_to_product"
    data_folder = Path("/opt/airflow/data") / dag_id
    file_name = f'{dag_id}.csv'
    file_path = create_path(data_folder) / file_name
    file_path_str = file_path.resolve().as_posix()

    @task
    def get_rxcuis():

        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = pg_hook.get_sqlalchemy_engine()

        df = pd.read_sql(
            "select distinct rxcui from datasource.rxnorm_rxnconso where tty in ('SCD','SBD','GPCK','BPCK') and sab = 'RXNORM' limit 100",
            con=engine
        )

        df.to_csv(
            file_path_str,
            index=False
        )
        
        return file_path_str

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
    def extract(file_path):
        data = pd.read_csv(file_path)
        rxcui_list = data['rxcui'].to_list()

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

        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = pg_hook.get_sqlalchemy_engine()

        atc_df.to_sql(
            "rxclass_atc_to_product",
            con=engine,
            schema="datasource",
            if_exists="replace",
            index=False
        )

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
    extract(get_rxcuis())

rxclass_atc_to_product()
