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


@dag(
    schedule="0 0 1 1 *",
    start_date=pendulum.yesterday(),
    catchup=False,
)
def rxnorm_historical():
    dag_id = "rxnorm_historical"
    data_folder = Path("/opt/airflow/data") / dag_id
    file_name = f'{dag_id}.csv'
    file_path = create_path(data_folder) / file_name
    file_path_str = file_path.resolve().as_posix()

    @task
    def get_rxcuis():

        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = pg_hook.get_sqlalchemy_engine()

        df = pd.read_sql(
            "select distinct rxcui from datasource.rxnorm_rxnconso where tty in ('SCD','SBD','GPCK','BPCK') and sab = 'RXNORM'",
            con=engine
        )

        df.to_csv(
            file_path_str,
            index=False
        )
        
        return file_path_str

    def get_historical_ndcs(rxcui):
        try:
            s = requests.Session()
            retries = Retry(total=5,
                            backoff_factor=0.5)
            s.mount('https://', HTTPAdapter(max_retries=retries))
            response = s.get(f'https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/allhistoricalndcs.json')
            response.raise_for_status()
            resp = response.json()
            return resp
        except Exception as e:
            print(e)
            return -1

    # Task to download data from web location
    @task
    def extract_load(file_path):
        data = pd.read_csv(file_path)
        rxcui_list = data['rxcui'].to_list()

        # Get historical NDCs for full list of RXCUI
        ndcs = {}
        failed_atc = []

        count = 0
        for rxcui in rxcui_list:
            count += 1
            if count % 1000 == 0:
                print(f'MILESTONE {count}')
            sleep(0.1)
            ndc = get_historical_ndcs(rxcui)
            if ndc == -1:
                failed_atc.append(rxcui)
            else:
                ndcs[rxcui] = ndc

        ndc_df = pd.DataFrame.from_dict(ndcs, orient='index').reset_index()
        ndc_df.columns = ['rxcui', 'ndcs']
        print(ndc_df.head(10))

        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = pg_hook.get_sqlalchemy_engine()

        engine.execute('DROP TABLE IF EXISTS datasource.rxnorm_historical cascade')

        ndc_df.to_sql(
            "rxnorm_historical",
            con=engine,
            schema="datasource",
            if_exists="append",
            dtype={"ndcs": sqlalchemy.dialects.postgresql.JSONB},
            index=False
        )

    # Task to transform data using dbt
    @task
    def transform():
        subprocess = SubprocessHook()
        result = subprocess.run_command(['dbt', 'run', '--select', 'models/staging/rxnorm_historical'], cwd='/dbt/sagerx')
        print("Result from dbt:", result)

    extract_load(get_rxcuis()) >> transform()

rxnorm_historical()
