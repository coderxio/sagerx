import logging
import os
from datetime import date

import requests
from bs4 import BeautifulSoup
import pandas as pd

import pendulum

from airflow_operator import create_dag
from airflow.providers.postgres.operators.postgres import PostgresOperator

from common_dag_tasks import  extract, transform, generate_sql_list, get_ds_folder
from sagerx import read_sql_file, load_df_to_pg

from airflow.decorators import task


dag_id = "ashp"

dag = create_dag(
    dag_id=dag_id,
    schedule="0 4 * * *",
    start_date=pendulum.yesterday(),
    catchup=False,
    concurrency=2,
)

with dag:
    url= "https://www.ashp.org/drug-shortages/current-shortages/drug-shortages-list?page=CurrentShortages"
    ds_folder = get_ds_folder(dag_id)

    transform_task = transform(dag_id)

    @task
    def extract_load_shortage_list():
        logging.basicConfig(level=logging.INFO, format='%(asctime)s : %(levelname)s : %(message)s')

        logging.info('Checking ASHP website for updates')
        shortage_list = requests.get(url)

        if shortage_list.status_code != 200:
            logging.error('ASHP website unreachable')
            exit()

        ashp_drugs = []
        soup = BeautifulSoup(shortage_list.content, 'html.parser')
        for link in soup.find(id='1_dsGridView').find_all('a'):
            ashp_drugs.append({
                'name': link.get_text(),
                'detail_url': link.get('href')
            })
        
        if len(ashp_drugs) > 0:
            df = pd.DataFrame(ashp_drugs)
            load_df_to_pg(df, "sagerx_lake", "ashp_shortage_list", "replace", index=False)
        else:
            logging.error('Drug shortage list not found')
        
    extract_load_shortage_list() >> transform_task
