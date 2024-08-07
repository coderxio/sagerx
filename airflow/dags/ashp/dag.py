import logging
import os
import re
from datetime import date, datetime
from time import sleep

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
    landing_url = "https://www.ashp.org/drug-shortages/current-shortages/drug-shortages-list?page=CurrentShortages"
    base_url = "https://www.ashp.org/drug-shortages/current-shortages/"
    created_regex = re.compile(r"Created (\w+ \d+, \d+)")
    updated_regex = re.compile(r"Updated (\w+ \d+, \d+)")

    ds_folder = get_ds_folder(dag_id)

    transform_task = transform(dag_id)

    @task
    def extract_load_shortage_list():
        logging.basicConfig(level=logging.INFO, format='%(asctime)s : %(levelname)s : %(message)s')

        logging.info('Checking ASHP website for updates')
        shortage_list = requests.get(landing_url)

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

        for shortage in ashp_drugs:
            shortage_detail_data = requests.get(base_url + shortage['detail_url'])
            soup = BeautifulSoup(shortage_detail_data.content, 'html.parser')

            stamp = soup.find(id='1_lblUpdated').find('p').get_text()

            # Get created date
            try:
                created_date = created_regex.search(stamp).group(1)
                created_date = datetime.strptime(created_date, '%B %d, %Y')
                shortage['created_date'] = created_date
            except AttributeError:
                logging.info(f'Missing ASHP created date for {shortage.get("name")}')
                shortage['created_date'] = None
            except ValueError:
                logging.error(f'Could not parse created date for {shortage.get("name")}')
                shortage['created_date'] = None

            # Get last updated date
            try:
                updated_date = updated_regex.search(stamp).group(1)
                updated_date = datetime.strptime(updated_date, '%B %d, %Y')
                shortage['update_date'] = updated_date
            except AttributeError:
                logging.info(f'Missing ASHP update date for {shortage.get("name")}')
                shortage['update_date'] = None
            except ValueError:
                logging.error(f'Could not parse update date for {shortage.get("name")}')
                shortage['update_date'] = None

            sleep(0.2)
        
        if len(ashp_drugs) > 0:
            df = pd.DataFrame(ashp_drugs)
            load_df_to_pg(df, "sagerx_lake", "ashp_shortage_list", "replace", index=False)
        else:
            logging.error('Drug shortage list not found')
        
    extract_load_shortage_list() >> transform_task
