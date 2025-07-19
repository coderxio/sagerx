from pathlib import Path
import os
import pendulum
import zipfile

from sagerx import create_path, get_dataset, read_sql_file, get_sql_list, alert_slack_channel

from airflow.decorators import dag, task

from airflow.operators.python import get_current_context
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.subprocess import SubprocessHook

import user_macros

@dag(
    schedule="0 0 1 */3 *",
    start_date=pendulum.yesterday(),
    catchup=False,
)
def cms_part_d():
    dag_id = "cms_part_d"
    file_date = "20250410"
    file_date_year = '2025'
    file_date_quarter = '1'
    # NOTE: this file is from 2025 and yet the files inside represent 2024 Q4 data...
    # so this logic needs to be revisited
    #file_date_year = file_date[:4]
    #file_date_quarter = user_macros.get_quarter(pendulum.from_format(file_date, 'YYYYMMDD'))

    #ds_url = f"https://download.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Pharmacy-Puf/Downloads/SPUF_{file_date_year}_{file_date}.zip"
    #ds_url = f"https://data.cms.gov/sites/default/files/2025-01/e78ce888-f571-4d86-baf0-7a1f9efffff4/SPUF_2025_{file_date}.zip"
    ds_url = "https://data.cms.gov/sites/default/files/dataset_zips/ff7b76ec186d7c104e7f032216f1f5fb/Quarterly%20Prescription%20Drug%20Plan%20Formulary%2C%20Pharmacy%20Network%2C%20and%20Pricing%20Information.zip"
    
    # Task to download data from web location
    @task
    def extract():
        data_path = Path("/opt/airflow/data") / dag_id
        data_path = get_dataset(ds_url, data_path, file_name="cms_part_d_data")
        return data_path

    @task
    def unzip(data_path):
        # SPUF_2025_20250410.zip/<many zip files>
        
        print(data_path)
        data_path = create_path(data_path, "Quarterly Prescription Drug Plan Formulary, Pharmacy Network, and Pricing Information", "2025-Q1")
        print(data_path)
        txt_file_names = []
        
        # Look for the main SPUF zip file
        main_zip_files = [file for file in os.listdir(data_path) if file.startswith('SPUF_') and file.endswith('.zip')]

        for main_zip_file in main_zip_files:
            main_zip_path = create_path(data_path, main_zip_file)
            
            if zipfile.is_zipfile(main_zip_path):
                with zipfile.ZipFile(main_zip_path, 'r') as main_zip:
                    # Extract all inner zip files from the main zip
                    inner_zip_files = [f for f in main_zip.namelist() if f.endswith('.zip')]
                    
                    for inner_zip_name in inner_zip_files:
                        # Extract the inner zip file to a temporary location
                        main_zip.extract(inner_zip_name, data_path)
                        inner_zip_path = create_path(data_path, inner_zip_name)
                        
                        # Now extract txt files from the inner zip
                        if zipfile.is_zipfile(inner_zip_path):
                            with zipfile.ZipFile(inner_zip_path, 'r') as inner_zip:
                                for txt_file in inner_zip.namelist():
                                    if txt_file.endswith('.txt'):
                                        inner_zip.extract(txt_file, data_path)
                                        txt_file_names.append(txt_file)
                        
                        # Clean up the extracted inner zip file
                        os.remove(inner_zip_path)

        return txt_file_names

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
                params={"year": file_date_year, "quarter": file_date_quarter}
            )
        )

    # Task to transform data using dbt
    @task
    def transform():
        subprocess = SubprocessHook()
        result = subprocess.run_command(['dbt', 'run', '--select', 'models/staging/fda_ndc'], cwd='/dbt/sagerx')
        print("Result from dbt:", result)

    unzip(extract()) >> load
    # extract() >> load >> transform()

cms_part_d()
