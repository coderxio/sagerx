from pathlib import Path
import os
import pendulum
# zipfile_deflate64 is needed likely because of how CMS zips these files
import zipfile_deflate64 as zipfile # this package is not easily accessible in this context - consider an alternative

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
def cms_part_d_plans():
    dag_id = "cms_part_d_plans"
    file_date = "20230908"
    file_date_year = file_date[:4]
    file_date_quarter = user_macros.get_quarter(pendulum.from_format(file_date, 'YYYYMMDD'))

    ds_url = f"https://download.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Pharmacy-Puf/Downloads/SPUF_{file_date_year}_{file_date}.zip"

    # Task to download data from web location
    @task
    def extract():
        data_path = Path("/opt/airflow/data") / dag_id
        data_path = get_dataset(ds_url, data_path)
        return data_path

    @task
    def unzip(data_path):
        data_path = create_path(data_path)
        zip_file_names = [file for file in os.listdir(data_path) if file.endswith('.zip')]

        for file_name in zip_file_names:
            # if not the sample file
            if not file_name.startswith('sample'):
                file_path = create_path(data_path, file_name)
                # check if the file is a .zip file
                if zipfile.is_zipfile(file_path):
                    with zipfile.ZipFile(file_path, 'r') as zip_ref:
                        # extract the contents to the output folder
                        zip_ref.extractall(data_path.with_suffix(""))

        txt_file_names = [file for file in os.listdir(data_path) if file.endswith('.txt')]

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

cms_part_d_plans()
