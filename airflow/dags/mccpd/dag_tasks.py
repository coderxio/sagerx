from airflow.decorators import task
import pandas as pd
import re
from sagerx import fetch_json, camel_to_snake, get_rxcuis, load_df_to_pg, get_concurrent_api_results, write_json_file, read_json_file, create_path
from common_dag_tasks import url_request, get_data_folder
import logging
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

@task
def extract(dag_id:str) -> str:
    url = 'https://us-central1-costplusdrugs-publicapi.cloudfunctions.net/main'
    results = fetch_json(url)
    print(results)

    data_folder = get_data_folder(dag_id)
    file_path = create_path(data_folder) / 'data.json'
    file_path_str = file_path.resolve().as_posix()

    write_json_file(file_path_str, results)

    print(f"Extraction Completed! Data saved to file: {file_path_str}")

    return file_path_str


@task
def load(file_path_str:str):
    results = read_json_file(file_path_str)

    # Create a DataFrame directly from JSON
    df = pd.json_normalize(
        results, 
        record_path=["results"]
    )

    print(f'Dataframe created of {len(df)} length.')
    load_df_to_pg(df,"sagerx_lake","mccpd","replace",index=False)
