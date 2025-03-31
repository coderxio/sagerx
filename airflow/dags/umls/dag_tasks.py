from airflow.decorators import task
import pandas as pd
import re
from sagerx import camel_to_snake, get_rxcuis, load_df_to_pg, get_concurrent_api_results, write_json_file, read_json_file, create_path
from common_dag_tasks import get_data_folder
import logging
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

def create_url_list(mesh_code_list:list)-> list:
    base_uri = 'https://uts-ws.nlm.nih.gov/rest'
    source = 'MSH'
    urls=[]

    # UMLS API key from Airflow Variables
    api_key = Variable.get("umls_api")

    version = 'current'

    # list of target vocabularies, e.g., ['ICD10CM', 'SNOMEDCT_US', 'ICD9CM']
    target_list = ['SNOMEDCT_US', 'ICD10CM', 'ICD9CM']
    targets = ','.join(target_list)

    for mesh_code in mesh_code_list:
        urls.append(f'{base_uri}/crosswalk/{version}/source/{source}/{mesh_code}?targetSource={targets}&apiKey={api_key}&pageSize=100')
        
    return urls

@task
def extract(dag_id:str) -> str:
    # connect to Postgres and query the table for records with class_type 'DISEASE'
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg_hook.get_sqlalchemy_engine()
    query = """
        select distinct
            class_id as mesh_code
        from sagerx_lake.rxclass
        where class_type = 'DISEASE'
    """
    df = pd.read_sql(query, con=engine)
    logging.info(f"Fetched {len(df)} rows from sagerx_lake.rxclass where class_type = 'DISEASE'.")

    # get list of MeSH codes from df
    mesh_code_list = df['mesh_code']

    logging.info("Starting data retrieval for UMLS...")

    # create list of urls
    url_list = create_url_list(mesh_code_list)
    print(url_list)
    print(f'URL list length: {len(url_list)}')
    results = get_concurrent_api_results(url_list)

    data_folder = get_data_folder(dag_id)
    file_path = create_path(data_folder) / 'data.json'
    file_path_str = file_path.resolve().as_posix()

    write_json_file(file_path_str, results)

    print(f"Extraction Completed! Data saved to file: {file_path_str}")

    return file_path_str


@task
def load(file_path_str:str):
    results = read_json_file(file_path_str)

    # regex pattern to extract MeSH code (D#####) from the url
    mesh_code_pattern = re.compile(r"/MSH/(D\d+)\?")

    # Create a DataFrame directly from JSON
    df = pd.json_normalize(
        results, 
        record_path=["response", "result"], 
        meta=["url"],  # Keep 'url' to extract mesh_code
    )

    # extract mesh_code from the URL column
    df["mesh_code"] = df["url"].str.extract(mesh_code_pattern)

    # drop the original URL column
    df.drop(columns=["url"], inplace=True)

    # rename columns to snake_case
    df.rename(columns={col: camel_to_snake(col) for col in df.columns}, inplace=True)

    # ensure mesh_code is the first column
    df = df[["mesh_code"] + [col for col in df.columns if col != "mesh_code"]]

    print(f'Dataframe created of {len(df)} length.')
    load_df_to_pg(df,"sagerx_lake","umls_crosswalk","replace",index=False)
