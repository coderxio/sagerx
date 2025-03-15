from airflow.decorators import task
import pandas as pd
import requests
import json
import logging
from sagerx import load_df_to_pg
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# UMLS API key from Airflow Variables
apikey = Variable.get("umls_api")

# List of target vocabularies, e.g., ['ICD10CM', 'SNOMEDCT_US', 'ICD9CM']
targets = ['SNOMEDCT_US', 'ICD10CM', 'ICD9CM']

def get_matching_codes_and_descriptions(mesh_code, version='current'):
    base_uri = 'https://uts-ws.nlm.nih.gov'
    source = 'MSH'
    combined_results = []
    for target in targets:
        crosswalk_endpoint = f'/rest/crosswalk/{version}/source/{source}/{mesh_code}'
        query = {'targetSource': target, 'apiKey': apikey}
        response = requests.get(base_uri + crosswalk_endpoint, params=query)
        if response.status_code != 200:
            logging.error(f"Error: Received status code {response.status_code} for mesh code {mesh_code} and target {target}")
            continue
        try:
            data = response.json()
        except json.JSONDecodeError as e:
            logging.error(f"JSONDecodeError for mesh code {mesh_code} and target {target}: {e}")
            continue
        if 'result' in data:
            for item in data['result']:
                result = {
                    'msh_code': mesh_code,
                    f'{target.lower()}_code': item.get('ui', ''),
                    f'{target.lower()}_description': item.get('name', '')
                }
                combined_results.append(result)
    return combined_results

def process_data():
    # Connect to Postgres and query the table for records with class_type 'DISEASE'
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg_hook.get_sqlalchemy_engine()
    query = """
        SELECT DISTINCT rxcui, name, class_id AS msh_code, class_name AS msh_name, rela
        FROM sagerx_lake.rxclass
        WHERE class_type = 'DISEASE'
    """
    df = pd.read_sql(query, con=engine)
    logging.info(f"Fetched {len(df)} rows from sagerx_lake.rxclass where class_type = 'DISEASE'.")

    # Fetch UMLS mapping codes for each unique MeSH code (which comes from class_id)
    unique_mesh_ids = df['msh_code'].unique()
    all_results = []
    for mesh_id in unique_mesh_ids:
        code_results = get_matching_codes_and_descriptions(mesh_id)
        all_results.extend(code_results)
    
    df_results = pd.DataFrame(all_results)
    if not df_results.empty:
        df_results.columns = [col.lower() for col in df_results.columns]
    df.columns = [col.lower() for col in df.columns]
    
    # Aggregate mappings to a single row per msh_code by taking the first value for each column
    if not df_results.empty:
        df_final = df_results.groupby('msh_code').agg({col: 'first' for col in df_results.columns if col != 'msh_code'}).reset_index()
        # Merge aggregated mapping data with the base RxClass data
        merged_df = pd.merge(df, df_final, on='msh_code', how='left')
    else:
        merged_df = df

    merged_df.columns = [col.lower() for col in merged_df.columns]
    merged_df.drop_duplicates(inplace=True)
    return merged_df

@task
def extract(dag_id: str) -> list:
    df = process_data()
    logging.info(f"Dataframe created with {len(df)} rows.")
    df = df.applymap(lambda x: None if pd.isna(x) else x)
    return df.to_dict(orient='records')

@task
def load(data: list):
    df = pd.DataFrame(data)
    logging.info(f"Dataframe loaded with {len(df)} rows.")
    df = df.applymap(lambda x: None if pd.isna(x) else x)
    load_df_to_pg(df, "sagerx_lake", "umls_condition_crosswalk", "replace", index=False)
    logging.info("Data loaded to PostgreSQL.")