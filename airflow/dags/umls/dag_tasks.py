from airflow.decorators import task
import pandas as pd
import requests
import json
import logging
from sagerx import load_df_to_pg
from airflow.models import Variable
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# UMLS API key from Airflow Variables
apikey = Variable.get("umls_api")

# List of target vocabularies, e.g., ['ICD10CM', 'SNOMEDCT_US', 'ICD9CM']
targets = ['SNOMEDCT_US', 'ICD10CM', 'ICD9CM']

def fetch_concepts_by_tty(tty):
    """
    Fetch all concept dictionaries for a given Therapeutic Type (TTY)
    from RxNav. Each concept dictionary includes fields like 'rxcui', 'name', etc.
    """
    url = f"https://rxnav.nlm.nih.gov/REST/allconcepts.json?tty={tty}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data['minConceptGroup']['minConcept']
    else:
        logging.error(f"Failed to fetch concepts for TTY {tty}: HTTP Status {response.status_code}")
        return []

def get_matching_codes_and_descriptions(mesh_code, version='current'):
    """
    For a given MeSH code, fetch matching codes and descriptions
    from the UMLS crosswalk API.
    """
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

def process_data(tty):
    """Main function to process data and generate the final DataFrame without NDC logic."""
    session = requests.Session()
    concepts = fetch_concepts_by_tty(tty)
    all_data = []
    
    # Iterate through the list of concepts (sequentially)
    for concept in concepts:
        rxcui = concept['rxcui']
        class_url = f"https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json?rxcui={rxcui}"
        class_response = session.get(class_url)
        if class_response.status_code == 200:
            class_data = class_response.json()
            if 'rxclassDrugInfoList' in class_data:
                for class_info in class_data['rxclassDrugInfoList']['rxclassDrugInfo']:
                    if class_info['rxclassMinConceptItem']['classType'] == 'DISEASE':
                        # Record one row per class_info, without any NDC data.
                        all_data.append({
                            'rxcui': rxcui,
                            'name': concept.get('name', ''),
                            'msh_code': class_info['rxclassMinConceptItem']['classId'],
                            'msh_name': class_info['rxclassMinConceptItem']['className'],
                            'rela': class_info.get('rela', '')
                        })
    df = pd.DataFrame(all_data)
    total_concepts = len(concepts)
    processed_concepts = df['rxcui'].nunique() if not df.empty else 0
    failed_count = total_concepts - processed_concepts
    logging.info(f"Processing complete. Total concepts: {total_concepts}, Processed: {processed_concepts}, Failed: {failed_count}")
    
    # Fetch UMLS mapping codes for each unique MeSH code
    unique_mesh_ids = df['msh_code'].unique()
    all_results = []
    for mesh_id in unique_mesh_ids:
        code_results = get_matching_codes_and_descriptions(mesh_id)
        all_results.extend(code_results)
    
    df_results = pd.DataFrame(all_results)
    df_results.columns = [col.lower() for col in df_results.columns]
    df.columns = [col.lower() for col in df.columns]
    
    # Aggregate mappings to a single row per msh_code by taking the first value for each column
    df_final = df_results.groupby('msh_code').agg({col: 'first' for col in df_results.columns if col != 'msh_code'}).reset_index()
    
    # Merge aggregated mapping data with the base RxClass data (without NDC)
    merged_df = pd.merge(df, df_final, on='msh_code', how='left')
    merged_df.columns = [col.lower() for col in merged_df.columns]
    merged_df.drop_duplicates(inplace=True)
    return merged_df

@task
def extract(dag_id: str) -> list:
    tty = 'IN+PIN+MIN+SCDC+SCDF+SCDFP+SCDG+SCDGP+SCD+GPCK+BN+SBDC+SBDF+SBDFP+SBDG+SBD+BPCK'
    df = process_data(tty)
    logging.info(f"Dataframe created with {len(df)} rows.")
    df = df.applymap(lambda x: None if pd.isna(x) else x)
    return df.to_dict(orient='records')

@task
def load(data: list):
    df = pd.DataFrame(data)
    logging.info(f"Dataframe loaded with {len(df)} rows.")
    df = df.applymap(lambda x: None if pd.isna(x) else x)
    load_df_to_pg(df, "sagerx_dev", "rxnorm_to_icd", "replace", index=False)
    logging.info("Data loaded to PostgreSQL.")
