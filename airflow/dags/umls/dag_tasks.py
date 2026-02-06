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

def create_snomed_relations_url_list(snomed_code_list:list)-> list:
    base_uri = 'https://uts-ws.nlm.nih.gov/rest'
    source = 'SNOMEDCT_US'
    urls=[]

    # UMLS API key from Airflow Variables
    api_key = Variable.get("umls_api")

    version = 'current'

    for snomed_code in snomed_code_list:
        urls.append(f'{base_uri}/content/{version}/source/{source}/{snomed_code}/relations?apiKey={api_key}&pageSize=100')
        
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

    logging.info("Starting first round of data retrieval for UMLS crosswalk...")

    # create list of urls for crosswalk
    url_list = create_url_list(mesh_code_list)
    print(f'Crosswalk URL list length: {len(url_list)}')
    crosswalk_results = get_concurrent_api_results(url_list)

    # Extract SNOMEDCT_US codes from first round results
    logging.info("Extracting SNOMEDCT_US codes from crosswalk results...")
    snomed_codes = set()
    
    for result in crosswalk_results:
        if result and result.get("response") and result["response"].get("result"):
            for item in result["response"]["result"]:
                # Check if the rootSource is SNOMEDCT_US
                # The API returns rootSource, ui, name fields
                if item.get("rootSource") == "SNOMEDCT_US":
                    snomed_code = item.get("ui")
                    if snomed_code:
                        snomed_codes.add(snomed_code)
    
    logging.info(f"Found {len(snomed_codes)} unique SNOMEDCT_US codes")
    
    # Second round: Get relations for SNOMEDCT_US codes
    relations_results = []
    if snomed_codes:
        logging.info("Starting second round of data retrieval for SNOMEDCT_US relations...")
        relations_url_list = create_snomed_relations_url_list(list(snomed_codes))
        print(f'Relations URL list length: {len(relations_url_list)}')
        relations_results = get_concurrent_api_results(relations_url_list)
        logging.info(f"Retrieved {len(relations_results)} relation results")
    
    # Combine both results into a single structure
    all_results = {
        "crosswalk": crosswalk_results,
        "relations": relations_results
    }

    data_folder = get_data_folder(dag_id)
    file_path = create_path(data_folder) / 'data.json'
    file_path_str = file_path.resolve().as_posix()

    write_json_file(file_path_str, all_results)

    print(f"Extraction Completed! Data saved to file: {file_path_str}")

    return file_path_str


@task
def load(file_path_str:str):
    all_data = read_json_file(file_path_str)
    
    # Process crosswalk results (first round)
    crosswalk_results = all_data.get("crosswalk", [])
    
    # regex pattern to extract MeSH code (D#####) from the url
    mesh_code_pattern = re.compile(r"/MSH/(D\d+)\?")
    
    # regex pattern to extract SNOMEDCT_US code from the URL
    snomed_code_pattern = re.compile(r"/SNOMEDCT_US/(\d+)/")
    
    # regex pattern to extract ICD10 code from the relatedId URL
    icd10_code_pattern = re.compile(r"/ICD10/([^/]+)")

    # Create a DataFrame from crosswalk results
    crosswalk_df = pd.json_normalize(
        crosswalk_results, 
        record_path=["response", "result"], 
        meta=["url"],  # Keep 'url' to extract mesh_code
    )

    # extract mesh_code from the URL column
    crosswalk_df["mesh_code"] = crosswalk_df["url"].str.extract(mesh_code_pattern)

    # drop the original URL column
    crosswalk_df.drop(columns=["url"], inplace=True)

    # rename columns to snake_case
    crosswalk_df.rename(columns={col: camel_to_snake(col) for col in crosswalk_df.columns}, inplace=True)

    # Process relations results (second round) to get SNOMEDCT_US -> ICD10 mappings
    relations_results = all_data.get("relations", [])
    
    snomed_to_icd10_mappings = []
    
    for result in relations_results:
        if result and result.get("response") and result["response"].get("result"):
            # Extract SNOMEDCT_US code from URL
            url = result.get("url", "")
            snomed_match = snomed_code_pattern.search(url)
            if not snomed_match:
                continue
            snomed_code = snomed_match.group(1)
            
            # Process each relation in the result
            for relation in result["response"]["result"]:
                additional_relation_label = relation.get("additionalRelationLabel", "")
                related_id = relation.get("relatedId", "")
                
                # Check if this is a "mapped_to" relation to ICD10
                if additional_relation_label == "mapped_to" and "ICD10" in related_id:
                    icd10_match = icd10_code_pattern.search(related_id)
                    if icd10_match:
                        icd10_code = icd10_match.group(1)
                        related_name = relation.get("relatedIdName", "")
                        
                        snomed_to_icd10_mappings.append({
                            "snomed_code": snomed_code,
                            "icd10_code": icd10_code,
                            "icd10_name": related_name
                        })
    
    # Create DataFrame from SNOMEDCT_US -> ICD10 mappings
    if snomed_to_icd10_mappings:
        snomed_icd10_df = pd.DataFrame(snomed_to_icd10_mappings)
        logging.info(f"Created {len(snomed_icd10_df)} SNOMEDCT_US to ICD10 mappings")
        
        # Merge crosswalk results with SNOMEDCT_US -> ICD10 mappings
        # First, get SNOMEDCT_US codes from crosswalk that don't have direct ICD10CM
        # The crosswalk_df has columns: root_source, ui, name (after camel_to_snake conversion)
        snomed_codes_in_crosswalk = crosswalk_df[
            crosswalk_df["root_source"] == "SNOMEDCT_US"
        ][["mesh_code", "ui"]].rename(columns={"ui": "snomed_code"})
        
        # Merge to get ICD10 codes for SNOMEDCT_US codes
        snomed_with_icd10 = snomed_codes_in_crosswalk.merge(
            snomed_icd10_df,
            on="snomed_code",
            how="inner"
        )
        
        # Add these as additional rows to the crosswalk DataFrame
        # Create rows that look like direct ICD10CM mappings
        # We need to match the structure of crosswalk_df (root_source, ui, name)
        additional_rows = []
        for _, row in snomed_with_icd10.iterrows():
            # Create a row matching the structure of crosswalk_df
            new_row = {
                "mesh_code": row["mesh_code"],
                "root_source": "ICD10CM",
                "ui": row["icd10_code"],
                "name": row["icd10_name"],
                "obsolete": False
            }
            # Copy other columns from crosswalk_df with None values
            for col in crosswalk_df.columns:
                if col not in new_row:
                    new_row[col] = None
            additional_rows.append(new_row)
        
        if additional_rows:
            additional_df = pd.DataFrame(additional_rows)
            # Ensure column order matches crosswalk_df
            additional_df = additional_df[crosswalk_df.columns]
            
            # Remove duplicates before merging
            # Drop duplicates based on mesh_code, root_source, and ui (the key identifying fields)
            initial_count = len(additional_df)
            additional_df = additional_df.drop_duplicates(subset=["mesh_code", "root_source", "ui"], keep="first")
            if initial_count != len(additional_df):
                logging.info(f"Removed {initial_count - len(additional_df)} duplicate rows from SNOMEDCT_US to ICD10 mappings")
            
            # Combine with original crosswalk_df
            crosswalk_df = pd.concat([crosswalk_df, additional_df], ignore_index=True)
            logging.info(f"Added {len(additional_df)} ICD10 mappings via SNOMEDCT_US")
    
    # ensure mesh_code is the first column
    df = crosswalk_df[["mesh_code"] + [col for col in crosswalk_df.columns if col != "mesh_code"]]

    print(f'Dataframe created of {len(df)} length.')
    load_df_to_pg(df,"sagerx_lake","umls_crosswalk","replace",index=False)
