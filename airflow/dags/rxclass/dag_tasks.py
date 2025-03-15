from airflow.decorators import task
import pandas as pd
from sagerx import load_df_to_pg, get_concurrent_api_results, write_json_file, read_json_file, create_path
from common_dag_tasks import get_data_folder
from airflow.hooks.postgres_hook import PostgresHook
import logging

def create_url_list(rxcui_list: list) -> list:
    urls = []
    for rxcui in rxcui_list:
        urls.append(f"https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json?rxcui={rxcui}")
    return urls

def get_rxcuis_from_db() -> list:
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg_hook.get_sqlalchemy_engine()
    tty_list = ['IN','PIN','MIN','SCDC','SCDF','SCDFP','SCDG','SCDGP',
                'SCD','GPCK','BN','SBDC','SBDF','SBDFP','SBDG','SBD','BPCK']
    # Format the tty_list for the SQL IN clause
    tty_tuple_str = "(" + ",".join(f"'{x}'" for x in tty_list) + ")"
    query = f"""
        SELECT DISTINCT rxcui
        FROM sagerx_lake.rxnorm_rxnconso
        WHERE tty IN {tty_tuple_str} AND rxcui IS NOT NULL
    """
    df = pd.read_sql(query, con=engine)
    rxcui_list = list(df['rxcui'])
    return rxcui_list

@task
def extract(dag_id: str) -> str:
    logging.info("Starting data retrieval for RxClass from database...")
    
    # 1. Get RXCUIs from the database
    rxcui_list = get_rxcuis_from_db()
    logging.info(f"Fetched {len(rxcui_list)} distinct RXCUIs from sagerx_lake.rxnorm_rxnconso.")
    
    # 2. Create list of API URLs using the RXCUIs
    url_list = create_url_list(rxcui_list)
    
    # 3. Retrieve results concurrently from the RxClass API
    results = get_concurrent_api_results(url_list)
    
    # 4. Filter out None responses and log the count of skipped responses
    none_count = sum(1 for result in results if result is None)
    if none_count:
        logging.info(f"Skipped {none_count} None responses from RxClass API calls.")
    results = [result for result in results if result is not None]
    
    # 5. Save the results to a JSON file
    data_folder = get_data_folder(dag_id)
    file_path = create_path(data_folder) / 'data.json'
    file_path_str = file_path.resolve().as_posix()
    
    write_json_file(file_path_str, results)
    logging.info(f"Extraction completed! Data saved to file: {file_path_str}")
    
    return file_path_str

@task
def load(file_path_str: str):
    results = read_json_file(file_path_str)
    
    classes = []
    for result in results:
        # Skip any None results for extra safety
        if result is None:
            continue
        response = result.get('response')
        if response and 'rxclassDrugInfoList' in response:
            for drug_info in response["rxclassDrugInfoList"]["rxclassDrugInfo"]:
                classes.append(
                    dict(
                        rxcui=drug_info["minConcept"].get("rxcui"),
                        name=drug_info["minConcept"].get("name", ""),
                        tty=drug_info["minConcept"].get("tty", ""),
                        rela=drug_info.get("rela", ""),
                        class_id=drug_info["rxclassMinConceptItem"].get("classId", ""),
                        class_name=drug_info["rxclassMinConceptItem"].get("className", ""),
                        class_type=drug_info["rxclassMinConceptItem"].get("classType", ""),
                        rela_source=drug_info.get("relaSource", "")
                    )
                )
    df = pd.DataFrame(classes).drop_duplicates()
    logging.info(f"Dataframe created with {len(df)} rows.")
    load_df_to_pg(df, "sagerx_lake", "rxclass", "replace", index=False)
    logging.info("Data loaded to PostgreSQL.")