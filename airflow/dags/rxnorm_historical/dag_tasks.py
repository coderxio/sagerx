from airflow.decorators import task
import pandas as pd
import re
from sagerx import get_rxcuis, load_df_to_pg, get_concurrent_api_results, read_json_file
from common_dag_tasks import get_data_folder
import logging
import json

def create_url_list(rxcui_list:list)-> list:
    urls=[]

    for rxcui in rxcui_list:
        urls.append(f'https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/allhistoricalndcs.json')
    return urls

rxcui_pattern = re.compile(r'rxcui\/(?P<rxcui>\d+)\/')

@task
def extract(dag_id:str) -> str:
    # 1. Fetch the list of concepts
    #tty_list = ['SCD', 'SBD', 'GPCK', 'BPCK']
    tty_list = ['BPCK']
    rxcui_list = get_rxcuis(tty_list)

    # 1.5. Create list of urls
    url_list = create_url_list(rxcui_list)

    # query API with each url
    results = get_concurrent_api_results(url_list)

    data_folder = get_data_folder(dag_id)
    file_path = data_folder / 'data.json'
    file_path_str = file_path.resolve().as_posix()

    with open(file_path_str, 'w') as f:
        json.dump(results, f)
    print(f"Extraction Completed! Data saved to file: {file_path_str}")

    return file_path_str

@task
def load(dag_id):
    # TODO: should pass file_path_str from extract to load
    # instead of doing this again
    data_folder = get_data_folder(dag_id)
    file_path = data_folder / 'data.json'
    file_path_str = file_path.resolve().as_posix()

    results = read_json_file(file_path_str)
    
    # Initialize a list to store the processed data
    records = []
    for result in results:
        if not len(result['response']) == 0:
            response = result['response']
            if 'historicalNdcConcept' in response:
                url = result['url']
                rxcui_match = re.search(rxcui_pattern, url)
                rxcui = rxcui_match.group('rxcui')

                # Extract data and transform into dictionaries
                historical_ndc = response['historicalNdcConcept']['historicalNdcTime']
                for entry in historical_ndc:
                    for ndc_time in entry['ndcTime']:
                        record = {
                            'ndc': ndc_time['ndc'][0] if len(ndc_time['ndc']) == 1 else None,
                            'rxcui_list': ndc_time['ndc'] if len(ndc_time['ndc']) > 1 else None,
                            'start_date': ndc_time['startDate'],
                            'end_date': ndc_time['endDate'],
                            'status': entry['status'],
                            'related_rxcui': entry['rxcui'],
                            'rxcui': rxcui
                        }
                        records.append(record)
            else:
                print(f'Error in parsing response: {response}')

    # Create a single DataFrame from the list of dictionaries
    df = pd.DataFrame.from_records(records)
    print(f'Processed {len(df)} RXCUIs.')

    # Load the final DataFrame into the database
    load_df_to_pg(df, "sagerx_lake", "rxnorm_historical", "replace", index=False, create_index=True, index_columns=['ndc', 'end_date'])
