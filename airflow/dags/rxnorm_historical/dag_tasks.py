from airflow.decorators import task
import pandas as pd
import re
from sagerx import get_rxcuis, load_df_to_pg, get_concurrent_api_results
import logging

def create_url_list(rxcui_list:list)-> list:
    urls=[]

    for rxcui in rxcui_list:
        urls.append(f'https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/allhistoricalndcs.json')
    return urls

rxcui_pattern = re.compile(r'rxcui\/(?P<rxcui>\d+)\/')

@task
def extract():
    logging.info("Starting data retrieval for RxNorm Historical...")

    # 1. Fetch the list of concepts
    tty_list = ['SCD', 'SBD', 'GPCK', 'BPCK']
    #tty_list = ['BPCK']
    rxcui_list = get_rxcuis(tty_list)
    logging.info(f"Fetched {len(rxcui_list)} RXCUIs.")

    # 1.5. Create list of urls
    url_list = create_url_list(rxcui_list)

    # query API with each url
    results = get_concurrent_api_results(url_list)

    dfs = []
    for result in results:
        url = result['url']
        response = result['response']
        if 'historicalNdcConcept' in response:
            rxcui_match = re.search(rxcui_pattern, url)
            rxcui = rxcui_match.group('rxcui')

            df = pd.json_normalize(response['historicalNdcConcept']['historicalNdcTime'], 'ndcTime', ['status', 'rxcui'], errors='ignore')
            df['ndc_list'] = df['ndc'].apply(lambda x: x if len(x) > 1 else None)
            df['ndc'] = df['ndc'].apply(lambda x: x[0] if len(x) == 1 else None)
            df.rename(columns={'startDate': 'start_date', 'endDate': 'end_date', 'rxcui': 'related_rxcui'}, inplace=True)
            df['rxcui'] = rxcui
            dfs.append(df)

    concat_dfs = pd.concat(dfs)
    print(f'Processed {len(concat_dfs)} RXCUIs.')
    load_df_to_pg(concat_dfs,"sagerx_lake","rxnorm_historical","replace",index=False, create_index=True, index_columns=['ndc','end_date'])

    '''
    # TODO: explore this as an alternative to making a ton of dataframes
    # OR: i think i wrote some other dict-based code in a local branch

    hist_concept = data_json.get("historicalNdcConcept", {})
    hist_times = hist_concept.get("historicalNdcTime", [])

    if not hist_times:
        logging.warning(f"No historicalNdcTime found for rxcui={rxcui}. Skipping.")
        return None

    rows = []
    for block in hist_times:
        status = block.get("status")
        historical_rxcui = block.get("rxcui")
        ndc_list = block.get("ndcTime", [])

        for ndc_obj in ndc_list:
            # Grab 'ndc' from the object (could be a list or None)
            ndc_data = ndc_obj.get("ndc")

            # If 'ndc_data' is a non-empty list, take its first element and make it a string. There is always only one element according to the docs.
            if isinstance(ndc_data, list) and ndc_data:
                ndc_data = ndc_data[0]

            # Append to the rows
            rows.append({
                "rxcui": rxcui,
                "status": status,
                "historical_rxcui": historical_rxcui,
                "ndc": ndc_data,
                "startDate": ndc_obj.get("startDate"),
                "endDate": ndc_obj.get("endDate"),
            })

    return rows

    '''
    

@task
def extract_ndc(ndc_list:list)->None:
    urls = create_url_list(ndc_list)
    print(f"URL List created of length: {len(urls)}")
    ndc_responses = []
    dfs = []
    for ndc_response in ndc_responses:
        if ndc_response['response'].get('historicalNdcConcept') == None:
            print(ndc_response)
        url = ndc_response['url']
        rxcui_match = re.search(rxcui_pattern, url)
        rxcui = rxcui_match.group('rxcui')

        df = pd.json_normalize(ndc_response['response']['historicalNdcConcept']['historicalNdcTime'], 'ndcTime', ['status', 'rxcui'], errors='ignore')
        df['ndc_list'] = df['ndc'].apply(lambda x: x if len(x) > 1 else None)
        df['ndc'] = df['ndc'].apply(lambda x: x[0] if len(x) == 1 else None)
        df.rename(columns={'startDate': 'start_date', 'endDate': 'end_date', 'rxcui': 'related_rxcui'}, inplace=True)
        df['rxcui'] = rxcui
        dfs.append(df)

    load_df_to_pg(pd.concat(dfs),"sagerx_lake","rxnorm_historical","replace",index=False, create_index=True, index_columns=['ndc','end_date'])
