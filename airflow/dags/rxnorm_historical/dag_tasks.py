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
    rxcui_list = get_rxcuis(tty_list)
    logging.info(f"Fetched {len(rxcui_list)} RXCUIs.")

    # 1.5. Create list of urls
    url_list = create_url_list(rxcui_list)

    # query API with each url
    results = get_concurrent_api_results(url_list)

    dfs = []
    for ndc_response in results:
        url = ndc_response['url']
        response = ndc_response['response']
        if 'historicalNdcConcept' in response:
            rxcui_match = re.search(rxcui_pattern, url)
            rxcui = rxcui_match.group('rxcui')

            df = pd.json_normalize(response['historicalNdcConcept']['historicalNdcTime'], 'ndcTime', ['status', 'rxcui'], errors='ignore')
            df['ndc_list'] = df['ndc'].apply(lambda x: x if len(x) > 1 else None)
            df['ndc'] = df['ndc'].apply(lambda x: x[0] if len(x) == 1 else None)
            df.rename(columns={'startDate': 'start_date', 'endDate': 'end_date', 'rxcui': 'related_rxcui'}, inplace=True)
            df['rxcui'] = rxcui
            dfs.append(df)

    load_df_to_pg(pd.concat(dfs),"sagerx_lake","rxnorm_historical","replace",index=False, create_index=True, index_columns=['ndc','end_date'])

    

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
