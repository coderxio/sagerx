from airflow.decorators import task
import pandas as pd
import re
from sagerx import load_df_to_pg, async_api_calls, parallel_api_calls, rate_limited_api_calls

def create_url_list(rxcui_list:list)-> list:
    return [
        f"https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/allhistoricalndcs.json"
        for rxcui in rxcui_list
    ]

rxcui_pattern = re.compile(r'rxcui\/(?P<rxcui>\d+)\/')

@task
def extract_ndc(rxcui_list:list) -> None:
    url_list = create_url_list(rxcui_list)
    print(f"URL List created of length: {len(url_list)}")
    response_list = async_api_calls(url_list)
    dfs = []
    for response in response_list:
        if not len(response['data']) == 0:
            if response['data'].get('historicalNdcConcept') == None:
                print(response)
            url = response['url']
            rxcui_match = re.search(rxcui_pattern, url)
            rxcui = rxcui_match.group('rxcui')

            df = pd.json_normalize(response['data']['historicalNdcConcept']['historicalNdcTime'], 'ndcTime', ['status', 'rxcui'], errors='ignore')
            df['rxcui_list'] = df['ndc'].apply(lambda x: x if len(x) > 1 else None)
            df['ndc'] = df['ndc'].apply(lambda x: x[0] if len(x) == 1 else None)
            df.rename(columns={'startDate': 'start_date', 'endDate': 'end_date', 'rxcui': 'related_rxcui'}, inplace=True)
            df['rxcui'] = rxcui
            dfs.append(df)

    load_df_to_pg(pd.concat(dfs),"sagerx_lake","rxnorm_historical","replace",index=False, create_index=True, index_columns=['ndc','end_date'])
