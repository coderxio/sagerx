from airflow.decorators import task
import pandas as pd
import re
from sagerx import load_df_to_pg, parallel_api_calls

def create_url_list(rxcui_list:list)-> list:
    urls=[]

    for rxcui in rxcui_list:
        urls.append(f'https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/allhistoricalndcs.json')
    return urls

@task
def get_rxcuis() -> list:
    from airflow.hooks.postgres_hook import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg_hook.get_sqlalchemy_engine()

    df = pd.read_sql(
            "select distinct rxcui from sagerx_lake.rxnorm_rxnconso where tty in ('SCD','SBD','GPCK','BPCK') and sab = 'RXNORM'",
            con=engine
        )
    results = list(df['rxcui'])
    print(f"Number of RxCUIs: {results}")
    return results

rxcui_pattern = re.compile(r'rxcui\/(?P<rxcui>\d+)\/')

@task
def extract_ndc(ndc_list:list)->None:
    urls = create_url_list(ndc_list)
    print(f"URL List created of length: {len(urls)}")
    ndc_responses = parallel_api_calls(urls)
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
