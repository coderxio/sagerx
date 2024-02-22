from airflow.decorators import task
import pandas as pd
from sagerx import load_df_to_pg, parallel_api_calls


def create_url_list(rxcui_list:list)-> list:
    url = f"https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json?rxcui={rxcui}&relaSource=ATCPROD"
    urls=[]

    for rxcui in rxcui_list:
        urls.append(url)
    return urls

def extract_rxcui_from_url(url:str)->str:
    from urllib.parse import urlparse, parse_qs
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)

    if 'rxcui' in query_params:
        rxcui_value = query_params['rxcui'][0]
    else:
        raise f"Unable to extract RxCui from url: {url}"
    return rxcui_value

@task()
def get_rxcuis() -> list:
    from airflow.hooks.postgres_hook import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg_hook.get_sqlalchemy_engine()

    df = pd.read_sql(
        "select distinct rxcui from datasource.rxnorm_rxnconso where tty in ('SCD','SBD','GPCK','BPCK') and sab = 'RXNORM'",
        con=engine
    )
    
    return list(df['rxcui'])


@task
def extract_atc(rxcui_list:list)->None:
   # Get ATC for full list of RXCUI
    urls = create_url_list(rxcui_list)
    atcs_list = parallel_api_calls(urls)

    atcs = {}

    for atc in atcs_list:
        rxcui = extract_rxcui_from_url(atc['url'])
        atcs[rxcui] = atc['response']["rxclassDrugInfoList"]["rxclassDrugInfo"][0]["rxclassMinConceptItem"]

    atc_df = pd.DataFrame.from_dict(atcs, orient='index').reset_index()

    load_df_to_pg(atc_df,"datasource","rxclass_atc_to_product","replace",index=False)
