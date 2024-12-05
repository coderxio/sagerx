from airflow.decorators import task
import pandas as pd
import asyncio
import aiohttp
import time
from sagerx import load_df_to_pg

def create_url_list(rxcui_list: list) -> list:
    return [
        f"https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json?rxcui={rxcui}"
        for rxcui in rxcui_list
    ]

async def fetch(session, url, semaphore):
    async with semaphore:
        async with session.get(url) as response:
            return await response.json()

async def fetch_all(urls, max_calls_per_second=20):
    semaphore = asyncio.Semaphore(max_calls_per_second)
    connector = aiohttp.TCPConnector(limit=max_calls_per_second)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for url in urls:
            tasks.append(fetch(session, url, semaphore))
            await asyncio.sleep(1 / max_calls_per_second)
        return await asyncio.gather(*tasks)

@task
def extract_rxclass(rxcui_list: list) -> None:
    url_list = create_url_list(rxcui_list)
    print(f"URL List created of length: {len(url_list)}")
    
    loop = asyncio.get_event_loop()
    response_list = loop.run_until_complete(fetch_all(url_list, max_calls_per_second=20))

    # Collect all rows in a list to turn into a Dataframe later
    rxclass_rows = []

    for response in response_list:
        if 'rxclassDrugInfoList' in response['response']:
            rxclasses = response['response']['rxclassDrugInfoList']['rxclassDrugInfo']
            for rxclass in rxclasses:
                rxclass_row = {
                    "rxcui": rxclass["minConcept"]["rxcui"],
                    "name": rxclass["minConcept"]["name"],
                    "tty": rxclass["minConcept"]["tty"],
                    "class_id": rxclass["rxclassMinConceptItem"]["classId"],
                    "class_name": rxclass["rxclassMinConceptItem"]["className"],
                    "class_type": rxclass["rxclassMinConceptItem"]["classType"],
                    "rela": rxclass["rela"],
                    "rela_source": rxclass["relaSource"],
                }
                rxclass_rows.append(rxclass_row)
        else:
            print(f"Unexpected response format: {response}")

    # Create DataFrame from list of rows
    rxclass_df = pd.DataFrame(rxclass_rows)

    # Remove duplicates
    rxclass_df.drop_duplicates(inplace=True)

    # Load the DataFrame
    load_df_to_pg(rxclass_df, "sagerx_lake", "rxclass_2", "replace", index=False)