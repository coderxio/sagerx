from airflow.decorators import task
import pandas as pd
import time
from sagerx import async_api_calls, load_df_to_pg

def create_url_list(rxcui_list: list) -> list:
    return [
        f"https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json?rxcui={rxcui}"
        for rxcui in rxcui_list
    ]

@task
def extract_rxclass(rxcui_list: list) -> None:
    url_list = create_url_list(rxcui_list)
    print(f"URL List created of length: {len(url_list)}")
    response_list = async_api_calls(url_list)

    # Collect all rows in a list to turn into a Dataframe later
    rxclass_rows = []
    for response in response_list:
        if not len(response['data']) == 0:
            if 'rxclassDrugInfoList' in response['data']:
                rxclasses = response['data']['rxclassDrugInfoList']['rxclassDrugInfo']
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
    load_df_to_pg(rxclass_df, "sagerx_lake", "rxclass", "replace", index=False)