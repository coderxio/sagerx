from airflow.decorators import task
import pandas as pd
import json
from sagerx import load_df_to_pg, parallel_api_calls, rate_limited_api_calls
import hashlib

def create_url_list(rxcui_list:list) -> list:
    urls=[]

    for rxcui in rxcui_list:
        urls.append(f"https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json?rxcui={rxcui}")

    return urls

# function to generate a hash for a row
def generate_hash(row):
    return hashlib.md5(str(tuple(row.values())).encode()).hexdigest()

@task
def extract_rxclass(rxcui_list:list) -> None:
    url_list = create_url_list(rxcui_list)
    print(f"URL List created of length: {len(url_list)}")
    response_list = rate_limited_api_calls(url_list)
    
    # initialize an empty DataFrame
    rxclass_df = pd.DataFrame(columns=[
            "rxcui", 
            "name",
            "tty",
            "class_id",
            "class_name",
            "class_type",
            "rela",
            "rela_source"
        ])
    
    # initialize a set to store hashes of existing rows
    existing_hashes = set()

    for response in response_list:
        rxclasses = response['rxclassDrugInfoList']['rxclassDrugInfo']
        for rxclass in rxclasses:
            rxclass_row = {
                "rxcui": rxclass["minConcept"]["rxcui"],
                "name": rxclass["minConcept"]["name"],
                "tty": rxclass["minConcept"]["tty"],
                "class_id": rxclass["rxclassMinConceptItem"]["classId"],
                "class_name": rxclass["rxclassMinConceptItem"]["className"],
                "class_type": rxclass["rxclassMinConceptItem"]["classType"],
                "rela": rxclass["rela"],
                "rela_source": rxclass["relaSource"]
            }
                
            rxclass_hash = generate_hash(rxclass_row)
    
            '''
            all of this hash stuff is for efficiency

            there are many, many duplicates in the responses of
            the api calls. for instance, a SBD call and a SCD
            call for a given product will likely return a lot of
            duplicate class information.

            this approach cleans it up in the most efficient way
            because there are 2 million rows in the full response,
            most of which are likely duplicates.
            '''
            # check if the hash is not in the set
            if rxclass_hash not in existing_hashes:
                # add the hash to the set
                existing_hashes.add(rxclass_hash)
                # append the object to the dataframe
                rxclass_df = rxclass_df.append(rxclass_row, ignore_index=True)

    load_df_to_pg(rxclass_df,"sagerx_lake","rxclass","replace",index=False)
