from airflow.decorators import task
import pandas as pd
from sagerx import get_rxcuis, load_df_to_pg, get_concurrent_api_results
import logging

def create_url_list(rxcui_list:list)-> list:
    urls=[]

    for rxcui in rxcui_list:
        urls.append(f"https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json?rxcui={rxcui}")
    return urls

@task
def extract():
    """
    Retrieves RxClass concepts from RxNav for the EPC class type,
    processes them concurrently, and loads results into Postgres.
    """
    logging.info("Starting data retrieval for RxClass...")

    # 1. Fetch the list of concepts
    tty_list = ['IN','PIN','MIN','SCDC','SCDF','SCDFP','SCDG','SCDGP','SCD','GPCK','BN','SBDC','SBDF','SBDFP','SBDG','SBD','BPCK']
    #tty_list = ['SCD', 'SBD', 'GPCK', 'BPCK']
    #tty_list = ['BPCK']
    rxcui_list = get_rxcuis(tty_list)
    logging.info(f"Fetched {len(rxcui_list)} RXCUIs.")

    # 1.5. Create list of urls
    url_list = create_url_list(rxcui_list)

    results = get_concurrent_api_results(url_list)

    classes = {}
    for result in results:
        response = result['response']
        if 'rxclassDrugInfoList' in response:
            for drug_info in response["rxclassDrugInfoList"]["rxclassDrugInfo"]:
                rxcui = drug_info["minConcept"].get("rxcui")
                class_info = {}
                class_info['class_id'] = drug_info["rxclassMinConceptItem"].get("classId","")
                class_info['class_name'] = drug_info["rxclassMinConceptItem"].get("className","")
                class_info['class_type'] = drug_info["rxclassMinConceptItem"].get("classType","")
                class_info["drug_name"] = drug_info["minConcept"].get("name","")
                class_info["drug_tty"] = drug_info["minConcept"].get("tty","")
                class_info["rela"] = drug_info.get("rela","")
                class_info["rela_source"] = drug_info.get("relaSource","")            

                classes[rxcui] = class_info

    df = pd.DataFrame.from_dict(classes, orient='index')
    df.index.names = ['rxcui']
    print(f'Dataframe created of {len(df)} length.')
    load_df_to_pg(df.reset_index(),"sagerx_lake","rxclass","replace",index=False)


    '''
        class_data = cur_json['rxclassDrugInfoList']['rxclassDrugInfo']
        # Return a list of merged data dicts
        return [
            dict(
                concept,
                class_data=k['rxclassMinConceptItem'],
                rela=k.get('rela'),
                relaSource=k.get('relaSource')
            )
            for k in class_data
        ]
    '''

    '''
    # 3. Flatten the list and track failures
    successful_results = [
        item for sublist in results if sublist is not None for item in sublist
    ]
    failed_concepts = [
        concept for concept, result in zip(concepts, results) if result is None
    ]

    # 4. Create DataFrame
    df = pd.DataFrame(successful_results)
    if df.empty:
        logging.warning("No data retrieved. DataFrame is empty.")
        return False  # Might raise an exception if you'd prefer to fail

    # 5. Normalize class data
    c_df = json_normalize(df["class_data"])
    full_df = (
        pd.concat([df.drop(columns=["class_data"]), c_df], axis=1)
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # 6. Log summary
    processed_concepts = len(set(df["rxcui"]))
    failed_count = len(failed_concepts)
    logging.info(
        "Processing complete. "
        f"Total: {total_concepts}, Processed: {processed_concepts}, Failed: {failed_count}"
    )

    # 9. Load data to Postgres
    logging.info("Loading data to Postgres")
    load_df_to_pg(full_df, "sagerx_lake", "rxclass", "replace")
    logging.info(f"Successfully loaded {len(full_df)} rows into 'rxclass' table.")

    logging.info("Done.")
    return True
    '''


@task
def extract_atc(rxcui_list:list)->None:
   # Get ATC for full list of RXCUI
    urls = create_url_list(rxcui_list)
    print(f"URL List created of length: {len(urls)}")
    atcs_list = parallel_api_calls(urls)
    atcs = {}

    for atc in atcs_list:
        for druginfo in atc['response']["rxclassDrugInfoList"]["rxclassDrugInfo"]:
            rxcui = druginfo["minConcept"].get("rxcui")
            atc_info = {}
            atc_info['class_id'] = druginfo["rxclassMinConceptItem"].get("classId","")
            atc_info['class_name'] = druginfo["rxclassMinConceptItem"].get("className","")
            atc_info['class_type'] = druginfo["rxclassMinConceptItem"].get("classType","")
            atc_info["drug_name"] = druginfo["minConcept"].get("name","")
            atc_info["drug_tty"] = druginfo["minConcept"].get("tty","") #
            atc_info["rela"] = druginfo["minConcept"].get("rela","")
            atc_info["rela_source"] = druginfo["minConcept"].get("relaSource","")            

            atcs[rxcui] = atc_info

    atc_df = pd.DataFrame.from_dict(atcs, orient='index')
    atc_df.index.names = ['rxcui']
    load_df_to_pg(atc_df.reset_index(),"sagerx_lake","rxclass_atc_to_product","replace",index=False)
