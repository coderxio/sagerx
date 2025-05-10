from airflow.decorators import task
import pandas as pd
import requests
import re
import logging
from sagerx import load_df_to_pg, create_path, write_json_file, read_json_file
from common_dag_tasks import get_data_folder

def format_ndc_to_11_digits(ndc):
    # Remove any dashes if present and split into segments
    if '-' in ndc:
        segments = ndc.split('-')
    else:
        # Check if ndc that are 10 digits long
        if len(ndc) != 10:
            return ndc
            
        # Get the format (4-4-2, 5-3-2, or 5-4-1)
        if re.match(r'^\d{4}\d{4}\d{2}$', ndc):  # 4-4-2 format
            segments = [ndc[:4], ndc[4:8], ndc[8:]]
        elif re.match(r'^\d{5}\d{3}\d{2}$', ndc):  # 5-3-2 format
            segments = [ndc[:5], ndc[5:8], ndc[8:]]
        elif re.match(r'^\d{5}\d{4}\d{1}$', ndc):  # 5-4-1 format
            segments = [ndc[:5], ndc[5:9], ndc[9:]]
        else:
            return ndc 
    
    if len(segments) != 3:
        return ndc  # If not in the expected format, return as is
        
    # Add zero to the appropriate ndc segment
    if len(segments[0]) == 4:  # 4-x-x → 5-x-x
        segments[0] = '0' + segments[0]
    elif len(segments[1]) == 3:  # x-3-x → x-4-x
        segments[1] = '0' + segments[1]
    elif len(segments[2]) == 1:  # x-x-1 → x-x-2
        segments[2] = '0' + segments[2]
    
    # Return as formatted 11-digit NDC without dashes
    return ''.join(segments)

@task
def extract(dag_id: str) -> str:
    """
    Extracts drug pregnancy category data from the openFDA API
    """
    logging.info("Starting data retrieval for FDA drug pregnancy categories...")
    
    API_BASE = "https://api.fda.gov/drug/label.json"
    
    # Search for drug labels with teratogenic effects information, this is where the pregnancy category is
    search_param = "_exists_:teratogenic_effects"
    
    params = {
        "search": search_param,
        "limit": 1000  # Maximum allowed per request
    }
    
    all_data = []
    total_processed = 0
    
    try:
        # Get total number of records
        initial_resp = requests.get(API_BASE, params=params)
        initial_resp.raise_for_status()
        initial_data = initial_resp.json()
        
        # Get total count of matching records
        total_records = initial_data.get("meta", {}).get("results", {}).get("total", 0)
        logging.info(f"Found {total_records} total records with teratogenic effects. Processing...")
        
        # Process records in batches of 1000 (API limit)
        skip = 0
        while total_processed < total_records:
            # Update params with current skip value
            params["skip"] = skip
            
            resp = requests.get(API_BASE, params=params)
            resp.raise_for_status()
            batch_data = resp.json()
            records = batch_data.get("results", [])
            
            if not records:
                break
            
            # Process each record in the batch
            for rec in records:
                open_fda = rec.get("openfda", {})
                
                # Skip records without package_ndc
                if "package_ndc" not in open_fda or not open_fda["package_ndc"]:
                    continue
                
                # Get rxcui if available. rxcui is not always available
                rxcui = None
                if "rxcui" in open_fda and open_fda["rxcui"]:
                    rxcui = open_fda["rxcui"][0]
                
                # Define regex pattern to extract pregnancy category
                pattern = r"(?:Pregnancy\s+)?Category\s+([ABCDX])\b"
                
                # Store just the pregnancy category letter
                pregnancy_category = None
                field_content = ""
                if "teratogenic_effects" in rec:
                    field_content = rec["teratogenic_effects"]
                    if isinstance(field_content, list):
                        field_content = " ".join(field_content)
                    
                    match = re.search(pattern, field_content, re.IGNORECASE)
                    if match:
                        pregnancy_category = match.group(1)

                # Skip records without a valid pregnancy category
                if not pregnancy_category:
                    continue
                
                # Get package_ndc values and create a row for each package_ndc
                for package_ndc in open_fda["package_ndc"]:
                    # Format the NDC
                    formatted_ndc = format_ndc_to_11_digits(package_ndc)
                    
                    all_data.append({
                        "ndc": formatted_ndc,
                        "rxcui": rxcui,
                        "pregnancy_category": pregnancy_category
                    })
            
            # Update counters for next batch
            total_processed += len(records)
            skip += len(records)
            logging.info(f"Processed {total_processed} of {total_records} records...")
        
        # Save results to JSON file
        data_folder = get_data_folder(dag_id)
        file_path = create_path(data_folder) / 'pregnancy_categories.json'
        file_path_str = file_path.resolve().as_posix()
        
        write_json_file(file_path_str, all_data)
        
        logging.info(f"Extraction Completed! Data saved to file: {file_path_str}")
        return file_path_str
        
    except requests.exceptions.HTTPError as err:
        logging.error(f"HTTP Error: {err}")
        # Save any data collected so far
        if all_data:
            data_folder = get_data_folder(dag_id)
            file_path = create_path(data_folder) / 'pregnancy_categories_partial.json'
            file_path_str = file_path.resolve().as_posix()
            write_json_file(file_path_str, all_data)
            return file_path_str
        raise

@task
def load(file_path_str: str):
    """
    Loads the pregnancy category data into the database.
    """
    logging.info(f"Loading pregnancy category data from {file_path_str}")
    
    data = read_json_file(file_path_str)
    
    df = pd.DataFrame(data)
    
    logging.info(f"Dataframe created with {len(df)} rows")
    
    load_df_to_pg(df, "sagerx_lake", "open_fda_pregnancy_categories", "replace", index=False)
    
    logging.info("Data successfully loaded into database")
    return "Data load complete"