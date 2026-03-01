from airflow.decorators import task
import pandas as pd
from sagerx import load_df_to_pg
import glob
import os

@task
def load(file_path_str:str):
    # Find the matching file
    pattern = os.path.join(file_path_str, "*ASP NDC-HCPCS Crosswalk*.xls*")
    matching_files = glob.glob(pattern)
    
    if not matching_files:
        raise FileNotFoundError(f"No matching NDC-HCPCS Crosswalk file found in {file_path_str}")
    
    # Use the first matching file
    file_path = matching_files[0]
    df = pd.read_excel(file_path, header=8)  # Skip first 8 rows, use row 9 as header
    # convert all column names to snake_case
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    load_df_to_pg(df,"sagerx_lake","cms_ndc_hcpcs_crosswalk","replace",index=False)
    