from airflow.decorators import task
import pandas as pd
from sagerx import load_df_to_pg
import glob
import os

@task
def load(file_path_str:str):
    # find the matching file
    pattern = os.path.join(file_path_str, "HCPC2026_JAN_ANWEB_01122026.xlsx")
    matching_files = glob.glob(pattern)
    
    if not matching_files:
        raise FileNotFoundError(f"No ASP Pricing File found in {file_path_str}")
    
    # use the first matching file
    file_path = matching_files[0]
    # skip first 8 rows, use row 9 as header
    df = pd.read_excel(file_path, header=8)
    # convert all column names to snake_case
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_').str.replace('%', '_percentage')
    load_df_to_pg(df,"sagerx_lake","cms_asp","replace",index=False)
    