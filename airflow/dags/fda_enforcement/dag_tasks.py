from airflow.decorators import task
from common_dag_tasks import url_request
from sagerx import read_json_file, load_df_to_pg

# Task to download data from web location
@task(task_id='extract')
def fda_enf_extract(data_interval_start=None, data_interval_end=None):
    import pandas as pd
    import logging

    start_date = data_interval_start.format("YYYYMMDD")
    end_date = data_interval_end.format("YYYYMMDD")
    print(f"Start date: {start_date}, End date: {end_date}")

    url = f"https://api.fda.gov/drug/enforcement.json?search=report_date:[{start_date}+TO+{end_date}]&limit=1000"
    logging.info(url)

    response = url_request(url)

    json_object = response.json()["results"]

    df = pd.DataFrame(json_object)
    df.set_index("recall_number")

    return df

@task
def load_json(data_path):
    import pandas as pd
    print(f"JSON path: {data_path}")
    json_object = read_json_file(data_path)
    df = pd.DataFrame(json_object["results"])
    df.set_index("recall_number")
    print(f"Dataframe loaded. Number of rows: {len(df)}")
    load_df_to_pg(df,"sagerx_lake","fda_enforcement","replace",dtype_name="openfda")
