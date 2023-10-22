from airflow.decorators import task

# Task to download data from web location
@task(task_id='extract')
def fda_enf_extract(data_interval_start=None, data_interval_end=None):
    import requests
    import pandas as pd
    import logging

    start_date = data_interval_start.format("YYYYMMDD")
    end_date = data_interval_end.format("YYYYMMDD")

    url = f"https://api.fda.gov/drug/enforcement.json?search=report_date:[{start_date}+TO+{end_date}]&limit=1000"
    logging.info(url)

    response = requests.get(url)
    json_object = response.json()["results"]

    df = pd.DataFrame(json_object)
    df.set_index("recall_number")

    return df