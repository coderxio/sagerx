from pathlib import Path
from sagerx import get_dataset, read_sql_file, get_sql_list
from airflow.decorators import task

def get_ds_folder(dag_id):
    return Path("/opt/airflow/dags") / dag_id

def generate_sql_list(dag_id, sql_prefix='load') -> list:
    ds_folder = get_ds_folder(dag_id)
    return get_sql_list(sql_prefix, ds_folder)

def get_ordered_sql_tasks(dag_id):
    tasks = []
    tasks.extend(generate_sql_list(dag_id,'load'))
    tasks.extend(generate_sql_list(dag_id,'staging'))
    tasks.extend(generate_sql_list(dag_id,'view'))
    tasks.extend(generate_sql_list(dag_id,'api'))
    tasks.extend(generate_sql_list(dag_id,'alter'))
    return tasks

def url_request(url):
    import requests
    try:
        response = requests.get(url)
    except Exception as e:
        raise e

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        print(f"Response Status Code: {response.status_code}")
        print(f"Response Text: {response.text}")
        raise response
    return response

@task
def extract(dag_id,url) -> str:
    # Task to download data from web location

    data_folder = Path("/opt/airflow/data") / dag_id
    data_path = get_dataset(url, data_folder)
    print(f"Extraction Completed! Data saved in folder: {data_folder}")
    return data_path


@task
def transform(dag_id, models_subdir='staging',task_id="") -> None:
    # Task to transform data using dbt
    from airflow.hooks.subprocess import SubprocessHook

    subprocess = SubprocessHook()
    result = subprocess.run_command(['dbt', 'run', '--select', f'models/{models_subdir}/{dag_id}'], cwd='/dbt/sagerx')
    print("Result from dbt:", result)


@task
def check_num_rows(num_rows):
    return num_rows > 0

@task
def load_df_to_pg(df):
    from airflow.hooks.postgres_hook import PostgresHook
    import sqlalchemy

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg_hook.get_sqlalchemy_engine()

    num_rows = df.to_sql(
        "fda_enforcement",
        con=engine,
        schema="datasource",
        if_exists="append",
        dtype={"openfda": sqlalchemy.types.JSON},
    )

    return num_rows

@task
def csv_haircut(data_path,rows_to_skip):
    import pandas as pd
    df = pd.read_csv(data_path,skiprows=rows_to_skip)
    df.to_csv(data_path, index=False)