from pathlib import Path
from sagerx import get_dataset, read_sql_file, get_sql_list
from airflow.decorators import task
from airflow.models import DagRun

def get_ds_folder(dag_id):
    return Path("/opt/airflow/dags") / dag_id

def get_data_folder(dag_id):
    return Path("/opt/airflow/data") / dag_id

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

def url_request(url,param=None,headers=None):
    import requests
    try:
        response = requests.get(url,param,headers)
    except Exception as e:
        raise e

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        print(f"Response Status Code: {response.status_code}")
        print(f"Response Text: {response.text}")
        raise response
    return response

def get_most_recent_dag_run(dag_id):
    dag_runs = DagRun.find(dag_id=dag_id)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    return dag_runs[0] if dag_runs else None

def return_files_in_folder(dir_path) -> list:
    files = [] 
    for file_path in dir_path.iterdir():
        if file_path.is_file():
            print(file_path.name)
            files.append(file_path)
        elif file_path.is_dir():
            files.append(return_files_in_folder(file_path))
    return files

def get_files_in_data_folder(dag_id) -> list:
    final_list = []
    ds_path = get_data_folder(dag_id)
    file_paths = [file for file in ds_path.iterdir() if not file.name.startswith('.DS_Store')]

    for file_path in file_paths:
        final_list.extend(return_files_in_folder(file_path))

    return final_list

def txt2csv(txt_path):    
    import pandas as pd

    output_file =  txt_path.with_suffix('.csv')
    csv_table = pd.read_table(txt_path, sep='\t', encoding='cp1252')
    csv_table.to_csv(output_file, index=False)

    print(f"Conversion complete. The CSV file is saved as {output_file}")
    return output_file

def upload_csv_to_gcs(dag_id):
    from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
    from os import environ

    gcp_tasks = []
    files = get_files_in_data_folder(dag_id)

    for file_path in files:
        if file_path.suffix == '.txt':
            csv_file_path = txt2csv(file_path)

            gcp_task = LocalFilesystemToGCSOperator(
                task_id=f'upload_to_gcs_{csv_file_path.name}',
                src=str(csv_file_path),
                dst=f"{dag_id}/{csv_file_path.name}",
                bucket=environ.get("GCS_BUCKET"),
                gcp_conn_id='google_cloud_default'
            )
            gcp_tasks.append(gcp_task)
    return gcp_tasks

@task
def extract(dag_id,url) -> str:
    # Task to download data from web location

    data_folder = get_data_folder(dag_id)
    data_path = get_dataset(url, data_folder)
    print(f"Extraction Completed! Data saved in folder: {data_folder}")
    return data_path


@task
def transform(dag_id, models_subdir='staging',task_id="") -> None:
    # Task to transform data using dbt
    from airflow.hooks.subprocess import SubprocessHook
    from airflow.exceptions import AirflowException

    subprocess = SubprocessHook()
    result = subprocess.run_command(['docker', 'exec', 'dbt','dbt', 'run', '--select', f'models/{models_subdir}/{dag_id}'], cwd='/dbt/sagerx')
    if result.exit_code != 0:
            raise AirflowException(f"Command failed with return code {result.exit_code}: {result.output}")
    print("Result from dbt:", result)