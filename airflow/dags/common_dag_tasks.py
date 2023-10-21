from pathlib import Path
from sagerx import get_dataset, read_sql_file, get_sql_list
from airflow.decorators import task

@task
def extract(dag_id,url) -> str:
    # Task to download data from web location

    data_folder = Path("/opt/airflow/data") / dag_id
    data_path = get_dataset(url, data_folder)
    print(f"Extraction Completed! Data saved in folder: {data_folder}")
    return data_path

@task
def load(dag_id,data_path) -> None:
    # Task to load data into source db schema
    from airflow.providers.postgres.operators.postgres import PostgresOperator

    load = []
    ds_folder = Path("/opt/airflow/dags") / dag_id
    for sql in get_sql_list("load", ds_folder):
        sql_path = ds_folder / sql
        task_id = sql[:-4] #remove .sql
        load.append(
            PostgresOperator(
                task_id=task_id,
                postgres_conn_id="postgres_default",
                sql=read_sql_file(sql_path).format(data_path=data_path),
            )
        )
    print(f"Loading of Data into PostGres Completed! Loaded {len(load)} tables")

@task
def transform(dag_id) -> None:
    # Task to transform data using dbt
    from airflow.hooks.subprocess import SubprocessHook

    subprocess = SubprocessHook()
    result = subprocess.run_command(['dbt', 'run', '--select', f'models/staging/{dag_id}'], cwd='/dbt/sagerx')
    print("Result from dbt:", result)


@task
def check_num_rows(num_rows):
    return num_rows > 0