from airflow_operator import create_dag
from airflow.utils.helpers import chain

from common_dag_tasks import  extract, get_ordered_sql_tasks, get_ds_folder
from sagerx import read_sql_file
from airflow.providers.postgres.operators.postgres import PostgresOperator
from purple_book.dag_tasks import modify_csv

dag_id = "purple_book"

dag = create_dag(
    dag_id=dag_id,
    schedule= "15 0 24 1 *",  # runs once monthly on the 23rd
    max_active_runs=1,
    concurrency=2,
)

with dag:
    file_name = "{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y') }}/purplebook-search-{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%B').lower() }}-data-download.csv"
    url = f"https://purplebooksearch.fda.gov/files/{file_name}"
    ds_folder = get_ds_folder(dag_id)

    extract_task = extract(dag_id,url)
    modify_task = modify_csv(extract_task)

    task_list = [extract_task,modify_task]

    for sql in get_ordered_sql_tasks(dag_id):
        sql_path = ds_folder / sql
        task_id = sql[:-4] #remove .sql

        sql_task = PostgresOperator(
            task_id=task_id,
            postgres_conn_id="postgres_default",
            sql=read_sql_file(sql_path).format(data_path=extract_task, file_name=file_name),
            dag=dag
        )
        task_list.append(sql_task)
    
    chain(*task_list) 
   