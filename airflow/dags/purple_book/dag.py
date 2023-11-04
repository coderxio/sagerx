from airflow_operator import create_dag
from airflow.utils.helpers import chain

from common_dag_tasks import  extract, get_ordered_sql_tasks, get_ds_folder
from sagerx import read_sql_file
from airflow.providers.postgres.operators.postgres import PostgresOperator


dag_id = "purple_book"

dag = create_dag(
    dag_id=dag_id,
    schedule= "15 0 24 1 *",  # runs once monthly on the 23rd
    max_active_runs=1,
    concurrency=2,
)

with dag:
    url = "https://purplebooksearch.fda.gov/files/{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y') }}/purplebook-search-{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%B').lower() }}-data-download.csv"
    ds_folder = get_ds_folder(dag_id)

    extract_task = extract(dag_id,url)

    task_list = [extract_task]
    for sql in get_ordered_sql_tasks(dag_id):
        sql_path = ds_folder / sql
        task_id = sql[:-4] #remove .sql

        sql_task = PostgresOperator(
            task_id=task_id,
            postgres_conn_id="postgres_default",
            sql=read_sql_file(sql_path).format(data_path=extract_task),
            dag=dag
        )
        task_list.append(sql_task)
    
    chain(*task_list) 
   