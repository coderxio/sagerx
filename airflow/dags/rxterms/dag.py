from airflow_operator import create_dag
from airflow.utils.helpers import chain

from common_dag_tasks import  extract, get_ordered_sql_tasks, get_ds_folder, transform
from sagerx import read_sql_file
from airflow.providers.postgres.operators.postgres import PostgresOperator


dag_id = "rxterms"

dag = create_dag(
    dag_id=dag_id,
    schedule= "45 0 15 1 *",  # runs once monthly on the 15th day at 00:45
    max_active_runs=1,
    concurrency=2,
)   

with dag:
    mnth = "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m' ) }}"
    url = f"https://data.lhncbc.nlm.nih.gov/public/rxterms/release/RxTerms{mnth}.zip"
    ds_folder = get_ds_folder(dag_id)

    extract_task = extract(dag_id,url)
    transform_task = transform(dag_id)

    sql_tasks = []
    for sql in get_ordered_sql_tasks(dag_id):
        sql_path = ds_folder / sql
        task_id = sql[:-4] #remove .sql

        sql_task = PostgresOperator(
            task_id=task_id,
            postgres_conn_id="postgres_default",
            sql=read_sql_file(sql_path).format(data_path=extract_task, mnth=mnth),
            dag=dag
        )
        sql_tasks.append(sql_task)
    
    extract_task >> sql_tasks >> transform_task
   