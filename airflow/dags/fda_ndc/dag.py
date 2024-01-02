import pendulum

from airflow_operator import create_dag
from airflow.providers.postgres.operators.postgres import PostgresOperator

from common_dag_tasks import  extract, transform, generate_sql_list, get_ds_folder
from sagerx import read_sql_file

dag_id = "fda_ndc"

dag = create_dag(
    dag_id=dag_id,
    schedule="0 4 * * *",
    start_date=pendulum.yesterday(),
    catchup=False,
    concurrency=2,
)

with dag:
    url= "https://www.accessdata.fda.gov/cder/ndctext.zip"
    ds_folder = get_ds_folder(dag_id)

    extract_task = extract(dag_id,url)
    transform_task = transform(dag_id)

    for sql in generate_sql_list(dag_id):
        sql_path = ds_folder / sql
        task_id = sql[:-4] #remove .sql
        sql_task = PostgresOperator(
            task_id=task_id,
            postgres_conn_id="postgres_default",
            sql=read_sql_file(sql_path).format(data_path=extract_task),
            dag=dag
        )
        extract_task >> sql_task >> transform_task