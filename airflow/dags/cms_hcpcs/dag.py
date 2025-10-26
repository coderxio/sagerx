import pendulum

from airflow_operator import create_dag
from airflow.providers.postgres.operators.postgres import PostgresOperator

from common_dag_tasks import extract, transform, generate_sql_list, get_ds_folder
from cms_hcpcs.dag_tasks import load
from sagerx import read_sql_file

dag_id = "cms_hcpcs"

dag = create_dag(
    dag_id=dag_id,
    schedule="0 4 * * *",
    start_date=pendulum.yesterday(),
    catchup=False,
    concurrency=2,
)

with dag:
    url= "https://www.cms.gov/files/zip/october-2025-alpha-numeric-hcpcs-file.zip"
    ds_folder = get_ds_folder(dag_id)

    extract_task = extract(dag_id,url)
    load_task = load(extract_task)
    transform_task = transform(dag_id)
      
    extract_task >> load_task >> transform_task
