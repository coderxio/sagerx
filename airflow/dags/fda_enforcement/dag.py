import pendulum
from airflow_operator import create_dag
from common_dag_tasks import  extract,transform, get_ds_folder
from fda_enforcement.dag_tasks import  load_json
# from airflow.operators.python import ShortCircuitOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from sagerx import read_sql_file

dag_id = "fda_enforcement"

dag = create_dag(
    dag_id=dag_id,
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2012, 1, 1),
    max_active_runs=1,
    concurrency=2,
)


with dag:
    url = "https://download.open.fda.gov/drug/enforcement/drug-enforcement-0001-of-0001.json.zip"
    ds_folder = get_ds_folder(dag_id)
    file_name = "/drug-enforcement-0001-of-0001.json"

    extract_task = extract(dag_id,url)
    
    load_task = load_json(str(extract_task)+file_name)
    
    transform_staging_task = transform.override(task_id='transform-staging')(dag_id)
    transform_intermediate_task = transform.override(task_id='transform-intermediate')(dag_id,'intermediate')

    extract_task >> load_task >> transform_staging_task  >> transform_intermediate_task