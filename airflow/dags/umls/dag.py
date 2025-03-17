import pendulum
from airflow.decorators import dag
from umls.dag_tasks import extract, load
from common_dag_tasks import transform

dag_id = "umls"

@dag(
    dag_id=dag_id,
    schedule_interval="0 3 15 * *",  # Runs on the 15th of each month at 3 AM
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False
)
def umls():
    extract_task = extract(dag_id)
    load_task = load(extract_task)
    transform_task = transform(dag_id, models_subdir=['intermediate'])

    extract_task >> load_task >> transform_task

dag = umls()