import pendulum
from airflow.decorators import dag
from openFDA.dag_tasks import extract, load

dag_id = "openfda_pregnancy_category"

@dag(
    dag_id=dag_id,
    schedule_interval="0 3 15 * *",
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False
)
def openfda():
    extract_task = extract(dag_id)
    load_task = load(extract_task)
    
    extract_task >> load_task

# Instantiate the DAG
dag = openfda()