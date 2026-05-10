import pendulum
from airflow.decorators import dag
from open_fda.dag_tasks import extract, load

dag_id = "open_fda_pregnancy_category"

@dag(
    dag_id=dag_id,
    schedule_interval="0 3 15 * *",
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False
)
def open_fda():
    extract_task = extract(dag_id)
    load_task = load(extract_task)
    
    extract_task >> load_task

# Instantiate the DAG
dag = open_fda()