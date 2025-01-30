import pendulum

from airflow.decorators import dag

from rxclass.dag_tasks import extract


dag_id = "rxclass"

@dag(
    dag_id=dag_id,
    schedule_interval="0 3 15 * *",  # Runs on the 15th of each month at 3 AM
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False
)
def rxclass():
    # Main processing task
    extract_task = extract()

    extract_task

# Instantiate the DAG
dag = rxclass()
