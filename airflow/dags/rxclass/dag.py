import pendulum

from airflow.decorators import dag

from rxclass.dag_tasks import extract, load

from common_dag_tasks import transform


dag_id = "rxclass"

@dag(
    dag_id=dag_id,
    schedule_interval="0 3 15 * *",  # Runs on the 15th of each month at 3 AM
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False
)
def rxclass():
    # Main processing task
    extract_task = extract(dag_id)
    load_task = load(extract_task)
    transform_task = transform(dag_id, models_subdir=['staging', 'intermediate'])
    
    extract_task >> load_task >> transform_task

# Instantiate the DAG
dag = rxclass()
