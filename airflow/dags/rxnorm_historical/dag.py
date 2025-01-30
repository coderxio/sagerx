import pendulum

from airflow.decorators import dag

from rxnorm_historical.dag_tasks import extract, load


dag_id = "rxnorm_historical"

@dag(
    dag_id=dag_id,
    schedule_interval="0 3 15 * *",  # Runs on the 15th of each month at 3 AM
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False
)
def rxnorm_historical():
    # Main processing task
    extract(dag_id) >> load(dag_id)
    
# Instantiate the DAG
dag = rxnorm_historical()
