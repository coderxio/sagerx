import pendulum
from airflow.decorators import dag
from umls.dag_tasks import extract, load

dag_id = "rxnorm_to_icd"

@dag(
    dag_id=dag_id,
    schedule_interval="0 3 15 * *",  # Runs on the 15th of each month at 3 AM
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False
)
def rxnorm_to_icd():
    extract_task = extract(dag_id)
    load_task = load(extract_task)

    extract_task >> load_task

dag = rxnorm_to_icd()