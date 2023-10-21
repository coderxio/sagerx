import pendulum
from airflow_operator import create_dag
from airflow.dags.common_dag_tasks import  extract, load, transform, check_num_rows
from dag_tasks import extract_load
from airflow.operators.python import ShortCircuitOperator

dag_id = "fda_enforcement"
url= "https://www.accessdata.fda.gov/cder/ndctext.zip"

dag = create_dag(
    dag_id=dag_id,
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2012, 1, 1),
    max_active_runs=1,
    concurrency=2,
)

test_contains_data = ShortCircuitOperator(
        task_id = 'test_contains_data',
        python_callable = check_num_rows
    )

with dag:
    extract_task = extract_load()
    transform_task = transform(dag_id)

    extract_task >> test_contains_data >> transform_task