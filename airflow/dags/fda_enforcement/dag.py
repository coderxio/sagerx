import pendulum
from airflow_operator import create_dag
from common_dag_tasks import  transform, check_num_rows, load_df_to_pg
from fda_enforcement.dag_tasks import fda_enf_extract
from airflow.operators.python import ShortCircuitOperator

dag_id = "fda_enforcement"

dag = create_dag(
    dag_id=dag_id,
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2012, 1, 1),
    max_active_runs=1,
    concurrency=2,
)


with dag:
    extract_task = fda_enf_extract()
    load_task = load_df_to_pg(extract_task)
    transform_staging_task = transform(dag_id)
    transform_intermediate_task = transform(dag_id,'intermediate')

    test_contains_data = ShortCircuitOperator(
        task_id = 'test_contains_data',
        python_callable = check_num_rows,
        op_kwargs = {"num_rows":load_task}
    )

    extract_task >> load_task >> test_contains_data >> transform_staging_task  >> transform_intermediate_task