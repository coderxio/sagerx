import pendulum
from airflow_operator import create_dag
from common_dag_tasks import  extract,transform, check_num_rows, load_df_to_pg, get_ds_folder
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
    url = "https://download.open.fda.gov/drug/enforcement/drug-enforcement-0001-of-0001.json.zip"
    ds_folder = get_ds_folder(dag_id)

    extract_task = extract(dag_id,url)
    
    extract_task = fda_enf_extract()
    load_task = load_df_to_pg(extract_task)
    transform_staging_task = transform.override(task_id='transform-staging')(dag_id)
    transform_intermediate_task = transform.override(task_id='transform-intermediate')(dag_id,'intermediate')

    test_contains_data = ShortCircuitOperator(
        task_id = 'test_contains_data',
        python_callable = check_num_rows,
        op_kwargs = {"num_rows":load_task}
    )

    extract_task >> load_task >> test_contains_data >> transform_staging_task  >> transform_intermediate_task