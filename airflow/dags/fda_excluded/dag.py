import pendulum

from airflow_operator import create_dag
from airflow.utils.helpers import chain

from common_dag_tasks import  extract, transform, get_ordered_sql_tasks, get_ds_folder
from sagerx import read_sql_file
from airflow.decorators import dag,task
from airflow.providers.postgres.operators.postgres import PostgresOperator


dag_id = "fda_excluded"

dag = create_dag(
    dag_id=dag_id,
    schedule= "0 4 * * *",  # run at 4am every day
    start_date=pendulum.yesterday(),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
)

@task(task_id="target_dag")
def run_this_func(dag_run=None):
    print(f"Remotely received value of {dag_run.conf.get('message')} for key=message")

with dag:
    url = "https://www.accessdata.fda.gov/cder/ndc_excluded.zip"
    ds_folder = get_ds_folder(dag_id)

    extract_task = extract(dag_id,url)
    transform_task = transform(dag_id)
    dag_message = run_this_func

    sql_tasks = []
    for sql in get_ordered_sql_tasks(dag_id):
        sql_path = ds_folder / sql
        task_id = sql[:-4] #remove .sql
        sql_task = PostgresOperator(
            task_id=task_id,
            postgres_conn_id="postgres_default",
            sql=read_sql_file(sql_path).format(data_path=extract_task),
            dag=dag
        )
        sql_tasks.append(sql_task)

    extract_task >> sql_tasks >> transform_task