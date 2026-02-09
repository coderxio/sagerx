import pendulum

from airflow_operator import create_dag
from airflow.providers.postgres.operators.postgres import PostgresOperator

from common_dag_tasks import extract, transform, generate_sql_list, get_ds_folder
from sagerx import read_sql_file

dag_id = "ncpdp"

dag = create_dag(
    dag_id=dag_id,
    schedule="0 4 * * 1",  # 4am every Monday (NCI EVS posts updates following last Monday of month)
    start_date=pendulum.yesterday(),
    catchup=False,
    concurrency=2,
)

with dag:
    url = "https://evs.nci.nih.gov/ftp1/NCPDP/NCPDP.txt"
    ds_folder = get_ds_folder(dag_id)

    extract_task = extract(dag_id, url)
    transform_task = transform(dag_id)

    sql_tasks = []
    for sql in generate_sql_list(dag_id):
        sql_path = ds_folder / sql
        task_id = sql[:-4]  # remove .sql
        sql_task = PostgresOperator(
            task_id=task_id,
            postgres_conn_id="postgres_default",
            sql=read_sql_file(sql_path).format(data_path=extract_task),
            dag=dag
        )
        sql_tasks.append(sql_task)

    extract_task >> sql_tasks >> transform_task
