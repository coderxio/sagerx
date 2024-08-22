from airflow_operator import create_dag
from common_dag_tasks import  extract, generate_sql_list, get_ds_folder, transform, read_sql_file
from dailymed_daily.dag_tasks import process_dailymed, unzip_data, process_dailymed_images
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
logging.getLogger("airflow.task.operators.postgres_operator").setLevel(logging.WARNING)

dag_id = "dailymed_daily"

dag = create_dag(
    dag_id=dag_id,
    schedule= "45 7 * * 1-5",  # at 7:45 am once daily
    max_active_runs=1,
    concurrency=2,
)


with dag:
    file_name = "{{ macros.ds_format(macros.ds_add(ds,-1), '%Y-%m-%d', '%m%d%Y') }}"
    #url = f"https://dailymed-data.nlm.nih.gov/public-release-files/dm_spl_daily_update_{file_name}.zip"
    url = "https://dailymed-data.nlm.nih.gov/public-release-files/dm_spl_release_human_rx_part1.zip"
    
    ds_folder = get_ds_folder(dag_id)

    extract_task = extract(dag_id,url)

    unzip = unzip_data(extract_task)
    process = process_dailymed(extract_task)
    process_images = process_dailymed_images(extract_task)
    # transform_task = transform(dag_id)

    # sql_tasks = []
    # for sql in generate_sql_list(dag_id):
    #     sql_path = ds_folder / sql
    #     task_id = sql[:-4] #remove .sql
    #     sql_task = PostgresOperator(
    #         task_id=task_id,
    #         postgres_conn_id="postgres_default",
    #         sql=read_sql_file(sql_path).format(data_path=extract_task),
    #         dag=dag
    #     )
    #     sql_tasks.append(sql_task)

    extract_task >> unzip >> process >> process_images 
    #>> sql_tasks >> transform_task