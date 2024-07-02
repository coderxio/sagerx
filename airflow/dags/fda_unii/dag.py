import pendulum

from airflow_operator import create_dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task

from common_dag_tasks import  extract, transform, generate_sql_list, get_ds_folder
from sagerx import read_sql_file

dag_id = "fda_unii"

dag = create_dag(
    dag_id=dag_id,
    schedule="0 4 * * *",
    start_date=pendulum.yesterday(),
    catchup=False,
    concurrency=2,
)

with dag:
    url= "https://precision.fda.gov/uniisearch/archive/latest/UNII_Data.zip"
    ds_folder = get_ds_folder(dag_id)

    extract_task = extract(dag_id,url)
    transform_task = transform(dag_id)

    @task
    def get_file_name(data_path) -> str:
        import re
        import os
        import logging

        logging.info(f'Data path: {data_path}')

        file_name = ''
        # note: extract_task contains the path to /opt/data/fda_unii/UNII_Data/
        # example file_name: UNII_Records_22Jun2024.txt
        for subfile in os.listdir(data_path):
            if re.match("UNII_Records", subfile):
                file_name = subfile
        
        if file_name == '':
            logging.error('Could not find file_name.')

        return file_name
    
    file_name_task = get_file_name(extract_task)
        
    sql_tasks = []
    for sql in generate_sql_list(dag_id):
        sql_path = ds_folder / sql
        task_id = sql[:-4] #remove .sql
        sql_task = PostgresOperator(
            task_id=task_id,
            postgres_conn_id="postgres_default",
            sql=read_sql_file(sql_path).format(
                data_path=extract_task,
                file_name=file_name_task
            ),
            dag=dag
        )
        sql_tasks.append(sql_task)
        
    file_name_task >> sql_tasks >> transform_task
