import pendulum

from airflow_operator import create_dag

from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow.operators.python_operator import PythonOperator

dag_id = "dbt_gcp"

dag = create_dag(
    dag_id=dag_id,
    schedule="0 4 * * *",
    start_date=pendulum.yesterday(),
    catchup=False,
    concurrency=2,
)

with dag:
    def run_dbt():
        from airflow.hooks.subprocess import SubprocessHook
        from airflow.exceptions import AirflowException

        subprocess = SubprocessHook()

        run_results = subprocess.run_command(['docker','exec','dbt','dbt', 'run'], cwd='/dbt/sagerx')
        if run_results.exit_code != 1:
            raise AirflowException(f"Command failed with return code {run_results.exit_code}: {run_results.output}")
        print(f"Command succeeded with output: {run_results.output}")

        docs_results = subprocess.run_command(['docker','exec','dbt','dbt', "docs", "generate"], cwd='/dbt/sagerx')
        if docs_results.exit_code != 0:
            raise AirflowException(f"Command failed with return code {docs_results.exit_code}: {docs_results.output}")
        print(f"Command succeeded with output: {docs_results.output}")

        check_results = subprocess.run_command(['docker','exec','dbt','dbt', 'run-operation', 'check_data_availability'], cwd='/dbt/sagerx')
        if check_results.exit_code != 0:
            raise AirflowException(f"Command failed with return code {check_results.exit_code}: {check_results.output}")
        print(f"Command succeeded with output: {check_results.output}")
            

    def get_dbt_models():
        from sagerx import run_query_to_df

        df = run_query_to_df("""
            SELECT * 
            FROM sagerx.data_availability
            WHERE has_data =  True
            AND materialized != 'view'
            """)

        print(f"Final list: {df}")
        return df.to_dict('index')
    
    def load_to_gcs(run_dbt_task):
        
        dbt_models = get_dbt_models()
        gcp_tasks = []
        for _,row_values in dbt_models.items():
            schema_name = row_values.get('schema_name')
            table_name =  row_values.get('table_name')
            columns_info = row_values.get('columns_info')

            print(f"{schema_name}.{table_name}")
            print(columns_info)

            # Transfer data from Postgres DB to Google Cloud Storage DB
            p2g_task = PostgresToGCSOperator(
                task_id=f'postgres_to_gcs_{schema_name}.{table_name}',
                postgres_conn_id='postgres_default',  
                sql=f"SELECT * FROM {schema_name}.{table_name}",
                bucket="sagerx_bucket", 
                filename=f'{table_name}', 
                export_format='csv', 
                gzip=False,  
                dag=dag,
            )

            # Populate BigQuery tables with data from Cloud Storage Bucket
            cs2bq_task = GCSToBigQueryOperator(
                task_id=f"bq_load_{schema_name}.{table_name}",
                bucket="sagerx_bucket",
                source_objects=[f"{table_name}"],
                destination_project_dataset_table=f"sagerx-420700.sagerx_lake.{table_name}",
                autodetect = True,
                external_table=False,
                create_disposition= "CREATE_IF_NEEDED", 
                write_disposition= "WRITE_TRUNCATE", 
                gcp_conn_id = 'google_cloud_default', 
                location='us-east5',
                dag = dag,
            )


            run_dbt_task.set_downstream(p2g_task)
            p2g_task.set_downstream(cs2bq_task)

            gcp_tasks.append(p2g_task)
            gcp_tasks.append(cs2bq_task)

        return gcp_tasks


    run_dbt_task = PythonOperator(
        task_id='run_dbt',
        python_callable=run_dbt,
        dag=dag,
    )

    gcp_tasks = load_to_gcs(run_dbt_task)


"""
Notes:
- Scaling issue if each table contains multiple tasks, can quickly grow since it grows exponentially 
- Single dbt run is the preferred method since the complexity of dependencies is handled once 
- If we can have dbt full run after each dag run and then only specify which tables to upload that would make it more targeted and less costly 
"""
