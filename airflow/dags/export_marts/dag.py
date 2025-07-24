import sqlalchemy
import pandas as pd

from os import environ
from airflow_operator import create_dag
from airflow.decorators import dag,task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.subprocess import SubprocessHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable



dag = create_dag(
    dag_id="export_marts",
    schedule = "0 7 * * 2", #every tuesday at 7:00am
    catchup=False,
    concurrency=2
)

with dag:

    @task
    def export_marts():
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = pg_hook.get_sqlalchemy_engine()
        marts_list = ["all_ndc_descriptions","atc_codes_to_rxnorm_products","all_ndcs_to_sources","products_to_inactive_ingredients","products","brand_products_with_related_ndcs"]
        mart_dfs={}
        with engine.connect() as connection:
            for mart in marts_list:
                if sqlalchemy.inspect(engine).has_table(mart, schema='sagerx_dev'):  
                    print(f'{mart} exists and will be exported')
                    df = pd.read_sql(f"SELECT * FROM sagerx_dev.{mart};", con=connection)
                    mart_dfs[mart] = df

        # get S3 destination from .env file, if any
        dest_bucket = environ.get("AWS_DEST_BUCKET")

        for k in list(mart_dfs.keys()):
            print(f'putting {k}')
            if dest_bucket != '': # if bucket is specified, write to bucket
                #mart_dfs[k].to_csv(dest_bucket+f'/{k}.csv', index=False) # if you want CSV
                mart_dfs[k].to_parquet(dest_bucket+f'/{k}.parquet', index=False)
                #mart_dfs[k].to_csv('/opt/airflow/exports/'+f'{k}.csv', index=False) # if you want CSV
                mart_dfs[k].to_parquet('/opt/airflow/exports/'+f'{k}.parquet', index=False)
            else:
                #mart_dfs[k].to_csv('/opt/airflow/exports/'+f'{k}.csv', index=False) # if you want CSV
                mart_dfs[k].to_parquet('/opt/airflow/exports/'+f'{k}.parquet', index=False)

    export_marts()
