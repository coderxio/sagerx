import sqlalchemy
import pandas as pd
import boto3

from io import StringIO
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
        marts_list = ["all_ndc_descriptions","atc_codes_to_rxnorm_products"]
        mart_dfs={}
        with engine.connect() as connection:
            for mart in marts_list:
                if sqlalchemy.inspect(engine).has_table(mart, schema='sagerx_dev'):  
                    print(f'{mart} exists and will be exported')
                    df = pd.read_sql(f"SELECT * FROM sagerx_dev.{mart};", con=connection)
                    mart_dfs[mart] = df

        access_key = environ.get("AWS_ACCESS_KEY_ID")
        secret_key = environ.get("AWS_SECRET_ACCESS_KEY")
        dest_bucket = environ.get("AWS_DEST_BUCKET")

        s3_resource = boto3.resource(
            's3',
            aws_access_key_id= access_key,
            aws_secret_access_key= secret_key
        )

        for k in list(mart_dfs.keys()):
            print(f'putting {k}')
            csv_buffer = StringIO()
            mart_dfs[k].to_csv(csv_buffer)

            s3_resource.Object(dest_bucket, f'{k}.csv').put(Body=csv_buffer.getvalue())

    export_marts()
