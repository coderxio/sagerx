import sqlalchemy
import pandas as pd
import boto3

from io import StringIO
from airflow_operator import create_dag
from airflow.decorators import dag,task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.subprocess import SubprocessHook
from airflow.hooks.postgres_hook import PostgresHook


dag = create_dag(
    dag_id="export_dag",
    #every 5 days
    catchup=False,
    concurrency=2
)

with dag:

    @task
    def export_tables():
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = pg_hook.get_sqlalchemy_engine()
        marts_list = ["all_ndc_descriptions","atc_codes_to_rxnorm_products"]

        mart_dfs=[]
        with engine.connect() as connection:
            for mart in marts_list:
                if sqlalchemy.inspect(engine).has_table(mart, schema='flatfile'):  
                    print(f'{mart} exists and will be exported')
                    df = pd.read_sql(f"SELECT * FROM flatfile.{mart};", con=connection)
                    mart_dfs.append(df)
                    
        # s3_resource = boto3.resource('s3',
        #     aws_access_key_id = '',
        #     aws_secret_access_key = '' 
        # )    
        sts = boto3.client('sts')
        response = sts.assume_role( 
            RoleArn='aws:arn:iam::181615070735:role/miles_IAM_user',
            RoleSessionName='airflow-export-session',
            #SourceIdentity='default',
            #aws_conn_id='aws_default',
            DurationSeconds=900
        )

        s3_resource = boto3.resource(
            's3',
            aws_access_key_id= response['Credentials']['AccessKeyId'],
            aws_secret_access_key= response['Credentials']['SecretAccessKey'],
            aws_session_token= response['Credentials']['SessionToken']
        )

        for df in mart_dfs:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)

            s3_resource.Object('miles-sagerx', f'{mart}.csv').put(Body=csv_buffer.getvalue())

    export = export_tables()    

