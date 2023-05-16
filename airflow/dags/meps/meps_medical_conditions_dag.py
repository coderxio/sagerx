from pathlib import Path
import pendulum

from sagerx import get_dataset, read_sql_file, get_sql_list, alert_slack_channel

from airflow.decorators import dag, task

from airflow.operators.python import get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.subprocess import SubprocessHook


@dag(
    schedule="0 4 * * *",
    start_date=pendulum.today(),
    catchup=False,
)
def meps_medical_conditions():
    col_names = ["DUID","PID","DUPERSID","CONDN","CONDIDX","PANEL","CONDRN","AGEDIAG","CRND1","CRND2","CRND3","CRND4","CRND5","INJURY","ACCDNWRK","ICD10CDX","CCSR1X","CCSR2X","CCSR3X","HHNUM","IPNUM","OPNUM","OBNUM","ERNUM","RXNUM","PERWT18F","VARSTR","VARPSU"]
    col_spaces = [(0,7),(7,10),(10,20),(20,23),(23,36),(36,38),(38,39),(39,42),(42,44),(44,46),(46,47),(47,49),(49,51),(51,52),(52,55),(55,58),(58,64),(64,70),(70,76),(76,78),(78,80),(80,83),(83,86),(86,88),(88,90),(90,102),(102,106),(106,107)]
    dag_id = "meps_medical_conditions"
    filename = "h207"
    ds_url = f"https://meps.ahrq.gov/mepsweb/data_files/pufs/{filename}/{filename}dat.zip"

    # Task to download data from web location
    @task
    def extract():
        data_folder = Path("/opt/airflow/data") / dag_id
        data_path = get_dataset(ds_url, data_folder)
        return data_path
    
    @task
    def load(data_path):
        import pandas as pd
        import sqlalchemy

        '''
        df = pd.read_excel(data_path + f'/{filename}.dat')
        '''

        df = pd.read_fwf(
            data_path + f'/{filename}.dat',
            header=None,
            names=col_names,
            converters={col: str for col in col_names},
            colspecs=col_spaces
        )

        # converting columns to lowercase so we don't have to put quotes around everything in postgres
        df.columns = df.columns.str.lower()

        print(df.head(10))

        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = pg_hook.get_sqlalchemy_engine()

        df.to_sql(
            dag_id,
            con=engine,
            schema="datasource",
            if_exists="replace",
            index=False
        )

    load(extract())

meps_medical_conditions()
