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
def meps_prescribed_medications():
    col_names = ['DUID', 'PID', 'DUPERSID', 'DRUGIDX', 'RXRECIDX', 'LINKIDX','PANEL', 'PURCHRD', 'RXBEGMM', 'RXBEGYRX', 'RXNAME',
                'RXDRGNAM', 'RXNDC', 'RXQUANTY', 'RXFORM', 'RXFRMUNT','RXSTRENG', 'RXSTRUNT', 'RXDAYSUP', 'PHARTP1', 'PHARTP2',
                'PHARTP3', 'PHARTP4', 'PHARTP5', 'PHARTP6', 'PHARTP7','PHARTP8', 'PHARTP9', 'RXFLG', 'IMPFLAG', 'PCIMPFLG',
                'DIABEQUIP', 'INPCFLG', 'PREGCAT', 'TC1', 'TC1S1','TC1S1_1', 'TC1S1_2', 'TC1S2', 'TC1S2_1', 'TC1S3',
                'TC1S3_1', 'TC2', 'TC2S1', 'TC2S1_1', 'TC2S1_2', 'TC2S2','TC3', 'TC3S1', 'TC3S1_1', 'RXSF18X', 'RXMR18X', 'RXMD18X',
                'RXPV18X', 'RXVA18X', 'RXTR18X', 'RXOF18X', 'RXSL18X','RXWC18X', 'RXOT18X', 'RXOR18X', 'RXOU18X', 'RXXP18X',
                'PERWT18F', 'VARSTR', 'VARPSU']

    col_spaces = [(0,7),(7,10),(10,20),(20,33),(33,52),(52,68),(68,70),(70,71),(71,74),(74,78),(78,128),(128,188),(188,199),
                    (199,206),(206,256),(256,306),(306,356),(356,406),(406,409),(409,412),(412,414),(414,416),(416,418),(418,420),(420,422),
                    (422,424),(424,426),(426,428),(428,429),(429,430),(430,431),(431,432),(432,433),(433,436),(436,439),(439,442),(442,445),
                    (445,448),(448,451),(451,454),(454,456),(456,458),(458,461),(461,464),(464,467),(467,470),(470,473),(473,476),(476,479),
                    (479,482),(482,490),(490,498),(498,506),(506,514),(514,522),(522,529),(529,536),(536,543),(543,550),(550,558),(558,566),
                    (566,573),(573,581),(581,593),(593,597),(597,None)]

    dag_id = "meps_prescribed_medications"
    filename = "h206a"
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
        df = pd.read_excel(data_path + f'/{filename}.xlsx')
        df.columns = df.columns.str.lower()
        '''
        
        df = pd.read_fwf(
            data_path + f'/{filename.upper()}.dat',
            header=None,
            names=col_names,
            converters={col: str for col in col_names},
            colspecs=col_spaces,
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

meps_prescribed_medications()
