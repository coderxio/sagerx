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
    col_names = ['duid', 'pid', 'dupersid', 'drugidx', 'rxrecidx', 'linkidx','panel', 'purchrd', 'rxbegmm', 'rxbegyrx', 'rxname',
                'rxdrgnam', 'rxndc', 'rxquanty', 'rxform', 'rxfrmunt','rxstreng', 'rxstrunt', 'rxdaysup', 'phartp1', 'phartp2',
                'phartp3', 'phartp4', 'phartp5', 'phartp6', 'phartp7','phartp8', 'phartp9', 'rxflg', 'impflag', 'pcimpflg',
                'diabequip', 'inpcflg', 'pregcat', 'tc1', 'tc1s1','tc1s1_1', 'tc1s1_2', 'tc1s2', 'tc1s2_1', 'tc1s3',
                'tc1s3_1', 'tc2', 'tc2s1', 'tc2s1_1', 'tc2s1_2', 'tc2s2','tc3', 'tc3s1', 'tc3s1_1', 'rxsf18x', 'rxmr18x', 'rxmd18x',
                'rxpv18x', 'rxva18x', 'rxtr18x', 'rxof18x', 'rxsl18x','rxwc18x', 'rxot18x', 'rxor18x', 'rxou18x', 'rxxp18x',
                'perwt18f', 'varstr', 'varpsu']

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

        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = pg_hook.get_sqlalchemy_engine()

        # create empty table with columns in postgres
        # overwrite existing table, if exists
        df = pd.DataFrame(columns = col_names)
        df.to_sql(
            dag_id,
            con=engine,
            schema="datasource",
            if_exists="replace",
            index=False
        )

        with pd.read_fwf(
            data_path + f'/{filename.upper()}.dat',
            header=None,
            names=col_names,
            converters={col: str for col in col_names},
            colspecs=col_spaces,
            chunksize=1000
        ) as reader:
            reader
            for chunk in reader:
                chunk.to_sql(
                    dag_id,
                    con=engine,
                    schema="datasource",
                    if_exists="append",
                    index=False
                )

    load(extract())

meps_prescribed_medications()
