import pendulum

from airflow_operator import create_dag
from airflow.utils.helpers import chain

from airflow.decorators import task
import pandas as pd

from dailymed_images.dag_tasks import *
from sagerx import load_df_to_pg

dag_id = "dailymed_images"

dag = create_dag(
    dag_id=dag_id,
    schedule= "0 8 * * 1-5",  # at 7:45 am once daily)
    start_date=pendulum.yesterday(),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
)

"""
Process

1. get xml data from dailymed_daily
2. read xml data 
3. find ndc and image data in xml
4. map ndc and image data 
5. map ndc and image ids toghether
5.a. check to see if NDC is in the image name, if so map together
5.b. check to see if NDC is under the same 51945-4 component, if so then map together
5.c. Run image PCR to pull NDC fr
6. upload to postgres


"""


@task
def get_dailymed_data():
    from airflow.hooks.postgres_hook import PostgresHook

    query = "SELECT * FROM sagerx_lake.dailymed_daily"
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg_hook.get_sqlalchemy_engine()
    df = pd.read_sql(query, con=engine)
    print(f"DF length of {len(df)} with columns: {df.columns}")

    df['raw_xml_content'] = df.apply(parse_xml_content, axis=1)
    df['set_id'] = df.apply(lambda x: extract_set_id(x['raw_xml_content']), axis=1)
    df['ndc_ids'] = df.apply(lambda x: find_ndc_numbers(x['raw_xml_content']), axis=1)
    df['image_ids'] = df.apply(lambda x: find_image_ids(x['raw_xml_content']), axis=1)
    df['ndc_image_mapping'] = df.apply(map_ndcs_parent_function, axis=1)

    load_df_to_pg(df[['spl','file_name','set_id','ndc_ids','image_ids','ndc_image_mapping']],"sagerx_lake","dailymed_images","replace",dtype_name="ndc_image_mapping")

    #df['med_dict'] = df.apply(extract_data, axis=1)

    # dfs = []
    # for md in df['med_dict']:
    #     df_temp = pd.DataFrame.from_dict(md,orient='index')
    #     df_temp.index.name = 'ndc'
    #     dfs.append(df_temp)

    # df_final = pd.concat(dfs)
    # df_final = df_final.reset_index()

    #print(df_final)
    # return df
     
    
with dag:
    get_dailymed_data()