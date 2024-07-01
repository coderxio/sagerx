import pendulum

from airflow_operator import create_dag
from airflow.utils.helpers import chain

from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
import xmltodict
import re

dag_id = "dailymed_images"

dag = create_dag(
    dag_id=dag_id,
    schedule= "0 8 * * 1-5",  # at 7:45 am once daily)
    start_date=pendulum.yesterday(),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
)

@task
def get_dailymed_data():
    from airflow.hooks.postgres_hook import PostgresHook
    query = "SELECT * FROM sagerx_lake.dailymed_daily limit 1"
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg_hook.get_sqlalchemy_engine()
    df = pd.read_sql(query, con=engine)

    return df

def isolate_ndc(input_string):
    pattern = r"\b\d{5}-\d{3}-\d{2}\b"
    match = re.search(pattern, input_string)
    
    if match:
        return match.group(0)
    else:
        return None

def clean_string(input_string):
    cleaned_string = input_string.strip()
    cleaned_string = ' '.join(cleaned_string.split())
    return cleaned_string

def extract_data(xml_string):
    xml_data = xmltodict.parse(xml_string)

    med_dict = {}
    set_id = xml_data['document']['setId']['@root']

    for component in xml_data['document']['component']['structuredBody']['component']:
        if component['section']['code']['@code'] == '51945-4':
            component_dict = {}

            additional_info = []
            for para in component['section']['text']['paragraph']:
                if isinstance(para, str):
                    if 'NDC' in para:
                        ndc = isolate_ndc(para)
                    else: 
                        additional_info.append(clean_string(para))
                elif isinstance(para,dict):
                    if 'content' in para.keys():
                        additional_info.append(clean_string(para['content']['#text']))
                    elif '#text' in para.keys():
                        if 'NDC' in para['#text']:
                            ndc = isolate_ndc(clean_string(para['#text']))
                        else:
                            additional_info.append(clean_string(para['#text']))
            
            component_dict['set_id'] = set_id
            component_dict['effective_time'] = component['section']['effectiveTime']['@value']

            if component['section']['component']['observationMedia']['value']['@mediaType'] == 'image/jpeg':
                image_id = component['section']['component']['observationMedia']['value']['reference']['@value']
                component_dict['image_id'] = image_id

            component_dict['image_url'] = f"https://dailymed.nlm.nih.gov/dailymed/image.cfm?name={image_id}&setid={set_id}&type=img"
            component_dict['additional_info'] = additional_info
            med_dict[ndc] = component_dict

    df = pd.DataFrame.from_dict(med_dict,orient='index')
    df.index.name = 'ndc'
    df = df.reset_index()

    return df

with dag:
    dm_data = get_dailymed_data() 

    print(dm_data['xml_content'])