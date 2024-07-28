from airflow.decorators import task
import pandas as pd
from sagerx import load_df_to_pg,
from urllib.request import urlopen
import json
from pandas import json_normalize
from concurrent.futures import ThreadPoolExecutor
from functools import partial

def fetch_json(url):
    with urlopen(url) as response:
        return json.loads(response.read())

def process_concept(class_base_url, concept):
    url = class_base_url + concept['rxcui']
    try:
        cur_json = fetch_json(url)
        class_data = cur_json['rxclassDrugInfoList']['rxclassDrugInfo']
        return [dict(concept, class_data=k['rxclassMinConceptItem']) for k in class_data]
    except:
        return []


base_url = "https://rxnav.nlm.nih.gov/REST/allconcepts.json?tty=SBD+SCD+GPCK+BPCK"
class_base_url = "https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json?rxcui="

cui_json = fetch_json(base_url)
concepts = cui_json['minConceptGroup']['minConcept']

with ThreadPoolExecutor() as executor:
    process_func = partial(process_concept, class_base_url)
    results = list(executor.map(process_func, concepts))

data = [item for sublist in results for item in sublist]

df = pd.DataFrame(data)
c_df = json_normalize(df['class_data'])
full_df = pd.concat([df.drop(columns=['class_data']), c_df], axis=1).drop_duplicates().reset_index(drop=True)

load_df_to_pg(full_df,"sagerx_lake","rxclass","replace",index=False)
