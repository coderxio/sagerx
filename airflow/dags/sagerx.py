from pathlib import Path
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models import Variable
from airflow.decorators import task
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests
from time import sleep
import pandas as pd
from urllib.request import urlopen
import json
from concurrent.futures import ThreadPoolExecutor
import time
from urllib.error import HTTPError
import threading
import asyncio
import aiohttp

# Filesystem functions
def create_path(*args):
    """creates and returns folder path object if it does not exist"""

    p = Path.cwd().joinpath(*args)
    if not p.exists():
        p.mkdir(parents=True)
    return p

def read_sql_file(sql_path: str):
    """reads a sql file and returns the string when given a path
    sql_path = path as string to sql file"""
    
    fd = open(sql_path, "r")
    sql_string = fd.read()
    fd.close()
    return sql_string

def read_json_file(json_path:str):
    import json

    with open(json_path,'r') as f:
        json_object = json.load(f)
    return json_object

# Web functions
def download_dataset(url: str, dest: Path = Path.cwd(), file_name: str = None):
    """Downloads a data set file from provided Url via a requests steam

    url = url to send request to
    dest = path to save downloaded file to
    filename = name to call file, if None last segement of url is used"""
    import requests
    import re

    with requests.get(url, stream=True, allow_redirects=True) as r:
        r.raise_for_status()

        if file_name == None:
            try:
                content_disposition_list = r.headers["Content-Disposition"].split(";")

                compiled_regex = re.compile(
                    r"""
                    # the filename directive keyword
                    filename=
                    # 0 or 1 quote
                    (?:"|')?
                    # capture the filename itself
                    (?P<filename>.+)
                    # a quote or end of string
                    # if not proceded by a quote
                    (?:"|'|(?<!(?:"|'))$)
                    """,
                    re.VERBOSE,
                )

                # get the only element of the content_disposition_list
                # after filtering based on regex pattern
                # NOTE: if 0 or >1 elements after filtering, will return ValueError
                [filename_directive] = list(
                    filter(compiled_regex.search, content_disposition_list)
                )

                match = compiled_regex.search(filename_directive)

                file_name = match.group("filename")

            except:
                file_name = url.split("/")[-1]

        dest_path = create_path(dest) / file_name

        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return dest_path


# Airflow DAG Functions
def get_dataset(ds_url, data_folder, ti=None, file_name=None):
    """retreives a dataset from the web and passes the filepath to airflow xcom
    if file is a zip, it extracts the contents and deletes zip

    ds_url = url to download dataset file from
    data_folder = path to save dataset to
    ti = airflow parameter to store task instance for xcoms"""
    import zipfile
    import logging

    file_path = download_dataset(url=ds_url, dest=data_folder)
    logging.info(f"requested url: {ds_url}")
    if file_path.suffix == ".zip":
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(file_path.with_suffix(""))
        Path.unlink(file_path)
        file_path = file_path.with_suffix("")

    # change name of file if one is provided
    if file_name != None:
        file_path.rename(file_path.with_name(file_name))
        file_path = file_path.with_name(file_name)

    file_path_str = file_path.resolve().as_posix()
    if ti != None:
        ti.xcom_push(key="file_path", value=file_path_str)
    logging.info(f"created dataset at path: {file_path}")
    return file_path_str


def get_sql_list(pre_str: str = "", ds_path: Path = Path.cwd()) -> list:
    """When given a folder path returns all .sql files in it as a list

    pre_str = determines what sql files to grab by matching str at start of name
    ds_path = folder path to grab sqls from"""

    sql_file_list = [path.name for path in ds_path.glob(pre_str + "*.sql")]
    return sql_file_list


# Slack webhook function
def alert_slack_channel(context):
    slack_api = Variable.get("slack_api")
    if slack_api:
        msg = """
                :red_circle: Task Failed
                *Task*: {task}  
                *Dag*: {dag} 
                *Execution Time*: {exec_date}  
                *Log Url*: {log_url} 
                """.format(
            task=context.get("task_instance").task_id,
            dag=context.get("task_instance").dag_id,
            ti=context.get("task_instance"),
            exec_date=context.get("execution_date"),
            log_url=context.get("task_instance").log_url,
        )

        SlackWebhookOperator(
            task_id="alert_slack_channel",
            http_conn_id="slack",
            message=msg,
        ).execute(context=None)

def load_df_to_pg(df,schema_name:str,table_name:str,if_exists:str,dtype_name:str="",index:bool=True, 
                  create_index: bool = False, index_columns: list = None) -> None:
    from airflow.hooks.postgres_hook import PostgresHook
    import sqlalchemy

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg_hook.get_sqlalchemy_engine()

    if dtype_name:
        dtype = {dtype_name:sqlalchemy.types.JSON}
    else:
        dtype = {}
    
    # trying it this way to prevent wiping tables that actually need to append
    if if_exists == 'replace':
        engine.execute(f'drop table if exists {schema_name}.{table_name} cascade')
        if_exists = 'append'

    df.to_sql(
        table_name,
        con=engine,
        schema=schema_name,
        if_exists=if_exists,
        dtype=dtype,
        index=index
    )

    if create_index and index_columns:
        columns_str = ', '.join(index_columns)
        engine.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_{"_".join(index_columns)} ON {schema_name}.{table_name} ({columns_str})')

def run_query_to_df(query:str) -> pd.DataFrame:
    from airflow.hooks.postgres_hook import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg_hook.get_sqlalchemy_engine()
    df = pd.read_sql(query, con=engine)
    
    return df

@retry(
        stop=stop_after_attempt(20),
        wait=wait_exponential(multiplier=1,max=10),
        reraise=True
)
def get_api(url):
    response = requests.get(url)
    if response.status_code == 429:
        sleep(60)
        raise requests.exceptions.RequestException("429 Too Many Requests, retry after 60 seconds", response=response)
    elif response.status_code != 200:
        raise Exception("API call failed with status code: {}".format(response.status_code))
    data = response.json()
    return data

def parallel_api_calls(api_calls:list) -> list:
    from concurrent.futures import ThreadPoolExecutor, as_completed
    output = []
    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = {executor.submit(get_api, api_call):api_call for api_call in api_calls}

        for future in as_completed(futures):
            url = futures[future]
            response = future.result()
            if not len(response) == 0:
                output.append({"url":url,"response":response})
            else:
                print(f"Empty response for url: {url}")
    return output


class RateLimiter:
    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = threading.Lock()

    def __call__(self, f):
        def wrapped(*args, **kwargs):
            with self.lock:
                now = time.time()
                self.calls = [c for c in self.calls if now - c < self.period]
                if len(self.calls) >= self.max_calls:
                    time.sleep(self.period - (now - self.calls[0]))
                self.calls.append(time.time())
            return f(*args, **kwargs)
        return wrapped

@RateLimiter(max_calls=20, period=1)
def fetch_json(url):
    with urlopen(url) as response:
        return json.loads(response.read())

def api_call(url, max_retries=3, initial_delay=1):
    for attempt in range(max_retries):
        try:
            json = fetch_json(url)
            return {"url":url,"response":json}
            #return json
        except HTTPError as e:
            if e.code == 429:
                delay = initial_delay * (2 ** attempt)
                time.sleep(delay)
            else:
                time.sleep(initial_delay)
        except Exception:
            time.sleep(initial_delay)
    
    return None

@task
def get_rxcuis(ttys:list) -> list:
    from airflow.hooks.postgres_hook import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg_hook.get_sqlalchemy_engine()

    ttys_str = ', '.join(f"'{item}'" for item in ttys)
    df = pd.read_sql(
            f"select distinct rxcui from sagerx_lake.rxnorm_rxnconso where tty in ({ttys_str}) and sab = 'RXNORM'",
            con=engine
        )
    rxcuis = list(df['rxcui'])
    
    print(f"Number of RxCUIs: {len(rxcuis)}")
    return rxcuis

@task
def get_rxcuis_from_rxnorm_api(ttys:list) -> list:
    ttys_str = '+'.join(ttys)

    # NOTE: this API seems to only return ACTIVE RXCUIs
    # this is important to note for things like RxNorm Historical
    # which probably requires more than just currently active RXCUIs
    base_url = f"https://rxnav.nlm.nih.gov/REST/allconcepts.json?tty={ttys_str}"

    json = fetch_json(base_url)
    concepts = json['minConceptGroup']['minConcept']
    rxcuis = [concept['rxcui'] for concept in concepts]
    
    print(f"Number of RxCUIs: {len(rxcuis)}")
    return rxcuis

def rate_limited_api_calls(url_list:list) -> list:
    with ThreadPoolExecutor(max_workers=20) as executor:
        responses = list(executor.map(api_call, url_list))

    return responses

async def fetch(session, url, semaphore):
    async with semaphore:
        async with session.get(url) as response:
            return {"url": url, "data": await response.json()}

async def fetch_all(url_list:list, max_calls_per_second=20):
    semaphore = asyncio.Semaphore(max_calls_per_second)
    connector = aiohttp.TCPConnector(limit=max_calls_per_second)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for url in url_list:
            tasks.append(fetch(session, url, semaphore))
            await asyncio.sleep(1 / max_calls_per_second)
        return await asyncio.gather(*tasks)

def async_api_calls(url_list:list, max_calls_per_second=20) -> list:
    loop = asyncio.get_event_loop()
    response_list = loop.run_until_complete(fetch_all(url_list, max_calls_per_second=max_calls_per_second))
    return response_list
