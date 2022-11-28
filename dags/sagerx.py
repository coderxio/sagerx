from pathlib import Path
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models import Variable

# Filesystem functions
def create_path(*args):
    """creates and returns folder path object if it does not exist"""

    p = Path.cwd().joinpath(*args)
    if not p.exists():
        p.mkdir(parents=True)
    return p


# SQL functions
def read_sql_file(sql_path: str):
    """reads a sql file and returns the string when given a path
    sql_path = path as string to sql file"""

    fd = open(sql_path, "r")
    sql_string = fd.read()
    fd.close()
    return sql_string


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
def get_dataset(ds_url, data_folder, ti, file_name=None):
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


def get_sql_list(pre_str: str = "", ds_path: Path = Path.cwd()):
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
