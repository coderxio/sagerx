from pathlib import Path
import os

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
def download_dataset(url: str, dest: os.PathLike = Path.cwd(), file_name: str = None):
    """Downloads a data set file from provided Url via a requests steam

    url = url to send request to
    dest = path to save downloaded file to
    filename = name to call file, if None last segement of url is used"""
    import requests

    if file_name == None:
        dest_path = create_path(dest) / url.split("/")[-1]
    else:
        dest_path = create_path(dest) / file_name
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return dest_path


# Airflow DAG Functions
def get_dataset(ds_url, data_folder, ti):
    """retreives a dataset from the web and passes the filepath to airflow xcom
    if file is a zip, it extracts the contents and deletes zip

    ds_url = url to download dataset file from
    data_folder = path to save dataset to
    ti = airflow parameter to store task instance for xcoms"""
    import zipfile

    file_path = download_dataset(url=ds_url, dest=data_folder)

    if file_path.suffix == ".zip":
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(file_path.with_suffix(""))
        os.remove(file_path)
        file_path = file_path.with_suffix("")

    file_path_str = file_path.resolve().as_posix()
    ti.xcom_push(key="file_path", value=file_path_str)


def get_sql_list(pre_str: str = "", ds_path: os.PathLike = Path.cwd()):
    """When given a folder path returns all .sql files in it as a list

    pre_str = determines what sql files to grab by matching str at start of name
    ds_path = folder path to grab sqls from"""
    import glob
    import os

    glob_folder = ds_path.resolve().as_posix()
    glob_path = glob_folder + "/" + pre_str + "*.sql"
    sql_file_list = []
    for path in glob.glob(glob_path):
        file = os.path.basename(path)
        sql_file_list.append(file)
    return sql_file_list
