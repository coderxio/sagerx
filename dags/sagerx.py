# Filesystem functions
def create_path(*args):
    """creates and returns folder path object if it does not exist"""
    from pathlib import Path

    p = Path.cwd().joinpath(*args)
    if not p.exists():
        p.mkdir(parents=True)
    return p


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
    import os

    file_path = download_dataset(url=ds_url, dest=data_folder)

    if file_path.suffix == ".zip":
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(file_path.with_suffix(""))
        os.remove(file_path)
        file_path = file_path.with_suffix("")

    file_path_str = file_path.resolve().as_posix()
    ti.xcom_push(key="file_path", value=file_path_str)
