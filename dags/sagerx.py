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
