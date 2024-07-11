from pathlib import Path
import pendulum

from airflow.decorators import dag, task
from airflow.hooks.subprocess import SubprocessHook

from lxml import etree

from sagerx import create_path

@dag(
    schedule="0 0 10 * *",
    start_date=pendulum.yesterday(),
    catchup=False
)
def dailymed():
    dag_id = "dailymed"

    ds_folder = Path("/opt/airflow/dags") / dag_id
    data_folder = Path("/opt/airflow/data") / dag_id
    #file_set = "dm_spl_release_human_rx_part"
    file_set = "dm_spl_daily_update_07092024"

    def connect_to_ftp_dir(ftp_str: str, dir: str):
        import ftplib

        ftp = ftplib.FTP(ftp_str)
        ftp.login()

        ftp.cwd(dir)

        return ftp

    def obtain_ftp_file_list(ftp):
        import fnmatch

        file_list = []
        for file in ftp.nlst():
            if fnmatch.fnmatch(file, f"*{file_set}*"):
                file_list.append(file)
        return file_list
    
    def get_dailymed_files(ftp, file_name: str):
        import zipfile
        import os

        zip_path = create_path(data_folder) / file_name

        with open(zip_path, "wb") as file:
            ftp.retrbinary(f"RETR {file_name}", file.write)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(data_folder.with_suffix(""))
        os.remove(zip_path)

    def transform_xml(input_xml, xslt):
        # load xml input
        dom = etree.parse(input_xml, etree.XMLParser(huge_tree=True))
        # load XSLT
        xslt_doc = etree.parse(xslt)
        xslt_transformer = etree.XSLT(xslt_doc)
        # apply XSLT on loaded dom
        new_xml = xslt_transformer(dom)
        return etree.tostring(new_xml, pretty_print=True).decode("utf-8")

    @task
    def extract():
        dailymed_ftp = "public.nlm.nih.gov"
        ftp_dir = "/nlmdata/.dailymed/"

        ftp = connect_to_ftp_dir(dailymed_ftp, ftp_dir)

        for file_name in obtain_ftp_file_list(ftp):
            get_dailymed_files(ftp, file_name)

    @task
    def load():
        import zipfile
        import re
        import os
        import pandas as pd
        import sqlalchemy

        xslt = ds_folder / "template.xsl"

        db_conn_string = os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"]
        db_conn = sqlalchemy.create_engine(db_conn_string)

        prescription_data_folder = (
            data_folder
            / "prescription"
        )

        for zip_folder in prescription_data_folder.iterdir():
            # logging.info(zip_folder)
            with zipfile.ZipFile(zip_folder) as unzipped_folder:
                folder_name = zip_folder.stem
                for subfile in unzipped_folder.infolist():
                    if re.search("\.xml$", subfile.filename):
                        new_file = unzipped_folder.extract(subfile, prescription_data_folder)
                        # xslt transform
                        xml_content = transform_xml(new_file, xslt)
                        os.remove(new_file)
                        df = pd.DataFrame(
                            columns=["spl", "file_name", "xml_content"],
                            data=[[folder_name, subfile.filename, xml_content]],
                        )
                        df.to_sql(
                            "dailymed",
                            schema="sagerx_lake",
                            con=db_conn,
                            if_exists="append",
                            index=False,
                        )

    # Task to transform data using dbt
    @task
    def transform():
        subprocess = SubprocessHook()
        result = subprocess.run_command(['dbt', 'run', '--select', 'models/staging/dailymed', 'models/intermediate/dailymed'], cwd='/dbt/sagerx')
        print("Result from dbt:", result)

    extract() >> load() >> transform()

dailymed()
