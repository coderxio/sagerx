from pathlib import Path
import pendulum
import zipfile
import os

from airflow.decorators import dag, task
from airflow.hooks.subprocess import SubprocessHook

from lxml import etree

from sagerx import create_path, load_df_to_pg

@dag(
    schedule="0 0 10 * *",
    start_date=pendulum.yesterday(),
    catchup=False
)
def dailymed():
    dag_id = "dailymed"

    ds_folder = Path("/opt/airflow/dags") / dag_id
    data_folder = Path("/opt/airflow/data") / dag_id

    # NOTE: "dm_spl_release_human" accounts for both 
    # rx and otc SPLs (but no other types of SPLs)
    # - "dm_spl_release_human_rx" for rx meds only
    # - "dm_spl_release_human_otc" for otc meds only
    # - "dm_spl_release_human_rx_part1" for a given part
    # - "dm_spl_daily_update_MMDDYYYY" for a given date
    #   (replace MMDDYYY with your month, day, and year)
    file_set = "dm_spl_release_human_rx"

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

    def load_xml_data(spl_type_data_folder: Path):
        import re
        import pandas as pd
        import sqlalchemy

        xslt = ds_folder / "template.xsl"

        db_conn_string = os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"]
        db_conn = sqlalchemy.create_engine(db_conn_string)

        data = []
        for zip_folder in spl_type_data_folder.iterdir():
            with zipfile.ZipFile(zip_folder) as unzipped_folder:
                zip_file = zip_folder.stem
                set_id = zip_file.split('_')[1]
                for subfile in unzipped_folder.infolist():
                    if re.search("\.xml$", subfile.filename):
                        xml_file = subfile.filename

                        # xslt transform
                        temp_xml_file = unzipped_folder.extract(subfile, spl_type_data_folder)
                        xml_content = transform_xml(temp_xml_file, xslt)
                        os.remove(temp_xml_file)

                        # append row to the data list
                        data.append({"set_id": set_id, "zip_file": zip_file, "xml_file": xml_file, "xml_content": xml_content})
        
        df = pd.DataFrame(
            data,
            columns=["set_id", "zip_file", "xml_file", "xml_content"],
        )

        load_df_to_pg(
            df,
            schema_name="sagerx_lake",
            table_name="dailymed",
            if_exists="append", # TODO: make this better - maybe don't put stuff in multiple folders?
            index=False,
        )

    @task
    def extract():
        dailymed_ftp = "public.nlm.nih.gov"
        ftp_dir = "/nlmdata/.dailymed/"

        ftp = connect_to_ftp_dir(dailymed_ftp, ftp_dir)

        file_list = obtain_ftp_file_list(ftp)
        print(f'Extracting {file_list}')

        for file_name in file_list:
            get_dailymed_files(ftp, file_name)

    @task
    def load():
        spl_types = ['prescription', 'otc']
        
        for spl_type in spl_types:
            spl_type_data_folder = (
                data_folder
                / spl_type
            )
            if os.path.exists(spl_type_data_folder):
                print(f'Loading {spl_type} SPLs...')
                load_xml_data(spl_type_data_folder)


    # Task to transform data using dbt
    @task
    def transform():
        subprocess = SubprocessHook()
        result = subprocess.run_command(['dbt', 'run', '--select', 'models/staging/dailymed', 'models/intermediate/dailymed'], cwd='/dbt/sagerx')
        print("Result from dbt:", result)

    extract() >> load() >> transform()

dailymed()
