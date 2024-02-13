from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task
from common_dag_tasks import  get_ds_folder

from lxml import etree

def transform_xml(input_xml, xslt):
    # load xml input
    dom = etree.parse(input_xml)
    # load XSLT
    xslt_doc = etree.parse(xslt)
    xslt_transformer = etree.XSLT(xslt_doc)
    # apply XSLT on loaded dom
    new_xml = xslt_transformer(dom)
    return etree.tostring(new_xml, pretty_print=True).decode("utf-8")


@task
def process_dailymed(dag_id, data_folder):
    import zipfile
    import re
    import os
    import pandas as pd
    import sqlalchemy
    from pathlib import Path

    db_conn_string = os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"]
    db_conn = sqlalchemy.create_engine(db_conn_string)
    xslt = get_ds_folder(dag_id) / "dailymed_prescription.xsl"

    data_folder = Path(data_folder) / "prescription"

    for zip_folder in data_folder.iterdir():
        print(zip_folder)
        with zipfile.ZipFile(zip_folder) as unzipped_folder:
            folder_name = zip_folder.stem
            for subfile in unzipped_folder.infolist():
                if re.match(".*.xml", subfile.filename):
                    new_file = unzipped_folder.extract(subfile, data_folder)
                    # xslt transform
                    xml_content = transform_xml(new_file, xslt)
                    os.remove(new_file)
                    df = pd.DataFrame(
                        columns=["spl", "file_name", "xml_content"],
                        data=[[folder_name, subfile.filename, xml_content]],
                    )
                    df.to_sql(
                        "dailymed_daily",
                        schema="datasource",
                        con=db_conn,
                        if_exists="append",
                        index=False,
                    )