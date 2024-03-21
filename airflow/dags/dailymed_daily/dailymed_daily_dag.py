from datetime import date, timedelta
from textwrap import dedent
from pathlib import Path
from lxml import etree

from sagerx import (
    read_sql_file,
    get_sql_list,
    alert_slack_channel,
    get_dataset,
)


ds = {
    "dag_id": "dailymed_daily",
    "schedule_interval": "45 7 * * 1-5",  # at 7:45 am once daily)
    "url": "https://dailymed-data.nlm.nih.gov/public-release-files/dm_spl_daily_update_{{ macros.ds_format(macros.ds_add(ds,-1), '%Y-%m-%d', '%m%d%Y') }}.zip",
}


def transform_xml(input_xml, xslt):
    # load xml input
    dom = etree.parse(input_xml)
    # load XSLT
    xslt_doc = etree.parse(xslt)
    xslt_transformer = etree.XSLT(xslt_doc)
    # apply XSLT on loaded dom
    new_xml = xslt_transformer(dom)
    return etree.tostring(new_xml, pretty_print=True).decode("utf-8")


def process_dailymed(data_folder, xslt, ti):
    import zipfile
    import re
    import os
    import logging
    import pandas as pd
    import sqlalchemy

    db_conn_string = os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"]
    db_conn = sqlalchemy.create_engine(db_conn_string)

    data_folder = (
        data_folder
        / ti.xcom_pull(key="file_path", task_ids="get_dailymed_daily")
        / "prescription"
    )

    for zip_folder in data_folder.iterdir():
        logging.info(zip_folder)
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
                        schema="sagerx_lake",
                        con=db_conn,
                        if_exists="append",
                        index=False,
                    )


########################### DYNAMIC DAG DO NOT TOUCH BELOW HERE #################################

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


# builds a dag for each data set in data_set_list
default_args = {
    "owner": "airflow",
    "start_date": days_ago(0),
    "depends_on_past": False,
    "email": ["admin@sagerx.io"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # none airflow common dag elements
    "on_failure_callback": alert_slack_channel,
}

dag_args = {**default_args, **ds}


dag_id = dag_args["dag_id"]
url = dag_args["url"]

dag = DAG(
    dag_id,
    schedule_interval=dag_args["schedule_interval"],
    default_args=dag_args,
    description=f"Processes {dag_id} source",
    user_defined_macros=dag_args.get("user_defined_macros"),
)

ds_folder = Path("/opt/airflow/dags") / dag_id
data_folder = Path("/opt/airflow/data") / dag_id


with dag:
    # Task to download data from web location

    tl = []
    # Task to download data from web location
    tl.append(
        PythonOperator(
            task_id=f"get_{dag_id}",
            python_callable=get_dataset,
            op_kwargs={"ds_url": url, "data_folder": data_folder},
        )
    )

    # Task to load data into source db schema
    tl.append(
        PythonOperator(
            task_id=f"load_{dag_id}",
            python_callable=process_dailymed,
            op_kwargs={
                "data_folder": data_folder,
                "xslt": ds_folder / "dailymed_prescription.xsl",
            },
        )
    )

    for sql in get_sql_list("staging-", ds_folder):
        sql_path = ds_folder / sql
        tl.append(
            PostgresOperator(
                task_id=sql,
                postgres_conn_id="postgres_default",
                sql=read_sql_file(sql_path),
            )
        )

    for i in range(len(tl)):
        if i not in [0]:
            tl[i - 1] >> tl[i]
