import pendulum
from airflow_operator import create_dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from common_dag_tasks import get_ds_folder, get_data_folder
from pathlib import Path
import requests, pandas as pd

dag_id = "cms_hcpcs_2020"
url = "https://www.cms.gov/files/zip/2020-corrections-alpha-numeric-hcpcs-file.zip"
"""change this url to whichever one you want"""
dag = create_dag(
    dag_id=dag_id,
    schedule="0 4 * * *",
    start_date=pendulum.yesterday(),
    catchup=False,
    concurrency=2,
)

def download_excel():
    folder = get_data_folder(dag_id)
    folder.mkdir(parents=True, exist_ok=True)
    out_path = folder / "HCPC2020_Corrections_Alpha.xlsx"
    response = requests.get(url)
    response.raise_for_status()

    zip_path = folder / "hcpcs.zip"
    with open(zip_path, "wb") as f:
        f.write(response.content)

    import zipfile
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(folder)

"""converts xlsx to csv"""
def convert_to_csv():
    folder = get_data_folder(dag_id)
    excel_file = next(folder.glob("*.xlsx"))
    csv_file = excel_file.with_suffix(".csv")
    df = pd.read_excel(excel_file, skiprows=4)
    df.to_csv(csv_file, index=False)
    print(f"Saved CSV: {csv_file}")

with dag:
    download_task = PythonOperator(
        task_id="download_excel",
        python_callable=download_excel,
    )

    convert_task = PythonOperator(
        task_id="convert_to_csv",
        python_callable=convert_to_csv,
    )

    load_task = PostgresOperator(
        task_id="load_hcpcs",
        postgres_conn_id="postgres_default",
        sql="""
        DROP TABLE IF EXISTS sagerx_lake.cms_hcpcs_2020 CASCADE;

        CREATE TABLE sagerx_lake.cms_hcpcs_2020 (
            code TEXT,
            action TEXT,
            eff_date TEXT,
            short_desc TEXT,
            long_desc TEXT,
            tos TEXT,
            betos TEXT,
            cov TEXT,
            price TEXT,
            xref_code TEXT,
            asc_ind TEXT,
            asc_date TEXT,
            comments TEXT
        );

        COPY sagerx_lake.cms_hcpcs_2020
        FROM '/opt/airflow/data/cms_hcpcs_2020/HCPC2020_Corrections_Alpha.csv'
        DELIMITER ',' CSV HEADER;
        """,
    )

    download_task >> convert_task >> load_task
