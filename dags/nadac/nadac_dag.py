import datetime
from pathlib import Path

from sagerx import get_dataset, read_sql_file, get_sql_list, alert_slack_channel


# The DAG object; we'll need this to instantiate a DAG
from airflow.decorators import dag, task

# Operators; we need this to operate!
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


@dag(
    start_date=days_ago(0),
    schedule_interval="0 6 * * 5",  # run a 6am every thur
    description="Dag for downloading NADAC weekly",
    on_failure_callback=alert_slack_channel,
)
def nadac():

    dag_id = "nadac"
    ds_folder = Path("/opt/airflow/dags") / dag_id
    data_folder = Path("/opt/airflow/data") / dag_id

    @task(task_id="download_nadac")
    def download_nadac(data_interval_start=None):
        import requests

        class nadac:
            def __init__(self):
                self.dataset_dict = self._build_dataset()

            def _build_dataset(self):
                url = "https://data.medicaid.gov/api/1/search/?sort=modified&sort-order=desc&theme=National%20Average%20Drug%20Acquisition%20Cost"
                response = requests.get(url)
                response.raise_for_status()
                result_json = response.json()["results"]
                dataset_dict = {}
                for key, value in result_json.items():
                    dataset_dict[value["title"]] = value["distribution"][0][
                        "downloadURL"
                    ]
                return dataset_dict

            def get_download_url(self, year):
                title = f"NADAC (National Average Drug Acquisition Cost) {year}"
                url = self.dataset_dict[title]
                self.title = title
                self.url = url
                return title, url

        # hit OpenFDA API to get
        ds_year = data_interval_start.strftime("%Y")
        nadac_obj = nadac()
        title, url = nadac_obj.get_download_url(ds_year)

        file = get_dataset(url, data_folder, None)

        return file

    tl = [download_nadac()]
    for types in ["load-", "staging-", "view-", "api-", "alter-"]:
        # Task to load data into source db schema
        for sql in get_sql_list(types, ds_folder):
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


nadac = nadac()
