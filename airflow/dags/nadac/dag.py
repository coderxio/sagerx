import datetime
from pathlib import Path
import pendulum

from sagerx import get_dataset, read_sql_file, get_sql_list, alert_slack_channel

from airflow.decorators import dag, task

from airflow.providers.postgres.operators.postgres import PostgresOperator

from common_dag_tasks import transform

starting_date = pendulum.parse("2013-12-01")


@dag(
    start_date=starting_date,
    schedule_interval="0 6 * * 4",  # run at 6am every thur
    description="DAG for downloading NADAC weekly",
    on_failure_callback=alert_slack_channel,
    max_active_runs=1,
    catchup=False
)
def nadac():

    dag_id = "nadac"
    ds_folder = Path("/opt/airflow/dags") / dag_id
    data_folder = Path("/opt/airflow/data") / dag_id

    @task
    def extract(data_interval_start=None):
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

        title, ds_url = nadac_obj.get_download_url(ds_year)

        data_path = get_dataset(ds_url, data_folder, file_name="NADAC.csv")

        return data_path

    # Task to load data into source db schema
    load = []
    ds_folder = Path("/opt/airflow/dags") / dag_id
    for sql in get_sql_list("load", ds_folder):
        sql_path = ds_folder / sql
        task_id = sql[:-4]
        load.append(
            PostgresOperator(
                task_id=task_id,
                postgres_conn_id="postgres_default",
                sql=read_sql_file(sql_path),
            )
        )

    transform_task = transform(dag_id, models_subdir=['staging', 'intermediate'])

    extract() >> load >> transform_task

nadac()
