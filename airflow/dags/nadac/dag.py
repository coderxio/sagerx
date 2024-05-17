import datetime
from pathlib import Path
import pendulum

from sagerx import get_dataset, read_sql_file, get_sql_list, alert_slack_channel

from airflow.decorators import dag, task

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.subprocess import SubprocessHook

now = pendulum.now()
previous_thursday = now.previous(pendulum.THURSDAY)

@dag(
    start_date=previous_thursday,
    schedule_interval="0 6 * * 4",  # run at 6am every thur
    description="DAG for downloading NADAC weekly",
    on_failure_callback=alert_slack_channel,
    max_active_runs=1,
)
def nadac():

    dag_id = "nadac"
    ds_folder = Path("/opt/airflow/dags") / dag_id
    data_folder = Path("/opt/airflow/data") / dag_id

    @task()
    def extract(data_interval_start=None):
        import requests

        class nadac:
            def __init__(self):
                self.dataset_dict = self._build_dataset()
                print(f"Length of JSON: len(self.dataset_dict)")

            def _build_dataset(self):
                url = "https://data.medicaid.gov/api/1/search/?sort=modified&sort-order=desc&theme=National%20Average%20Drug%20Acquisition%20Cost"
                response = requests.get(url)
                response.raise_for_status()
                result_json = response.json()["results"]
                dataset_dict = {}
                for key, value in result_json.items():
                    if "NADAC (National Average Drug Acquisition Cost)" in value["title"]:
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

    # Task to transform data using dbt
    @task
    def transform():
        subprocess = SubprocessHook()
        result = subprocess.run_command(['dbt', 'run', '--select', 'models/staging/nadac'], cwd='/dbt/sagerx')
        print("Result from dbt:", result)

    extract() >> load >> transform()

nadac()
