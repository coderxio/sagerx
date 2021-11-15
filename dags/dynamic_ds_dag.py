from datetime import date, timedelta
from textwrap import dedent
from pathlib import Path
import calendar

from sagerx import get_dataset, read_sql_file, get_sql_list

import user_macros

data_set_list = [
    {
        "dag_id": "nadac",
        "schedule_interval": "0 6 * * 5",  # run a 6am every thur (url marco minuses day to get wed)
        "url": "https://download.medicaid.gov/data/nadac-national-average-drug-acquisition-cost-{{ get_date_of_prior_weekday('wednesday', ds_datetime( ds ), '%m-%d-%Y' ) }}.csv",
        "user_defined_macros": {
            "get_date_of_prior_weekday": user_macros.get_date_of_prior_weekday,
            "ds_datetime": user_macros.ds_datetime,
        }
        #   "url": "https://download.medicaid.gov/data/nadac-national-average-drug-acquisition-cost-10-20-2021.csv"
    },
    {
        "dag_id": "cms_noc_pricing",
        "schedule_interval": "0 0 20 */3 *",  # runs every quarter on the 20th day of the month
        "url": "https://www.cms.gov/files/zip/{{ get_first_day_of_quarter(ds_datetime( ds ), '%B-%Y' ) }}-noc-pricing-file.zip",
        #   "url": "https://www.cms.gov/files/zip/october-2021-noc-pricing-file.zip"
        "user_defined_macros": {
            "get_first_day_of_quarter": user_macros.get_first_day_of_quarter,
            "ds_datetime": user_macros.ds_datetime,
        },
    },
    {
        "dag_id": "cms_ndc_hcpcs",
        "schedule_interval": "0 0 20 */3 *",  # runs every quarter on the 20th of the month
        "url": "https://www.cms.gov/files/zip/{{ get_first_day_of_quarter(ds_datetime( ds ), '%B-%Y' ) }}-asp-ndc-hcpcs-crosswalk.zip",
        # https://www.cms.gov/files/zip/october-2021-asp-ndc-hcpcs-crosswalk.zip
        "user_defined_macros": {
            "get_first_day_of_quarter": user_macros.get_first_day_of_quarter,
            "ds_datetime": user_macros.ds_datetime,
        },
    },
    {
        "dag_id": "cms_asp_pricing",
        "schedule_interval": "0 0 20 */3 *",  # runs once every quarter on the 20th of each month
        "url": "https://www.cms.gov/files/zip/{{ get_first_day_of_quarter(ds_datetime( ds ), '%B-%Y' ) }}-asp-pricing-file.zip",
        #   "url": "https://www.cms.gov/files/zip/october-2021-asp-pricing-file.zip"
        "user_defined_macros": {
            "get_first_day_of_quarter": user_macros.get_first_day_of_quarter,
            "ds_datetime": user_macros.ds_datetime,
        },
    },
    {
        "dag_id": "cms_addendum_a",
        "schedule": "0 0 20 */3 *",  # runs every quarter on the 20th
        "url": "https://www.cms.gov/files/zip/addendum-{{ get_first_day_of_quarter(ds_datetime( ds ), '%B-%Y' ) }}.zip?agree=yes&next=Accept",
        # "url":https://www.cms.gov/files/zip/addendum-october-2021.zip?agree=yes&next=Accept
        "user_defined_macros": {
            "get_first_day_of_quarter": user_macros.get_first_day_of_quarter,
            "ds_datetime": user_macros.ds_datetime,
        },
    },
    {
        "dag_id": "cms_addendum_b",
        "schedule": "0 0 20 */3 *",  # runs every quarter on the 20th
        "url": "https://www.cms.gov/files/zip/{{ get_first_day_of_quarter(ds_datetime( ds ), '%B-%Y' ) }}-addendum-b.zip?agree=yes&next=Accept",
        # "url": "https://www.cms.gov/files/zip/october-2021-addendum-b.zip?agree=yes&next=Accept"
        "user_defined_macros": {
            "get_first_day_of_quarter": user_macros.get_first_day_of_quarter,
            "ds_datetime": user_macros.ds_datetime,
        },
    },
    {
        "dag_id": "fda_excluded",
        "schedule_interval": "30 4 * * *",  # run a 4:30am every day
        "url": "https://www.accessdata.fda.gov/cder/ndc_excluded.zip",
    },
    {
        "dag_id": "fda_ndc",
        "schedule_interval": "0 4 * * *",  # run a 4am every day
        "url": "https://www.accessdata.fda.gov/cder/ndctext.zip",
    },
    {
        "dag_id": "fda_unfinished",
        "schedule_interval": "15 4 * * *",  # run a 4:15am every day
        "url": "https://www.accessdata.fda.gov/cder/ndc_unfinished.zip",
    },
]


########################### DYNAMIC DAG DO NOT TOUCH BELOW HERE #################################

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


def create_dag(dag_args):

    dag_id = dag_args["dag_id"]
    url = dag_args["url"]
    retrieve_dataset_function = dag_args["retrieve_dataset_function"]

    dag = DAG(
        dag_id,
        default_args=dag_args,
        description=f"Processes {dag_id} source",
        user_defined_macros=dag_args.get("user_defined_macros"),
    )

    ds_folder = Path("/opt/airflow/dags") / dag_id
    data_folder = Path("/opt/airflow/data") / dag_id

    with dag:

        # Task to download data from web location
        get_data = PythonOperator(
            task_id=f"get_{dag_id}",
            python_callable=retrieve_dataset_function,
            op_kwargs={"ds_url": url, "data_folder": data_folder},
        )

        tl = [get_data]
        # Task to load data into source db schema
        for sql in get_sql_list(
            "load-",
            ds_folder,
        ):
            sql_path = ds_folder / sql
            tl.append(
                PostgresOperator(
                    task_id=sql,
                    postgres_conn_id="postgres_default",
                    sql=read_sql_file(sql_path),
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

        for sql in get_sql_list("view-", ds_folder):
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

    return dag


# builds a dag for each data set in data_set_list
for ds in data_set_list:

    default_args = {
        "owner": "airflow",
        "start_date": days_ago(0),
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # none airflow common dag elements
        "retrieve_dataset_function": get_dataset,
    }

    dag_args = {**default_args, **ds}

    globals()[dag_args["dag_id"]] = create_dag(dag_args)
