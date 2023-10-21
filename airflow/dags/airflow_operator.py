from airflow import DAG

from sagerx import get_dataset, read_sql_file, get_sql_list, alert_slack_channel
    
def create_dag(dag_id,**kwargs) -> DAG:
    from airflow.utils.dates import days_ago
    from datetime import timedelta

    dag_args ={
        "dag_id":dag_id,
        "start_date": days_ago(0),
        "schedule": "0 5 * * *",  # run at 5am every day
        "description": f"Processes {dag_id} source",
    }

    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "email": ["admin@sagerx.io"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "retrieve_dataset_function": get_dataset,
        "on_failure_callback": alert_slack_channel,
        "dagrun_timeout":60
    }

    dag_args.update(kwargs)
    default_args.update(kwargs)

    dag = DAG(**dag_args,default_args=default_args)

    return dag


    # def create_dag2(self):
    #     dag_id = self.dag_args["dag_id"]
    #     url = self.dag_args["url"]
    #     retrieve_dataset_function = self.dag_args["retrieve_dataset_function"]

    #     dag = DAG(
    #         dag_id,
    #         schedule_interval=self.dag_args["schedule_interval"],
    #         default_args=self.dag_args,
    #         description=f"Processes {dag_id} source",
    #         user_defined_macros=self.dag_args.get("user_defined_macros"),
    #     )

    #     ds_folder = Path("/opt/airflow/dags") / dag_id
    #     data_folder = Path("/opt/airflow/data") / dag_id

    #     with dag:

    #         # Task to download data from web location
    #         get_data = PythonOperator(
    #             task_id=f"get_{dag_id}",
    #             python_callable=retrieve_dataset_function,
    #             op_kwargs={"ds_url": url, "data_folder": data_folder},
    #         )

    #         tl = [get_data]
    #         # Task to load data into source db schema
    #         for sql in get_sql_list("load-", ds_folder):
    #             sql_path = ds_folder / sql
    #             tl.append(
    #                 PostgresOperator(
    #                     task_id=sql,
    #                     postgres_conn_id="postgres_default",
    #                     sql=read_sql_file(sql_path),
    #                 )
    #             )

    #         for sql in get_sql_list("staging-", ds_folder):
    #             sql_path = ds_folder / sql
    #             tl.append(
    #                 PostgresOperator(
    #                     task_id=sql,
    #                     postgres_conn_id="postgres_default",
    #                     sql=read_sql_file(sql_path),
    #                 )
    #             )

    #         for sql in get_sql_list("view-", ds_folder):
    #             sql_path = ds_folder / sql
    #             tl.append(
    #                 PostgresOperator(
    #                     task_id=sql,
    #                     postgres_conn_id="postgres_default",
    #                     sql=read_sql_file(sql_path),
    #                 )
    #             )

    #         for sql in get_sql_list("api-", ds_folder):
    #             sql_path = ds_folder / sql
    #             tl.append(
    #                 PostgresOperator(
    #                     task_id=sql,
    #                     postgres_conn_id="postgres_default",
    #                     sql=read_sql_file(sql_path),
    #                 )
    #             )

    #         for sql in get_sql_list("alter-", ds_folder):
    #             sql_path = ds_folder / sql
    #             tl.append(
    #                 PostgresOperator(
    #                     task_id=sql,
    #                     postgres_conn_id="postgres_default",
    #                     sql=read_sql_file(sql_path),
    #                 )
    #             )

    #         for i in range(len(tl)):
    #             if i not in [0]:
    #                 tl[i - 1] >> tl[i]

    #     return dag


