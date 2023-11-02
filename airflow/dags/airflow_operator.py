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