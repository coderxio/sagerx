import pendulum

from airflow_operator import create_dag
from datetime import datetime
from airflow.decorators import dag,task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.subprocess import SubprocessHook

dag = create_dag(
    dag_id="ndc_desc_mart",
    catchup=False,
    concurrency=2
)

with dag:

    fda_dags = []
    dag_dependencies = ["fda_ndc","fda_unfinished","fda_excluded"]

    #note to user: dependent DAGs need to be started in the airflow UI in order for this to work
    for dag_id in dag_dependencies:
        sub_dag = TriggerDagRunOperator(
            task_id=f"{dag_id}_task",
            trigger_dag_id=dag_id,
            conf={"source_dag_id": "ndc_desc_mart", 
                  "schedule":"None"},
            wait_for_completion=True)
        fda_dags.append(sub_dag)
    
    historical_sub_dag = TriggerDagRunOperator(
        task_id = "rxnorm_historical_task",
        trigger_dag_id = "rxnorm_historical",
        conf={"source_dag_id":"ndc_desc_mart",
            "schedule":"None"},
        wait_for_completion=True)

    norm_sub_dag = TriggerDagRunOperator(
        task_id = "rxnorm_task",
        trigger_dag_id = "rxnorm",
        conf={"source_dag_id":"ndc_desc_mart",
                "schedule":"None"},
        wait_for_completion=True)

    @task
    def transform_task():
        subprocess = SubprocessHook()
        result = subprocess.run_command(['dbt', 'run', '--select', '+', 'models/marts/ndc'], cwd='/dbt/sagerx')
        print("Result from dbt:", result)
    
    transform = transform_task()


fda_dags>>norm_sub_dag>>historical_sub_dag>>transform

