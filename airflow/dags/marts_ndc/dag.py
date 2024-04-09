import pendulum
import logging

from airflow_operator import create_dag
from common_dag_tasks import get_most_recent_dag_run
from datetime import timedelta
from airflow.exceptions import AirflowSkipException
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.decorators import dag,task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.subprocess import SubprocessHook


def run_dag_condition(dag_id):
    last_run = get_most_recent_dag_run(dag_id)
    if not last_run is None or (pendulum.now() - last_run.execution_date).minutes > 120:
        raise AirflowSkipException()
        # logging.info(f"{dag_id} WILL be executed") 
        # return True
    # else:
    #     logging.info(f"{dag_id} was last run at {last_run.execution_date} and WILL NOT be executed") 
    #     return False

dag = create_dag(
    dag_id="build_marts",
    catchup=False,
    concurrency=2
)

with dag:

    list_of_dags = []
    dag_dependencies = ["fda_ndc","fda_unfinished","fda_excluded","rxnorm","rxclass_atc_to_product","rxnorm_historical"]
    for dag_id in dag_dependencies:
        # if run_dag_condition(dag_id):
        #     print(f'{dag_id} being run')
        sub_dag = TriggerDagRunOperator(
            task_id=f"{dag_id}_task",
            trigger_dag_id=dag_id,
            conf={"source_dag_id": "build_marts", 
                "schedule":"None"},
            wait_for_completion= True,
            pre_execute=run_dag_condition(dag_id))
        list_of_dags.append(sub_dag)

        # sub_dag_sensor =  ExternalTaskSensor(
        #     task_id=f'wait_for_{dag_id}',
        #     external_dag_id=dag_id,
        #     external_task_id='transform_task',
        #     execution_delta=timedelta(minutes=90),
        #     pre_execute=run_dag_condition(dag_id)) 
        # list_of_dags.append(sub_dag_sensor)

    @task
    def transform_tasks():
        ndc_subprocess = SubprocessHook()
        result = ndc_subprocess.run_command(['dbt', 'run', '--select', 'models/marts/ndc'], cwd='/dbt/sagerx')
        print("Result from dbt:", result)
        atc_subprocess = SubprocessHook()
        result = atc_subprocess.run_command(['dbt', 'run', '--select', 'models/marts/classification'], cwd='/dbt/sagerx')
        print("Result from dbt:", result)
    transform = transform_tasks()

list_of_dags.append(transform)

for i in range(len(list_of_dags) - 1):
    list_of_dags[i].set_downstream(list_of_dags[i + 1])

