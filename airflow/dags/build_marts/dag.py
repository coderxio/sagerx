import pendulum

from airflow_operator import create_dag
from common_dag_tasks import get_most_recent_dag_run
from airflow.decorators import dag,task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from common_dag_tasks import run_subprocess_command

def run_dag_condition(dag_id):
    last_run = get_most_recent_dag_run(dag_id)
    # if a DAG from the list of dependencies is more than 5 days stale
    if last_run is None or (pendulum.now() - last_run.execution_date).days > 5:
        if last_run is not None:
            print(f'{dag_id} was last run {last_run.execution_date}.')
        else:
            print(f'{dag_id} has never been run.')
        return True
    else:
        print(f"{dag_id} was last run {last_run.execution_date} and will now skipped.")
        return False

def get_dag_list():
    list_of_dags = []
    dag_dependencies = ["fda_ndc","fda_unfinished","fda_excluded","rxnorm","rxclass_atc_to_product","rxnorm_historical"]
    for dag in dag_dependencies:
        if run_dag_condition(dag):
            list_of_dags.append(dag)
    print(f'list of dags to run{list_of_dags}')
    return list_of_dags

dag = create_dag(
    dag_id="build_marts",
    schedule = "0 5 * * 2", #every tuesday at 5:00am
    catchup=False,
    concurrency=2
)
with dag:

    # PLEASE NOTE this block will execute each of the DAGs in turn;
    # When all are being run consecutively, the process will take in excess of 60 minutes
    
    @task
    def execute_external_dag_list(**kwargs): 
        dags_list = get_dag_list()
        for ex_dag in dags_list:
            print(f'triggering {ex_dag}')
            dag_task = TriggerDagRunOperator(
                task_id=f"{ex_dag}_task",
                trigger_dag_id=ex_dag,
                conf={"source_dag_id": "build_marts"},
                wait_for_completion=True)
            dag_task.execute(context=kwargs)

    # Once DBT freshness metrics are implemented, this task can be updated
    @task
    def transform_tasks():
        run_subprocess_command(['dbt', 'run', '--select', '+models/marts/ndc'], cwd='/dbt/sagerx')
        run_subprocess_command(['dbt', 'run', '--select', '+models/marts/classification'], cwd='/dbt/sagerx')
        run_subprocess_command(['dbt', 'run', '--select', '+models/marts/products'], cwd='/dbt/sagerx')

    execute_external_dag_list() >> transform_tasks()
