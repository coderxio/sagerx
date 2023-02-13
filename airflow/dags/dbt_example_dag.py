import pendulum

from airflow.decorators import dag, task
from airflow.hooks.subprocess import SubprocessHook


@dag(
    schedule=None,
    start_date=pendulum.today(),
    catchup=False,
)
def dbt_example_dag():

    @task
    def hello_world():
        print('hello world')

    @task
    def execute_dbt():
        subprocess = SubprocessHook()
        result = subprocess.run_command(['dbt', 'run'], cwd='/dbt/sagerx')
        print(result.exit_code)

    hello_world() >> execute_dbt()

dbt_example_dag()
