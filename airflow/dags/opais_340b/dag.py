import pendulum

from airflow_operator import create_dag
from common_dag_tasks import get_data_folder
from opais_340b.dag_tasks import load

dag_id = "opais_340b"

dag = create_dag(
    dag_id=dag_id,
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    concurrency=1,
)

with dag:
    data_folder = get_data_folder(dag_id)
    load_task = load(dag_id, data_folder.as_posix())
