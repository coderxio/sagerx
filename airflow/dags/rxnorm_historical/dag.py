from airflow_operator import create_dag
from common_dag_tasks import  get_ds_folder, get_data_folder
from rxnorm_historical.dag_tasks import get_rxcuis, extract_ndc

dag_id = "rxnorm_historical"

dag = create_dag(
    dag_id=dag_id,
    schedule= "0 0 1 1 *",
    max_active_runs=1,
    catchup=False,
)

with dag:
    ds_folder = get_ds_folder(dag_id)
    data_folder = get_data_folder(dag_id)

    rxcuis = get_rxcuis()
    extract_ndc(rxcuis)