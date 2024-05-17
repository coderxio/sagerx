from airflow_operator import create_dag

from rxclass.dag_tasks import get_rxcuis, extract_atc
from common_dag_tasks import  get_ds_folder, get_data_folder, transform


dag_id = 'rxclass_atc_to_product'

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
    extract_atc(rxcuis) >> transform('rxclass')