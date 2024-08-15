from airflow_operator import create_dag

from sagerx import get_rxcuis
from rxclass.dag_tasks import extract_rxclass
from common_dag_tasks import  get_ds_folder, get_data_folder, transform

dag_id = 'rxclass'

dag = create_dag(
    dag_id=dag_id,
    schedule= "0 0 1 1 *",
    max_active_runs=1,
    catchup=False,
)


with dag:
    ds_folder = get_ds_folder(dag_id)
    data_folder = get_data_folder(dag_id)
    all_ttys = ['IN','PIN','MIN','SCDC','SCDF','SCDFP','SCDG','SCDGP','SCD','GPCK','BN','SBDC','SBDF','SBDFP','SBDG','SBD','BPCK']
    rxcuis = get_rxcuis(ttys=all_ttys)
    rxcuis >> extract_rxclass(rxcuis)
