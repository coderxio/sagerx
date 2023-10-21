import pendulum
from airflow_operator import create_dag
from common_airflow_tasks import  extract, load,transform

dag_id = "fda_ndc"
url= "https://www.accessdata.fda.gov/cder/ndctext.zip"

dag = create_dag(
    dag_id=dag_id,
    schedule="0 4 * * *",
    start_date=pendulum.yesterday(),
    catchup=False,
    concurrency=2,
)

with dag:
    extract_task = extract(dag_id,url)
    load_task = load(dag_id,extract_task)
    transform_task = transform(dag_id)
    extract_task >> load_task >> transform_task