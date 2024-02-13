from airflow_operator import create_dag
from common_dag_tasks import  extract, get_ordered_sql_tasks, get_ds_folder
from dailymed_daily.dag_tasks import process_dailymed

dag_id = "dailymed_daily"

dag = create_dag(
    dag_id=dag_id,
    schedule= "45 7 * * 1-5",  # at 7:45 am once daily
    max_active_runs=1,
    concurrency=2,
)


with dag:
    file_name = "{{ macros.ds_format(macros.ds_add(ds,-1), '%Y-%m-%d', '%m%d%Y') }}"
    url = f"https://dailymed-data.nlm.nih.gov/public-release-files/dm_spl_daily_update_{file_name}.zip"
    
    ds_folder = get_ds_folder(dag_id)

    extract_task = extract(dag_id,url)
    process_dailymed(dag_id=dag_id,data_folder=extract_task)