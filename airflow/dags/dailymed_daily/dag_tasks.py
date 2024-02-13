from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task
from common_dag_tasks import  get_ds_folder

@task
def process_dailymed(dag_id, data_folder):
    import zipfile
    import re
    import os
    import pandas as pd
    import sqlalchemy

    db_conn_string = os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"]
    db_conn = sqlalchemy.create_engine(db_conn_string)
    xslt = get_ds_folder(dag_id) / "dailymed_prescription.xsl"

    data_folder = (data_folder / "prescription")

    for subfile in data_folder.infolist():
        zipfolder = zipfile.ZipFile(subfile)
        if re.match(".*.xml", subfile.filename):
            new_file = data_folder.extract(subfile, data_folder)
            # xslt transform
            xml_content = transform_xml(new_file, xslt)
            os.remove(new_file)
            df = pd.DataFrame(
                columns=["spl", "file_name", "xml_content"],
                data=[[data_folder, subfile.filename, xml_content]],
            )
            df.to_sql(
                "dailymed_daily",
                schema="datasource",
                con=db_conn,
                if_exists="append",
                index=False,
            )


#     tl = []
#     # Task to download data from web location
#     tl.append(
#         PythonOperator(
#             task_id=f"get_{dag_id}",
#             python_callable=get_dataset,
#             op_kwargs={"ds_url": url, "data_folder": data_folder},
#         )
#     )

#     # Task to load data into source db schema
#     tl.append(
#         PythonOperator(
#             task_id=f"load_{dag_id}",
#             python_callable=process_dailymed,
#             op_kwargs={
#                 "data_folder": data_folder,
#                 "xslt": ds_folder / "dailymed_prescription.xsl",
#             },
#         )
#     )

#     for sql in get_sql_list("staging-", ds_folder):
#         sql_path = ds_folder / sql
#         tl.append(
#             PostgresOperator(
#                 task_id=sql,
#                 postgres_conn_id="postgres_default",
#                 sql=read_sql_file(sql_path),
#             )
#         )

#     for i in range(len(tl)):
#         if i not in [0]:
#             tl[i - 1] >> tl[i]