import os
import yaml
from airflow_operator import AirflowOperator

dir = os.path.dirname(__file__)
af = AirflowOperator()

with open(os.path.join(dir,'dag_list.yml'),'r') as dags_file:
    dag_list = yaml.safe_load(dags_file)
    print(dag_list)

# builds a dag for each data set in data_set_list
for dag in dag_list:
    globals()[dag] = af.create_dag(dag_list[dag])