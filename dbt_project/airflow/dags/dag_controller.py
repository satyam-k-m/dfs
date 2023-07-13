import os
import sys
from airflow.models import Variable
import json
import importlib
import final_dag

# os.environ['PYTHONPATH']="$HOME/naman/dfs/dbt_project/airflow"
# sys.path.insert(0,os.environ['PYTHONPATH'])


HOME = os.environ["HOME"] # retrieve the location of your home folder
config_file_path = os.path.join(HOME,"dfs/dbt_project/airflow/dags/config.json")

with open(config_file_path, 'r') as json_file:
    # Load the JSON data
    config = json.load(json_file)

for key, value in config.items():
    if "div" in value:
        divisions = value["div"]
        for div in divisions:
            dag_id = f'dbt_process_{key}_{div}'
            final_dag.dbt_dag_generator(dag_id)
