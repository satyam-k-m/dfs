import os
import json
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
from task_builder_final import *
from airflow.decorators import dag
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pendulum import datetime
from airflow.utils.task_group import TaskGroup
import snowflake_operations as sql_stmts
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

SNOWFLAKE_DIM_PIPELINE_TABLE = "dim_pipeline"
SNOWFLAKE_DIM_TASK_TABLE = "dim_task"
SNOWFLAKE_FCT_PIPELINE_TABLE = "fct_pipeline"
SNOWFLAKE_FCT_TASKS_TABLE = "fct_task"

SNOWFLAKE_CONN_ID = "snowflake_default"

HOME = os.environ["HOME"] # retrieve the location of your home folder
dbt_path = os.path.join(HOME,  "dfs/dbt_project/dbt/dbt/") # path to your dbt project
manifest_path = os.path.join(HOME, "dfs/dbt_project/dbt/dbt/target/manifest.json") # path to manifest.json
print(HOME)


# BLOB_NAME = "trigger.txt"
# AZURE_CONTAINER_NAME = "input"
table_list = [ "ext_chrg_info","ext_dvsn", "ext_lcl_crrncy", "ext_pos_shop", "ext_pos_trmnl", "ext_pos_dscnt", "ext_pos_tndr", "ext_pos_tx", "ext_pos_tx_dct", "ext_pos_tx_ln", "ext_rfnd_tx_rf", "ext_tndr_type", "ext_tx_type"]
SQL_REFRESH_STATEMENT = "ALTER EXTERNAL TABLE INSIGHT_DEV.INS_BKP.%(table_name)s REFRESH"
SQL_LIST = [ SQL_REFRESH_STATEMENT % {"table_name": table_name}  for table_name in table_list ]
SQL_MULTIPLE_STMTS = "; ".join(SQL_LIST)

with open(manifest_path) as f: # Open manifest.json
  manifest = json.load(f) # Load its contents into a Python Dictionary
  nodes = manifest["nodes"] # Extract just the nodes
  sources = manifest["sources"]

# Build an Airflow DAG

node_key = 'model.development.silver_division'

with DAG(
  dag_id="test_model_lineage", # The name that shows up in the UI
  start_date=pendulum.now(), # Start date of the DAG
  catchup=False,
) as dag:
    
    Variable.set("start_date_var", pendulum.now())
   
    start_date_var = Variable.get("start_date_var")
    with TaskGroup("dbt_task_group") as dbt_tg:
        task_builder = TaskBuilder(nodes, sources, dbt_path, start_date_var)
        dbt_tasks = dict()

        for node_id, node_info in nodes.items():
                if len(node_info['sources']):
                    source_id = node_info["depends_on"]["nodes"][0]

                    dbt_tasks[source_id] = task_builder.build_source_task_group(node_info)
                dbt_tasks[node_id] = task_builder.build_node_task_group(node_id, node_info)

            # Define relationships between Operators
        for node_id, node_info in nodes.items():
            
                if len(node_info["sources"]):
                    source_id = node_info["depends_on"]["nodes"][0]
                    node = [x for x in sources.keys() if x == source_id]
                    upstream_nodes = node
                else:
                    upstream_nodes = node_info["depends_on"]["nodes"]
                if upstream_nodes and (node_id == node_key):
                    for upstream_node in upstream_nodes:
                        dbt_tasks[upstream_node] >> dbt_tasks[node_id]


    dbt_tg

if __name__ == "__main__":
  dag.cli()

