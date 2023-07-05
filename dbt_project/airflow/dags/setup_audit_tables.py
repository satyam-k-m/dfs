import os
import json
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
from task_builder import *
from airflow.decorators import dag
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pendulum import datetime
from airflow.utils.task_group import TaskGroup
import snowflake_operations as sql_stmts

SNOWFLAKE_DIM_PIPELLINE_TABLE = "dim_pipeline"
SNOWFLAKE_DIM_TASK_TABLE = "dim_task"
SNOWFLAKE_FCT_PIPELINE_TABLE = "fct_pipeline"
SNOWFLAKE_FCT_TASKS_TABLE = "fct_task"

SNOWFLAKE_CONN_ID = "snowflake_default"

HOME = os.environ["HOME"] # retrieve the location of your home folder
dbt_path = os.path.join(HOME,  "dfs/dbt_project/dbt/dbt/dbt/") # path to your dbt project
manifest_path = os.path.join(HOME, "dfs/dbt_project/dbt/dbt/target/manifest.json") # path to manifest.json
print(HOME)


BLOB_NAME = "trigger.txt"
AZURE_CONTAINER_NAME = "input"

with open(manifest_path) as f: # Open manifest.json
  manifest = json.load(f) # Load its contents into a Python Dictionary
  nodes = manifest["nodes"] # Extract just the nodes
  sources = manifest["sources"]


start_date = pendulum.today()
start_date_dag_var = {'run_time_stamp':start_date}
# Build an Airflow DAG

task_builder = TaskBuilder(nodes, sources, dbt_path)

with DAG(
  dag_id="dfs_pipeline_setup_audit", # The name that shows up in the UI
  start_date=start_date, # Start date of the DAG
  catchup=False,
) as dag:
    with TaskGroup("dbt_task_group") as dbt_tg:
        dbt_tasks = dict()
        for node_id, node_info in nodes.items():
            if len(node_info['sources']):
                source_id = node_info["depends_on"]["nodes"][0]

                dbt_tasks[source_id] = task_builder.build_source_task_group( node_info)
            dbt_tasks[node_id] = task_builder.build_node_task_group(node_id, node_info)

            # Define relationships between Operators
        for node_id, node_info in nodes.items():
            if len(node_info["sources"]):
                source_id = node_info["depends_on"]["nodes"][0]
                node = [x for x in sources.keys() if x == source_id]
                upstream_nodes = node
            else:
                upstream_nodes = node_info["depends_on"]["nodes"]
            if upstream_nodes:
                for upstream_node in upstream_nodes:
                    dbt_tasks[upstream_node] >> dbt_tasks[node_id]


    with TaskGroup("setup_pipeline_table") as tg_pipeline:

        create_dim_pipeline_table = SnowflakeOperator(
            task_id="create_dim_pipeline_table",
            sql=sql_stmts.create_dim_pipeline,
            params={"table_name": SNOWFLAKE_DIM_PIPELLINE_TABLE},
        )

        create_dim_task_table = SnowflakeOperator(
            task_id="create_dim_task_table",
            sql=sql_stmts.create_dim_task,
            params={"table_name": SNOWFLAKE_DIM_TASK_TABLE},
        )

        insert_into_dim_pipeline_table = SnowflakeOperator(
        task_id="insert_dim_pipeline_table",
        sql=sql_stmts.insert_dim_pipeline,
        params={"table_name": SNOWFLAKE_DIM_PIPELLINE_TABLE, "pipeline_id": dag.dag_id, "pipeline_name":"dbt_airflow"},
        )

        create_dim_pipeline_table >> create_dim_task_table >> insert_into_dim_pipeline_table

    read_config_table = SnowflakeOperator(
        task_id = 'read_pipeline_table',
       sql = sql_stmts.read_dim_table,
       params = {"table_name": SNOWFLAKE_DIM_PIPELLINE_TABLE}

    )

    tg_pipeline >> read_config_table >>  dbt_tg


if __name__ == "__main__":
  dag.cli()


        


