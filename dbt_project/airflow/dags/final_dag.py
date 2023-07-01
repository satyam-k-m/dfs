import os
import json
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
#from test_task_builder import *
from task_builder_final import *
# from task_builder import *
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
dbt_path = os.path.join(HOME,  "dbt_project/dbt/dbt/") # path to your dbt project
manifest_path = os.path.join(HOME, "dbt_project/dbt/dbt/target/manifest.json") # path to manifest.json
print(HOME)


BLOB_NAME = "trigger.txt"
AZURE_CONTAINER_NAME = "input"

with open(manifest_path) as f: # Open manifest.json
  manifest = json.load(f) # Load its contents into a Python Dictionary
  nodes = manifest["nodes"] # Extract just the nodes
  sources = manifest["sources"]



# Build an Airflow DAG



def set_dag_vars():
    Variable.set("start_date_var", pendulum.now())


with DAG(
  dag_id="dfs_pipeline", # The name that shows up in the UI
  start_date=pendulum.now(), # Start date of the DAG
  catchup=False,
) as dag:
    
    python_op = PythonOperator(
        task_id="set_dag_vars",
        python_callable=set_dag_vars
    )

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
            if upstream_nodes:
                for upstream_node in upstream_nodes:
                    dbt_tasks[upstream_node] >> dbt_tasks[node_id]

    wait_for_blob = WasbBlobSensor(
        task_id="wait_for_blob",
        wasb_conn_id="azure_blob",
        container_name=AZURE_CONTAINER_NAME,
        blob_name=BLOB_NAME,
    )
    with TaskGroup("setup_audit_config") as tg:

        read_config_table = SnowflakeOperator(
            task_id = 'initialize_run_time_audit',
            sql = sql_stmts.read_dim_table,
            params = {"table_name": SNOWFLAKE_DIM_TASK_TABLE}

            )

        refresh_stage = SnowflakeOperator(
            task_id = 'refresh_stage_table',
            sql = sql_stmts.refresh_stage
            )
        


        create_fact_pipeline = SnowflakeOperator(
            task_id = "create_fct_pipeline",
            sql = sql_stmts.create_fct_pipeline,
            params = {"table_name":SNOWFLAKE_FCT_PIPELINE_TABLE}
        )
        create_fact_task = SnowflakeOperator(
            task_id = "create_fct_task",
            sql = sql_stmts.create_fct_task,
            params = {"table_name":SNOWFLAKE_FCT_TASKS_TABLE}
        )

        insert_pipeline_fact = SnowflakeOperator(
            task_id = "insert_fct_pipeline",
            sql = sql_stmts.insert_pipeline_status,
            params = {"fact_table_name":SNOWFLAKE_FCT_PIPELINE_TABLE, 
                      "dim_table_name":SNOWFLAKE_DIM_PIPELINE_TABLE, 
                      "pipeline_id": dag.dag_id, "run_ts":start_date_var }
        )
        insert_task_fact = SnowflakeOperator(
            task_id = "insert_fct_task",
            sql = sql_stmts.insert_task_status,
            params = {"fact_table_name":SNOWFLAKE_FCT_TASKS_TABLE, 
                      "dim_table_name":SNOWFLAKE_DIM_TASK_TABLE, 
                      "pipeline_id": dag.dag_id,"run_ts":start_date_var }
        )


        refresh_stage >> read_config_table >> [create_fact_pipeline, create_fact_task]  
        read_config_table >> [insert_pipeline_fact, insert_task_fact]


    update_fact_pipeline_pre = SnowflakeOperator(
        task_id = "update_fct_pipeline_pre_execution",
        sql = sql_stmts.update_pipeline_status_pre,
        params = {"table_name":SNOWFLAKE_FCT_PIPELINE_TABLE, "status_cd":"running", 
                  "pipeline_id":dag.dag_id, "run_ts":start_date_var}
    )
    update_fact_pipeline_post = SnowflakeOperator(
        task_id = "update_fct_pipeline_post_execution",
        sql = sql_stmts.update_pipeline_status_post,
        params = {"table_name":SNOWFLAKE_FCT_PIPELINE_TABLE, "status_cd":"success", 
                  "pipeline_id":dag.dag_id, "run_ts":start_date_var}
    )


    wait_for_blob >> python_op >> tg >> update_fact_pipeline_pre >> dbt_tg >> update_fact_pipeline_post


if __name__ == "__main__":
  dag.cli()


        


