import os
import json
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor

HOME = os.environ["HOME"] # retrieve the location of your home folder
dbt_path = os.path.join(HOME,  "airflow/dags/dbt/") # path to your dbt project
manifest_path = os.path.join(HOME, "airflow/target/manifest.json") # path to manifest.json
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
with DAG(
  dag_id="airflow_dbt", # The name that shows up in the UI
  start_date=start_date, # Start date of the DAG
  catchup=False,
) as dag:

  # Create a dict of Operators
  with TaskGroup("dbt_task", tooltip="DBT tasks") as dbt_task:
    dbt_tasks = dict()
    for node_id, node_info in nodes.items():
        if len(node_info['sources']):
          source_id = node_info["depends_on"]["nodes"][0]
          item = sources[source_id]


          dbt_tasks[source_id] = BashOperator(
              task_id = ".".join(
                [
                    item["resource_type"],
                    item["package_name"],
                    item['source_name'],
                    item["name"],
                ]
            ),
          bash_command=f"cd {dbt_path}" # Go to the path containing your dbt project
          + f" && dbt run --models {node_info['name']} --vars '{start_date_dag_var}'", # run the model!
          )


        dbt_tasks[node_id] = BashOperator(
            task_id= ".".join(
                [
                    node_info["resource_type"],
                    node_info["package_name"],
                    node_info["name"],
                ]
            ),

            bash_command=f"cd {dbt_path}" # Go to the path containing your dbt project
            + f" && dbt run --models {node_info['name']} --vars '{start_date_dag_var}'", # run the model!
        )


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

  wait_for_blob >> dbt_task


if __name__ == "__main__":
  dag.cli()