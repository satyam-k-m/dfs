import pendulum
from azure.storage.blob import BlobServiceClient
import csv
from airflow.sensors.base import BaseSensorOperator
from re import match
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import pendulum
from airflow import DAG
from airflow.decorators import dag
from pendulum import datetime
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun  import TriggerDagRunOperator

# tggr_file = 'in_demo_trgg_in_mac'
CONTAINER_NAME = "abcd"
CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=dfsaassdpnprdadls01;AccountKey=t6mFmpdIM/FvC8BwIV87TySyAhXOLSGS1qxTadvafil9L2sX5N9dIUi+S6I6bqXu8+gV6kkazgZO+AStCQaVxg==;EndpointSuffix=core.windows.net"
BLOB_SERVICE_CLIENT = BlobServiceClient.from_connection_string(CONNECTION_STRING)
CONTAINER_CLIENT = BLOB_SERVICE_CLIENT.get_container_client(CONTAINER_NAME)
CURRENT_DATE = pendulum.now("UTC").format("YYYYMMDD")

class CustomWasbSensor(BaseSensorOperator):
    def poke(self, context):
        prefix = "some/constant/prefix/"
        pattern = r".*TRGGR.*"
        ti = context['task_instance']
        provide_context=True,
        hook = WasbHook('dfs_blob') 
        files = hook.get_blobs_list(CONTAINER_NAME) 
        matched_tggr_file = list(filter(lambda file_path: match(pattern, file_path.replace(prefix, "")), files))

        ti.xcom_push(key='matched_tggr_file', value=matched_tggr_file)
        print(matched_tggr_file)
        if len(matched_tggr_file) > 0:
            return matched_tggr_file

with DAG(
  dag_id="test_dfs_blob", # The name that shows up in the UI
  start_date=pendulum.now(), # Start date of the DAG
  catchup=False,
) as dag:
    
    wait_for_blob = CustomWasbSensor(
        task_id="waiting_for_trigger_file",
    )

   
    wait_for_blob 

if __name__ == "__main__":
  dag.cli()

