from azure.storage.filedatalake import DataLakeServiceClient
from airflow.sensors.base import BaseSensorOperator
import os
import json
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task, task_group
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
from airflow.sensors.base import BaseSensorOperator
from re import match
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

from airflow.decorators import dag
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pendulum import datetime
from airflow.utils.task_group import TaskGroup
import snowflake_operations as sql_stmts
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

# class AzureDataLakeSensor(BaseSensorOperator):
#     def __init__(self, path, filename, account_url, access_token, **kwargs):
#       super().__init__(**kwargs)
#       self._client = DataLakeServiceClient(
#             account_url=account_url,
#             credential=access_token
#       )

#       self.path = path
#       self.filename = filename

#     def poke(self, context):
#         container = self._client.get_file_system_client(file_system="raw")
#         dir_client = container.get_directory_client(self.path)
#         file = dir_client.get_file_client(self.filename)
#         return file.exists()


class CustomWasbSensor(BaseSensorOperator):
    #template_fields = ('wildcard', 'container_name', 'wasb_conn_id')
    def poke(self, context):
        prefix = "some/constant/prefix/" # used to reduce the list size, you can skip it if you have regex in all the prefix
        wildcard = "[a-zA-Z0-9_]*trgg[a-zA-Z0-9]*" # */images/*.jpg
        #wildcard = "check.txt"
        hook = WasbHook("azure_blob") # use a connection
        #files = hook.check_for_blob(self.container_name, self.blob_name, **self.check_options)
        files = hook.get_blobs_list("output") # for some use cases you can use a delimiter like delimiter='.jpg'
        matched_files = list(filter(lambda file_path: match(wildcard, file_path.replace(prefix, "")), files))
        
        print(matched_files)
        if len(matched_files) > 0: 
            return matched_files



with DAG(
    "file_pattern_dag",
    schedule_interval=None,
    start_date=pendulum.now(),  # Override to match your needs
) as dag:

    # [START how_to_wait_for_blob]
    wait_for_blob = CustomWasbSensor(
        task_id="wait_for_blob",
        # wasb_conn_id="azure_blob",
        # container_name="output",
        # wildcard="checksum",
    )
    
    wait_for_blob

if __name__ == "__main__":
  dag.cli()
