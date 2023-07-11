import pendulum
from azure.storage.blob import BlobServiceClient
import csv
from airflow.sensors.base import BaseSensorOperator
from airflow.sensors.filesystem import FileSensor
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
# CONTAINER_NAME = "abcd"
# CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=dfsaassdpnprdadls01;AccountKey=t6mFmpdIM/FvC8BwIV87TySyAhXOLSGS1qxTadvafil9L2sX5N9dIUi+S6I6bqXu8+gV6kkazgZO+AStCQaVxg==;EndpointSuffix=core.windows.net"
# BLOB_SERVICE_CLIENT = BlobServiceClient.from_connection_string(CONNECTION_STRING)
# CONTAINER_CLIENT = BLOB_SERVICE_CLIENT.get_container_client("abcd")
# CURRENT_DATE = pendulum.now("UTC").format("YYYYMMDD")


class CustomHook(WasbHook):
    def get_blob_list_recursive(self, container_name, endswith: str = ""):
        container = self._get_container_client(container_name)
        blob_list = []
        blobs = container.list_blobs()
        for blob in blobs:
            if blob.name.endswith(endswith):
                blob_list.append(blob.name)
        return blob_list
        
class CustomWasbSensor(BaseSensorOperator):
    def poke(self, context):
        prefix = "some/constant/prefix/"
        pattern = r".*TRGGR.*"
        ti = context['task_instance']
        provide_context=True,
        hook = CustomHook('azure_blob') 
        files = hook.get_blob_list_recursive("datafiles") 
        matched_tggr_file = list(filter(lambda file: match(pattern, file), files))
        print(matched_tggr_file)
        ti.xcom_push(key='matched_tggr_file', value=matched_tggr_file)
        if len(matched_tggr_file) > 0:
            return matched_tggr_file


      

# def parse_ctrl_files(ti=None, **context):
#     data_files_list = []
#     control_file_list = ti.xcom_pull(key="ctrl_files_list",task_ids="get_the_control_files")
#     tggr_file_list=ti.xcom_pull(key="matched_tggr_file", task_ids="waiting_for_trigger_file")
#     print("getting xcom from xcom_push",control_file_list)
#     for file in control_file_list:
#         print(file)
#         blob_client = CONTAINER_CLIENT.get_blob_client(file)
#         blob_data = blob_client.download_blob().readall().decode('utf-8')
#         reader = csv.reader(blob_data.splitlines(),    delimiter="|")
#         for row in reader:
#             data_files_dict={'file_name': row[1], 'expected_count': row[3]}
#             data_files_list.append(data_files_dict)
#     print(data_files_list)
#     validate_data_files(data_files_list,tggr_file_list,control_file_list)


# def validate_data_files(data_files_list,tggr_file_list,control_file_list):
#     print("satrting validating files")
#     all_files_present= True
#     all_matched_records = True
# # Iterate through the data files
#     for data_file in data_files_list:
#         file_name = data_file['file_name']
#         expected_count = data_file['expected_count']
#         print(f"Processing {file_name} has {expected_count} records")
#         print(type(expected_count))

#         # Get a reference to the data file
#         blob_client = CONTAINER_CLIENT.get_blob_client(file_name)
        
#         # Check if the data file exists in the container
#         if blob_client.exists():
#             # Download the data file content
#             file_data = blob_client.download_blob().readall().decode('utf-8')
            
#             # Count the number of records in the data file
#             actual_count = int(len(file_data.split('\n'))) - 2 
#             print(type(actual_count))
#             # Compare the actual count with the expected count
#             if int(actual_count) == int(expected_count):
#                 print(f"Data file '{file_name}' is present. Expected record count:{expected_count} matching with Actual Count: {actual_count}")
#             else:
#                 all_matched_records = False
#                 print(f"Data file '{file_name}' is present. Expected record count of {expected_count} not matching Actual Count: {actual_count}")
#         else:
#             all_files_present = False
#             print(f"Data file '{file_name}' does not exist in the container.")
            

# # Move files to appropriate folders
#     for file_dict in data_files_list:
#         file_name =file_dict["file_name"]
#         source_blob_client = CONTAINER_CLIENT.get_blob_client(file_name)
#         if source_blob_client.exists():
#             file = f"{file_name}_{CURRENT_DATE}"
#             destination_folder = 'external' if (all_files_present and  all_matched_records) else 'failed'
#             destination_blob_client = CONTAINER_CLIENT.get_blob_client(destination_folder + "/" + file)
#             destination_blob_client.start_copy_from_url(source_blob_client.url)
#             source_blob_client.delete_blob()
#     for ctrl_file_name in control_file_list:
#         print("Moving CONTROLS file to archive folder")
#         source_blob_client = CONTAINER_CLIENT.get_blob_client(ctrl_file_name)
#         file = f"{ctrl_file_name}_{CURRENT_DATE}"
#         destination_folder = 'archived'
#         destination_blob_client = CONTAINER_CLIENT.get_blob_client(destination_folder + "/" + file)
#         destination_blob_client.start_copy_from_url(source_blob_client.url)
#         source_blob_client.delete_blob()
#     for tggr_file in tggr_file_list:
#         print("Moving TRIGGER file to archive folder")
#         source_blob_client = CONTAINER_CLIENT.get_blob_client(tggr_file)
#         file = f"{tggr_file}_{CURRENT_DATE}"
#         destination_folder = 'archived'
#         destination_blob_client = CONTAINER_CLIENT.get_blob_client(destination_folder + "/" + file)
#         destination_blob_client.start_copy_from_url(source_blob_client.url)
#         source_blob_client.delete_blob()
#     if all_files_present==False: 
#         raise FileNotFoundError("Some file is missing...moving the files to failed folder.")
#     elif all_matched_records==False:
#         raise ValueError("Records does not match...moving the files to failed folder.")

# def process_blobs(ti=None,**context):
#     tggr_name=ti.xcom_pull(key="matched_tggr_file", task_ids="waiting_for_trigger_file")
#     c_tggr = tggr_name[0].split(".")[-1]
#     pattern = f"{c_tggr}.CF"
#     blob_list = CONTAINER_CLIENT.list_blobs()
#     matched_ctrl_files = []
#     for blob in blob_list:
#         if blob.name.endswith(pattern):
#             matched_ctrl_files.append(blob.name)
#     print(matched_ctrl_files)
#     ti.xcom_push(key="ctrl_files_list",value=matched_ctrl_files)

with DAG(
  dag_id="test_sensor", # The name that shows up in the UI
  start_date=pendulum.now(), # Start date of the DAG
  catchup=False,
) as dag:
    
   look_for_files = CustomWasbSensor(
        task_id="waiting_for_trigger_file",
    )

   look_for_files

if __name__ == "__main__":
  dag.cli()

