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
CONTAINER_NAME = "output"
CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=acstorage901;AccountKey=k+DzqtOi5TQd2Mq/vyi3W43L1LXV0zcAw8OwMVRS/cMOnW/dGFrLt1aDD/9CNTg0/zv1Un4rIDwI+AStY5LJjQ==;EndpointSuffix=core.windows.net"
BLOB_SERVICE_CLIENT = BlobServiceClient.from_connection_string(CONNECTION_STRING)
CONTAINER_CLIENT = BLOB_SERVICE_CLIENT.get_container_client(CONTAINER_NAME)
CURRENT_DATE = pendulum.now("UTC").format("YYYYMMDD")

def set_dag_vars():
    Variable.set("start_date_var", pendulum.now())

class CustomWasbSensor(BaseSensorOperator):
    def poke(self, context):
        prefix = "some/constant/prefix/"
        pattern = r".*TRGGR.*"
        ti = context['task_instance']
        provide_context=True,
        hook = WasbHook('azure_blob') 
        files = hook.get_blobs_list("output") 
        matched_tggr_file = list(filter(lambda file_path: match(pattern, file_path.replace(prefix, "")), files))

        ti.xcom_push(key='matched_tggr_file', value=matched_tggr_file)
        print(matched_tggr_file)
        if len(matched_tggr_file) > 0:
            return matched_tggr_file

def parse_ctrl_files(ti=None, **context):
    data_files_list = []
    control_file_list = ti.xcom_pull(key="ctrl_files_list",task_ids="get_the_control_files")
    tggr_file_list=ti.xcom_pull(key="matched_tggr_file", task_ids="waiting_for_trigger_file")
    print("getting xcom from xcom_push",control_file_list)
    for file in control_file_list:
        print(file)
        blob_client = CONTAINER_CLIENT.get_blob_client(file)
        blob_data = blob_client.download_blob().readall().decode('utf-8')
        reader = csv.reader(blob_data.splitlines(),    delimiter="|")
        for row in reader:
            data_files_dict={'file_name': row[1], 'expected_count': row[3]}
            data_files_list.append(data_files_dict)
    print(data_files_list)
    validate_data_files(data_files_list,tggr_file_list,control_file_list)


def validate_data_files(data_files_list,tggr_file_list,control_file_list):
    print("satrting validating files")
    all_files_present= True
    all_matched_records = True
# Iterate through the data files
    for data_file in data_files_list:
        file_name = data_file['file_name']
        expected_count = data_file['expected_count']
        print(f"Processing {file_name} has {expected_count} records")
        print(type(expected_count))

        # Get a reference to the data file
        blob_client = CONTAINER_CLIENT.get_blob_client(file_name)
        
        # Check if the data file exists in the container
        if blob_client.exists():
            # Download the data file content
            file_data = blob_client.download_blob().readall().decode('utf-8')
            
            # Count the number of records in the data file
            actual_count = int(len(file_data.split('\n'))) - 1
            print(type(actual_count))
            # Compare the actual count with the expected count
            if int(actual_count) == int(expected_count):
                print(f"Data file '{file_name}' is present. Expected record count:{expected_count} Actual Count: {actual_count}")
            else:
                all_matched_records = False
                print(f"Data file '{file_name}' is present. Expected record count of {expected_count} Actual Count: {actual_count}")
        else:
            all_files_present = False
            print(f"Data file '{file_name}' does not exist in the container.")
            

# Move files to appropriate folders
    for file_dict in data_files_list:
        file_name =file_dict["file_name"]
        source_blob_client = CONTAINER_CLIENT.get_blob_client(file_name)
        if source_blob_client.exists():
            file = f"{file_name}_{CURRENT_DATE}"
            destination_folder = 'success' if (all_files_present or all_matched_records) else 'failed'
            destination_blob_client = CONTAINER_CLIENT.get_blob_client(destination_folder + "/" + file)
            destination_blob_client.start_copy_from_url(source_blob_client.url)
            source_blob_client.delete_blob()
    for ctrl_file_name in control_file_list:
        print("Moving CONTROLS file to archive folder")
        source_blob_client = CONTAINER_CLIENT.get_blob_client(ctrl_file_name)
        file = f"{ctrl_file_name}_{CURRENT_DATE}"
        destination_folder = 'archived'
        destination_blob_client = CONTAINER_CLIENT.get_blob_client(destination_folder + "/" + file)
        destination_blob_client.start_copy_from_url(source_blob_client.url)
        source_blob_client.delete_blob()
    for tggr_file in tggr_file_list:
        print("Moving TRIGGER file to archive folder")
        source_blob_client = CONTAINER_CLIENT.get_blob_client(tggr_file)
        file = f"{tggr_file}_{CURRENT_DATE}"
        destination_folder = 'archived'
        destination_blob_client = CONTAINER_CLIENT.get_blob_client(destination_folder + "/" + file)
        destination_blob_client.start_copy_from_url(source_blob_client.url)
        source_blob_client.delete_blob()
    if all_files_present==False: 
        raise FileNotFoundError("Some file is missing...moving the files to failed folder.")
    elif all_matched_records==False:
        raise ValueError("Records does not match...moving the files to failed folder.")

def process_blobs(ti=None,**context):
    tggr_name=ti.xcom_pull(key="matched_tggr_file", task_ids="waiting_for_trigger_file")
    c_tggr = tggr_name[0].split(".")[-1]
    pattern = f"{c_tggr}.CF"
    blob_list = CONTAINER_CLIENT.list_blobs()
    matched_ctrl_files = []
    for blob in blob_list:
        if blob.name.endswith(pattern):
            matched_ctrl_files.append(blob.name)
    print(matched_ctrl_files)
    ti.xcom_push(key="ctrl_files_list",value=matched_ctrl_files)

with DAG(
  dag_id="dfs_file_processor", # The name that shows up in the UI
  start_date=pendulum.now(), # Start date of the DAG
  catchup=False,
) as dag:
    
    # python_op = PythonOperator(
    #     task_id="set_dag_vars",
    #     python_callable=set_dag_vars
    # )

    # start_date_var = Variable.get("start_date_var")

    # [START how_to_wait_for_blob]
    wait_for_blob = CustomWasbSensor(
        task_id="waiting_for_trigger_file",
    )

    process_blobs_task = PythonOperator(
        task_id='get_the_control_files',
        python_callable=process_blobs,
        provide_context =True,
    )
    parse_control_files = PythonOperator(
        task_id='parse_and_validate_control_files',
        python_callable=parse_ctrl_files,
        provide_context =True
    )

    trigger_dbt_dag = TriggerDagRunOperator(
            task_id = "trigger_dbt_model",
            trigger_dag_id= "dfs_pipeline"
            
        )

    trigger_file_dag = TriggerDagRunOperator(
            task_id = "wait_for_trigger_file_again",
            trigger_dag_id= "dfs_file_processor"
            
        )
    wait_for_blob >> process_blobs_task >> parse_control_files >> trigger_dbt_dag >> trigger_file_dag

if __name__ == "__main__":
  dag.cli()

