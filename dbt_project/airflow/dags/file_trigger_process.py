import pendulum
from azure.storage.blob import BlobServiceClient
import csv
from airflow.sensors.base import BaseSensorOperator
from re import match
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

# tggr_file = 'in_demo_trgg_in_mac'
CONTAINER_NAME = "output"
CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=acstorage901;AccountKey=k+DzqtOi5TQd2Mq/vyi3W43L1LXV0zcAw8OwMVRS/cMOnW/dGFrLt1aDD/9CNTg0/zv1Un4rIDwI+AStY5LJjQ==;EndpointSuffix=core.windows.net"
BLOB_SERVICE_CLIENT = BlobServiceClient.from_connection_string(CONNECTION_STRING)
CONTAINER_CLIENT = BLOB_SERVICE_CLIENT.get_container_client(CONTAINER_NAME)
CURRENT_DATE = pendulum.now("UTC").format("YYYYMMDD")


class CustomWasbSensor(BaseSensorOperator):
    def poke(self, context):
        prefix = "some/constant/prefix/"
        pattern = r".*TRGGR.*"
        ti = context['task_instance']
        provide_context=True,
        hook = WasbHook('wasb_connection_id') 
        files = hook.get_blobs_list("output") 
        matched_tggr_file = list(filter(lambda file_path: match(pattern, file_path.replace(prefix, "")), files))

        ti.xcom_push(key='matched_tggr_file', value=matched_tggr_file)
        print(matched_tggr_file)
        if len(matched_tggr_file) > 0:
            return matched_tggr_file

def parse_ctrl_files(ti=None, **context):
    data_files = []
    control_file_list = ti.xcom_pull(key="ctrl_files_list",task_ids="process_blobs_task")
    tggr_file_list=ti.xcom_pull(key="matched_tggr_file", task_ids="wait_for_blob")
    print("getting xcom from xcom_push",control_file_list)
    for file in control_file_list:
        print(file)
        blob_client = CONTAINER_CLIENT.get_blob_client(file)

        blob_data = blob_client.download_blob().readall().decode('utf-8')
        reader = csv.reader(blob_data.splitlines(),    delimiter="|")
        for row in reader:
            data_files.append(row[1])
    print(data_files)
    validate_data_files(data_files,tggr_file_list,control_file_list)


def validate_data_files(data_files_list,tggr_file_list,control_file_list):
    print("satrting validating files")
    all_files_present = True
    for file in data_files_list:
        print("checking "+file)
        blob_client = CONTAINER_CLIENT.get_blob_client(file)
        if not blob_client.exists():
            print("file doest not exists...breaking")
            all_files_present = False
            break
# Move files to appropriate folders
    for file_name in data_files_list:
        source_blob_client = CONTAINER_CLIENT.get_blob_client(file_name)
        if source_blob_client.exists():
            file = f"{file_name}_{CURRENT_DATE}"
            destination_folder = 'success' if all_files_present else 'failed'
            destination_blob_client = CONTAINER_CLIENT.get_blob_client(destination_folder + "/" + file)
            destination_blob_client.start_copy_from_url(source_blob_client.url)
            source_blob_client.delete_blob()
    for ctrl_file_name in control_file_list:
        source_blob_client = CONTAINER_CLIENT.get_blob_client(ctrl_file_name)
        file = f"{ctrl_file_name}_{CURRENT_DATE}"
        destination_folder = 'archived'
        destination_blob_client = CONTAINER_CLIENT.get_blob_client(destination_folder + "/" + file)
        destination_blob_client.start_copy_from_url(source_blob_client.url)
        source_blob_client.delete_blob()
    for tggr_file in tggr_file_list:
        source_blob_client = CONTAINER_CLIENT.get_blob_client(tggr_file)
        file = f"{tggr_file}_{CURRENT_DATE}"
        destination_folder = 'archived'
        destination_blob_client = CONTAINER_CLIENT.get_blob_client(destination_folder + "/" + file)
        destination_blob_client.start_copy_from_url(source_blob_client.url)
        source_blob_client.delete_blob()

def process_blobs(ti=None,**context):
    tggr_name=ti.xcom_pull(key="matched_tggr_file", task_ids="wait_for_blob")
    c_tggr = tggr_name[0].split(".")[-1]
    pattern = f"{c_tggr}.CF"
    blob_list = CONTAINER_CLIENT.list_blobs()
    matched_ctrl_files = []
    for blob in blob_list:
        if blob.name.endswith(pattern):
            matched_ctrl_files.append(blob.name)
    print(matched_ctrl_files)
    ti.xcom_push(key="ctrl_files_list",value=matched_ctrl_files)
