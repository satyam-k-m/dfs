import pendulum
from azure.storage.blob import BlobServiceClient
import csv
from airflow.sensors.base import BaseSensorOperator
from parse_and_validate import process_blobs
from re import match
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import pendulum
from airflow import DAG
from airflow.decorators import dag
from pendulum import datetime
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun  import TriggerDagRunOperator
from file_processor import FileProcessor


CONTAINER_NAME = "dfsfiles"
CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=skmstorage01;AccountKey=82AORD4SS64vDMh+mSiZ67k/jX5fJX2CMaoKq8OuDgl+L/omw78P7bpzCVRFAAQgVWrB9q+piTuW+AStrhUOJQ==;EndpointSuffix=core.windows.net"
BLOB_SERVICE_CLIENT = BlobServiceClient.from_connection_string(CONNECTION_STRING)
CONTAINER_CLIENT = BLOB_SERVICE_CLIENT.get_container_client(CONTAINER_NAME)
CURRENT_DATE = pendulum.now("UTC").format("YYYYMMDD")


def set_dag_vars():
    Variable.set("start_date_var", pendulum.now())

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
        
        if len(matched_tggr_file) > 0:
            Variable.set(key="tggr_file_list",value=matched_tggr_file)
            ti.xcom_push(key='matched_tggr_file', value=matched_tggr_file)
            return matched_tggr_file


def process_file(trigger_file, CONTAINER_CLIENT, CURRENT_DATE, ti=None, **context):
    file_processor = FileProcessor(trigger_file,CONTAINER_CLIENT, CURRENT_DATE)
    file_processor.process_files()

def get_dbt_dag_id(ti=None,**context):
    country_code = ti.xcom_pull(key="country_code", task_ids="get_the_control_files")
    dbt_process_dag_run_id = f'dbt_process_{country_code}'
    Variable.set("dbt_process_dag_id",dbt_process_dag_run_id)


with DAG(
  dag_id="file_sensor_and_processor", # The name that shows up in the UI
  start_date=pendulum.now(), # Start date of the DAG
  catchup=False,
) as dag:
    
    wait_for_blob = CustomWasbSensor(
        task_id="waiting_for_trigger_file",
    )

    with TaskGroup("dynamic_task_group") as process_task_group:
        dynamic_tasks = []

        # Use the list returned by generate_list_task
        #trigger_file_list = Variable.get("trigger_file_list")
        trigger_file_list = Variable.get("tggr_file_list", 
                                        default_var=["sample/mac/tggr_file"])
        print(trigger_file_list)
        
        trigger_file_list = ['pos/mac/INS.DI.MCS_TRGGR.MAC', 'sales/mac/INS.DI.MCS_TRGGR.MAC']
        for tggr_file in trigger_file_list:
            division = tggr_file.split("/")[1]
            source = tggr_file.split("/")[0]
            # division='mac'
            task = PythonOperator(
                task_id=f"dynamic_task_{source}_{division}",
                python_callable=process_file,
                op_args={tggr_file:"tggr_file",CONTAINER_CLIENT:"CONTAINER_CLIENT", CURRENT_DATE: "CURRENT_DATE"}
            )
            #dynamic_tasks.append(task)

            trigger_dbt_dag = TriggerDagRunOperator(
            task_id = f'trigger_dbt_{source}_{division}',
            trigger_dag_id = f'dbt_process_{source}_{division}'
            
            )
            task >> trigger_dbt_dag

    # get_dbt_dag_id_task = PythonOperator(
    #     task_id='get_dbt_dag_id_task',
    #     python_callable=get_dbt_dag_id,
    #     provide_context =True,    
    # )

    

    trigger_file_dag = TriggerDagRunOperator(
            task_id = "wait_for_trigger_file_again",
            trigger_dag_id= "dfs_file_processor"
            
        )
    

    wait_for_blob >> process_task_group >> trigger_file_dag

if __name__ == "__main__":
  dag.cli()

