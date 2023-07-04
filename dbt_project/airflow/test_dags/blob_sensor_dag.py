import os

from airflow import DAG
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.edgemodifier import Label
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def test_python():
    print("Hello World")


BLOB_NAME = "checksum.txt"
AZURE_CONTAINER_NAME = "input"



with DAG(
    "blob_sensor_dag",
    schedule_interval=None,
    start_date=days_ago(0),  # Override to match your needs
) as dag:

    # [START how_to_wait_for_blob]
    wait_for_blob = WasbBlobSensor(
        task_id="wait_for_blob",
        wasb_conn_id="azure_blob",
        container_name=AZURE_CONTAINER_NAME,
        blob_name=BLOB_NAME,
    )
    
    trigger_python_code = PythonOperator(

        task_id = 'run_python_command',
        python_callable = test_python,
        provide_context=True,
        retries=5

    )
    trigger = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="airflow_dbt",  # Ensure this equals the dag_id of the DAG to trigger
        conf={"message": "Hello World"},
    )
    
    

    
    wait_for_blob >> trigger_python_code >> trigger