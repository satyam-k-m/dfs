import os
import json
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor

container ='abcd' 
file_name = 'trigger.txt'

with DAG(dag_id="test_blob_connection",
        start_date=pendulum.today()
        ) as dag:
    wait_for_blob = WasbBlobSensor(
            task_id = "look_for_trigger_file",
            wasb_conn_id="azure_blob",
            container_name=container,
            blob_name=file_name
            )


    wait_for_blob



if __name__ == "__main__":
    dag.cli()

