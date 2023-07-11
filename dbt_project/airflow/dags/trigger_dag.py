import os
import json
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
from task_builder_final import *
from airflow.decorators import dag
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pendulum import datetime
from airflow.utils.task_group import TaskGroup
import snowflake_operations as sql_stmts
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator



with DAG("trigger_dag", start_date=pendulum.now()) as dag:

    trigger_step = TriggerDagRunOperator(
        task_id="trigger_dag",
        trigger_dag_id="target_dag",
        conf={"env": "dev", "div_code":"mac"},
        dag=dag
    )

    trigger_step

if __name__ == "__main__":
  dag.cli()