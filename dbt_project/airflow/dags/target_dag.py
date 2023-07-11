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


def get_var(dag_run=None, **context):
    ti= context["task_instance"]
    dag_var = {dag_run.conf.get("env")}
    return dag_var


def execute_command(**context):
   ti=context["task_instance"]
   env = ti.xcom_pull(task_ids='get_dag_var')
   env = env.pop()
   print(env)
   dag_var = Variable.get(env, deserialize_json=True)
   print(dag_var.get("bash_command"))
   return dag_var.get("bash_command")
   #bash_command = env.get("bash_command")


dag_var = Variable.get("source", deserialize_json=True)


with DAG(dag_id=dag_var["dag_id"], start_date=pendulum.now()) as dag:

    get_dag_var = PythonOperator(
       task_id = "get_dag_var",
       python_callable = get_var,
       provide_context=True
    )

    python_test = PythonOperator(
        task_id = "simple_bash_python_from_dag_var",
        python_callable = execute_command,
        provide_context=True
    )

    bash_op = BashOperator(
       task_id = "test_bash_operator",
       bash_command = '{{ ti.xcom_pull(task_ids="simple_bash_python_from_dag_var")}}'
    )


    get_dag_var >> python_test 


if __name__ == "__main__":
  dag.cli()