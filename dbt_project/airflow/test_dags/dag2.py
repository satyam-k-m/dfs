from __future__ import annotations

import pendulum

from airflow import DAG, XComArg
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun  import TriggerDagRunOperator

value_1 = [1, 2, 3]
value_2 = {"a": "b"}


def python_push_function(ti=None, **context):
    ti.xcom_push(key="python_push", value=value_1)
    return value_1

def python_pull_function(ti=None, **context):

    python_manually_push_value = ti.xcom_pull(key="python_push", task_ids="python_push")
    python_push_value_return = ti.xcom_pull(key="return_value", task_ids="python_push")
    print(f"The xcom value pushed by python push {python_manually_push_value}")
    print(f"The xcom value pushed via return value is {python_push_value_return}")

with DAG(
    "test_dag2",
    schedule="@once",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    python_push = PythonOperator(
        task_id = "python_push",
        python_callable=python_push_function,
        provide_context=True
    )
    python_pull = PythonOperator(
        task_id = "python_pull",
        python_callable=python_pull_function,
        provide_context=True
    )
    dag_trigger = TriggerDagRunOperator(
        task_id = "trigger_same_dag",
        trigger_dag_id= "test_x_com"
        
    )

    python_push >> python_pull >> dag_trigger
