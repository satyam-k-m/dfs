
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.decorators import dag
import pendulum
from airflow.operators.dummy import DummyOperator

def values_function(**context):
    values = ["first", "second", "third"]
    return values

with DAG(
  dag_id="test_dynamic_flow", # The name that shows up in the UI
  start_date=pendulum.now(), # Start date of the DAG
  catchup=False,
) as dag:
    def group(**kwargs):
            #load the values if needed in the command you plan to execute
        dyn_value = "{{ task_instance.xcom_pull(task_ids='push_func') }}"
        for element in dyn_value:
            return BashOperator(
                    task_id='JOB_NAME_{}'.format(element),
                    bash_command='script.sh {}'.format(element),
                    dag=dag)

    push_func = PythonOperator(
            task_id='push_func',
            provide_context=True,
            python_callable=values_function,
            dag=dag)

    complete = DummyOperator(
            task_id='All_jobs_completed',
            dag=dag)

    push_func >> group()>> complete
