import json
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import snowflake_operations as sql_stmts



class CustomeSnowflakeOperator(SnowflakeOperator):
  template_fields = ('sql', 'parameters')

with DAG(
    dag_id="test_vars",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    test_snowflake_pipeline = CustomeSnowflakeOperator(
        task_id="create_pet_table",
        # provide_context=True,
        parameters={"pipeline_id": dag.dag_id,
                "task_id": "{{ task_instance_key_str }}",
                "run_id": "{{ run_id }}",
                "start_date": "{{ ds }}"
              },
        sql=sql_stmts.test_snowflake_query
  )
  # task_task_id >> 
    test_snowflake_pipeline


if __name__ == "__main__":
  dag.cli()