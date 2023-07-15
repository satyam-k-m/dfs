from datetime import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id='my_dag',
    schedule_interval='@once',
    start_date=datetime(2021, 1, 1)
)


def run_this_func(**context):
    print(context["dag_run"].conf)


run_this = PythonOperator(
    task_id='run_this',
    provide_context=True,
    python_callable=run_this_func,
    dag=dag,
)