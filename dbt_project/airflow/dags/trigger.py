from datetime import datetime
from airflow.models import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator

dag = DAG(
    dag_id='trigger',
    schedule_interval='@once',
    start_date=datetime(2021, 1, 1)
)


def modify_dro(context, dagrun_order):
    print(context)
    print(dagrun_order)
    dagrun_order.payload = {
        "message": "This is my conf message"
    }
    return dagrun_order


run_this = TriggerDagRunOperator(
    task_id='run_this',
    trigger_dag_id='my_dag',
    python_callable=modify_dro,
    dag=dag
)