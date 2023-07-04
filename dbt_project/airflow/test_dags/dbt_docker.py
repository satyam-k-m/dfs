from airflow import DAG
from airflow.providers.microsoft.azure.operators.container_instances import AzureContainerInstancesOperator
from datetime import datetime, timedelta


with DAG('azure_container_instances',
         start_date=datetime(2020, 12, 1),
         max_active_runs=1,
         schedule='@daily',
         default_args = {
            'retries': 1,
            'retry_delay': timedelta(minutes=1)
        },
         catchup=False
         ) as dag:

    opr_run_container = AzureContainerInstancesOperator(
        task_id='run_container',
        ci_conn_id=None,
        registry_conn_id='azure_conn_container',
        resource_group='rg-learn',
        name='dbtdocker',
        image='dbtdocker.azurecr.io/dbt/dbt_image:latest',
        region='South India',
        command = "sudo docker run dbt run"

    )