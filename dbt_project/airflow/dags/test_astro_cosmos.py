from airflow.decorators import dag
from cosmos.providers.dbt.task_group import DbtTaskGroup
from cosmos.providers.dbt.dag import DbtDag
from pendulum import datetime

CONNECTION_ID = "dbt_snow_conn"
DB_NAME = "DFS_COSMOS"
SCHEMA_NAME = "DFS_COSMOS"
DBT_PROJECT_NAME = "dfs"
# the path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = "/home/satyam/.local/bin/dbt"
# The path to your dbt root directory
#DBT_ROOT_PATH = "/home/rrathi/.local/bin/dbt"

PROJECT_NAME = "/home/satyam/dfs/dbt_project/dbt/dbt/"

example_local = DbtDag (
    dbt_executable_path=DBT_EXECUTABLE_PATH,
    dbt_project_name=DBT_PROJECT_NAME,
    conn_id=CONNECTION_ID,
    dbt_args={"schema": "DBT_COSMOS"},
    execution_mode="local",
    operator_args={
        "project_dir": PROJECT_NAME,
       
    },
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="example_local"
)
