from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,
    SQLTableCheckOperator,
)
from airflow.decorators import dag
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pendulum import datetime
from airflow.utils.task_group import TaskGroup
import snowflake_operations as sql_stmts

SNOWFLAKE_DIM_PIPELLINE_TABLE = "dim_pipeine"
SNOWFLAKE_DIM_TASKS_TABLE = "dim_task"
SNOWFLAKE_FCT_PIPELINE_TABLE = "fct_pipeline"
SNOWFLAKE_FCT_TASKS_TABLE = "fct_task"

SNOWFLAKE_CONN_ID = "snowflake_default"

ROW_COUNT_CHECK = "COUNT(*) = 9"

with DAG(
    "complex_snowflake_example",
    description="""
        Example DAG showcasing loading, transforming, 
        and data quality checking with multiple datasets in Snowflake.
    """,
    doc_md=__doc__,
    start_date=datetime(2022, 12, 1),
    schedule=None,
    # defining the directory where SQL templates are stored
   # template_searchpath="/",
    catchup=False,
) as dag_:
    """
    #### Snowflake table creation
    Create the tables to store sample data.
    """
    def get_tasks(**context):
        dagrun: DAG = context["dag_run"]
        tasks = []
        for ti in dagrun.get_task_instances():
            tasks.append({"task_id":ti.task_id, "dag_id":ti.dag_id, "run_id":ti.run_id})
        return tasks

    
    tasks = PythonOperator(
        task_id="tasks",
        python_callable=get_tasks,

    )
    

    def pull_function(**kwargs):
        # ti = kwargs['ti']
        ti = dag_.get_task_instances()
        ls = ti.xcom_pull()
        print("X_COM pull:", ls)
        # for i in ls:
        insert_fct_task_table = SnowflakeOperator(
        task_id = f"insert_fct_task",
        sql = sql_stmts.insert_fct_task, 
        xcom_push=True,
        params={"table_name":SNOWFLAKE_FCT_TASKS_TABLE, "task_id":ls[0]['task_id'],
                "dag_id": ls[0]['dag_id'], "run_id":ls[0]['run_id']},  
            )
        insert_fct_task_table.execute(dict())
        

    task_pull = PythonOperator(
        task_id="tasks_pull",
        python_callable=pull_function,
        

    )

    create_dim_task_table = SnowflakeOperator(
        task_id="create_dim_task_table",
        sql=sql_stmts.create_dim_pipeline,
        params={"table_name": SNOWFLAKE_DIM_TASKS_TABLE},
    )

    create_dim_pipeline_table = SnowflakeOperator(
        task_id="create_dim_pipeline_table",
        sql=sql_stmts.create_dim_pipeline,
        params={"table_name": SNOWFLAKE_DIM_PIPELLINE_TABLE},
    )

    create_fct_pipeline_table = SnowflakeOperator(
        task_id="create_fct_pipeline",
        sql=sql_stmts.create_fct_pipeline,
        params={"table_name": SNOWFLAKE_FCT_PIPELINE_TABLE},
    )

    create_fct_task_table = SnowflakeOperator(
        task_id="create_fct_task",
        sql=sql_stmts.create_fct_task,
        params={"table_name": SNOWFLAKE_FCT_TASKS_TABLE},
    )
    """
    #### Insert data
    Insert data into the Snowflake tables using existing SQL queries
    stored in the include/sql/snowflake_examples/ directory.
    """
    # load_forestfire_data = SnowflakeOperator(
    #     task_id="load_forestfire_data",
    #     sql=sql_stmts.load_forestfire_data,
    #     params={"table_name": SNOWFLAKE_FORESTFIRE_TABLE},
    # )

    # load_cost_data = SnowflakeOperator(
    #     task_id="load_cost_data",
    #     sql=sql_stmts.load_cost_data,
    #     params={"table_name": SNOWFLAKE_COST_TABLE},
    # )

    # load_forestfire_cost_data = SnowflakeOperator(
    #     task_id="load_forestfire_cost_data",
    #     sql=sql_stmts.load_forestfire_cost_data,
    #     params={"table_name": SNOWFLAKE_FORESTFIRE_COST_TABLE},
    # )

    """
    #### Transform
    Transform the forestfire_costs table to perform
    sample logic.
    """
    # transform_forestfire_cost_table = SnowflakeOperator(
    #     task_id="transform_forestfire_cost_table",
    #     sql=sql_stmts.transform_forestfire_cost_table,
    #     params={"table_name": SNOWFLAKE_FORESTFIRE_COST_TABLE},
    # )

    """
    #### Quality checks
    Perform data quality checks on the various tables.
    """
    # with TaskGroup(
    #     group_id="quality_check_group_forestfire",
    #     default_args={
    #         "conn_id": SNOWFLAKE_CONN_ID,
    #     },
    # ) as quality_check_group_forestfire:
    #     """
    #     #### Column-level data quality check
    #     Run data quality checks on columns of the forestfire table
    #     """
    #     forestfire_column_checks = SQLColumnCheckOperator(
    #         task_id="forestfire_column_checks",
    #         table=SNOWFLAKE_FORESTFIRE_TABLE,
    #         column_mapping={
    #             "ID": {"null_check": {"equal_to": 0}},
    #             "RH": {"max": {"leq_to": 100}},
    #         },
    #     )

    #     """
    #     #### Table-level data quality check
    #     Run data quality checks on the forestfire table
    #     """
    #     forestfire_table_checks = SQLTableCheckOperator(
    #         task_id="forestfire_table_checks",
    #         table=SNOWFLAKE_FORESTFIRE_TABLE,
    #         checks={"row_count_check": {"check_statement": ROW_COUNT_CHECK}},
    #     )

    # with TaskGroup(
    #     group_id="quality_check_group_cost",
    #     default_args={
    #         "conn_id": SNOWFLAKE_CONN_ID,
    #     },
    # ) as quality_check_group_cost:
    #     """
    #     #### Column-level data quality check
    #     Run data quality checks on columns of the forestfire table
    #     """
    #     cost_column_checks = SQLColumnCheckOperator(
    #         task_id="cost_column_checks",
    #         table=SNOWFLAKE_COST_TABLE,
    #         column_mapping={
    #             "ID": {"null_check": {"equal_to": 0}},
    #             "LAND_DAMAGE_COST": {"min": {"geq_to": 0}},
    #             "PROPERTY_DAMAGE_COST": {"min": {"geq_to": 0}},
    #             "LOST_PROFITS_COST": {"min": {"geq_to": 0}},
    #         },
    #     )

    #     """
    #     #### Table-level data quality check
    #     Run data quality checks on the forestfire table
    #     """
    #     cost_table_checks = SQLTableCheckOperator(
    #         task_id="cost_table_checks",
    #         table=SNOWFLAKE_COST_TABLE,
    #         checks={"row_count_check": {"check_statement": ROW_COUNT_CHECK}},
    #     )

    # with TaskGroup(
    #     group_id="quality_check_group_forestfire_costs",
    #     default_args={
    #         "conn_id": SNOWFLAKE_CONN_ID,
    #     },
    # ) as quality_check_group_forestfire_costs:
    #     """
    #     #### Column-level data quality check
    #     Run data quality checks on columns of the forestfire table
    #     """
    #     forestfire_costs_column_checks = SQLColumnCheckOperator(
    #         task_id="forestfire_costs_column_checks",
    #         table=SNOWFLAKE_FORESTFIRE_COST_TABLE,
    #         column_mapping={"AREA": {"min": {"geq_to": 0}}},
    #     )

    #     """
    #     #### Table-level data quality check
    #     Run data quality checks on the forestfire table
    #     """
    #     forestfire_costs_table_checks = SQLTableCheckOperator(
    #         task_id="forestfire_costs_table_checks",
    #         table=SNOWFLAKE_FORESTFIRE_COST_TABLE,
    #         checks={
    #             "row_count_check": {"check_statement": ROW_COUNT_CHECK},
    #             "total_cost_check": {
    #                 "check_statement": "land_damage_cost + \
    #                 property_damage_cost + lost_profits_cost = total_cost"
    #             },
    #         },
    #     )

    # """
    # #### Delete tables
    # Clean up the tables created for the example.
    # """
    # delete_forestfire_table = SnowflakeOperator(
    #     task_id="delete_forestfire_table",
    #     sql="delete_table.sql",
    #     params={"table_name": SNOWFLAKE_FORESTFIRE_TABLE},
    # )

    # delete_cost_table = SnowflakeOperator(
    #     task_id="delete_costs_table",
    #     sql="delete_table.sql",
    #     params={"table_name": SNOWFLAKE_COST_TABLE},
    # )

    # delete_forestfire_cost_table = SnowflakeOperator(
    #     task_id="delete_forestfire_cost_table",
    #     sql="delete_table.sql",
    #     params={"table_name": SNOWFLAKE_FORESTFIRE_COST_TABLE},
    # )

    begin = EmptyOperator(task_id="begin")
    create_done = EmptyOperator(task_id="create_done")
    # load_done = EmptyOperator(task_id="load_done")
    end = EmptyOperator(task_id="end")
    chain(
        begin,
        tasks,
        task_pull,
        [create_dim_pipeline_table, create_dim_task_table, create_fct_pipeline_table, create_fct_task_table],
        create_done,
        end,
    )