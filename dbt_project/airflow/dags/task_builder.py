
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import snowflake_operations as sql_stmts
from airflow.operators.python_operator import PythonOperator

SNOWFLAKE_DIM_PIPELLINE_TABLE = "dim_pipeline"
SNOWFLAKE_DIM_TASK_TABLE = "dim_task"
SNOWFLAKE_FCT_PIPELINE_TABLE = "fct_pipeline"
SNOWFLAKE_FCT_TASKS_TABLE = "fct_task"


class TaskBuilder():
    def __init__(self, nodes, sources, dbt_path):
        self.nodes = nodes
        self.sources = sources
        self.dbt_path = dbt_path

    def get_task_vars(self, ti=None, **kwargs):
        dag_instance = kwargs['dag']
        print( ti.run_id )
        print(ti.dag_id)
        print(ti.task_id)
        print(ti.start_date)
        print(ti.end_date)
        run_id = ti.run_id
        task_id = ti.task_id
        dag_id = ti.dag_id
        start_date = ti.start_date
        end_date = ti.end_date
        # print(vars.task_id, vars.run_id)
        insert_dim_task = SnowflakeOperator(
                    task_id = "insert_dim_task",
                    sql = sql_stmts.update_task_status,
                    params = {"table_name":SNOWFLAKE_FCT_TASKS_TABLE, "pipeline_id": dag_id, "task_id": task_id, "run_id":run_id,
                              "status_cd":"running", "start_ts":start_date, "end_ts":end_date, "duration":"null"}
                )

        insert_dim_task

    def build_source_task_group(self, node_info):
        source_id = node_info["depends_on"]["nodes"][0]
        item = self.sources[source_id]
        group_id = source_id.replace(".", "_")
        with TaskGroup(group_id=group_id) as tg:

            # insert_task_dim = BaseException(
            #     task_id="dummy_bash_operator_pre",
            #     bash_command= "echo Hello")

            

            insert_dim_task = PythonOperator(
                task_id = "get_context",
                python_callable = self.get_task_vars
            )
            
            source_id = BashOperator(
                    task_id = ".".join(
                        [
                            item["resource_type"],
                            item["package_name"],
                            item['source_name'],
                            item["name"],
                        ]
                    ),
                bash_command=f"cd {self.dbt_path}" # Go to the path containing your dbt project
                + f" && dbt run --models {node_info['name']}", # run the model!
                )

            insert_dim_task >> source_id 
            
        return tg
    
    def build_node_task_group(self,node_id, node_info):

        group_id = node_id.replace(".", "_")
        with TaskGroup(group_id=group_id) as tg:
            
            insert_dim_task = PythonOperator(
                task_id = "get_context",
                python_callable = self.get_task_vars
            )
            
            node_id = BashOperator(
                task_id= ".".join(
                    [
                        node_info["resource_type"],
                        node_info["package_name"],
                        node_info["name"],
                    ]
                ),

                bash_command=f"cd {self.dbt_path}" # Go to the path containing your dbt project
                + f" && dbt run --models {node_info['name']}", # run the model!
            )

            insert_dim_task >> node_id
            
        return tg
         


               



    

