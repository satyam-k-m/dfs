
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



    def build_source_task_group(self, node_info):
        source_id = node_info["depends_on"]["nodes"][0]
        item = self.sources[source_id]
        group_id = source_id.replace(".", "_")
        
        with TaskGroup(group_id=group_id) as tg:

            insert_dim_task_source =  SnowflakeOperator(
                    task_id = "insert_dim_task_source",
                    sql = sql_stmts.insert_dim_task,
                    params = {"table_name":SNOWFLAKE_DIM_TASK_TABLE, "pipeline_id": "dfs_pipeline", "task_name":group_id}
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
              #  + f" && dbt build --models {node_info['name']} ", # run the model!
                )

            
            insert_dim_task_source >> source_id
            
        return tg
    
    def build_node_task_group(self,node_id, node_info):
        group_id = node_id.replace(".", "_")
        with TaskGroup(group_id=group_id) as tg:

            insert_dim_task_model =  SnowflakeOperator(
                    task_id = "insert_dim_task_model",
                    sql = sql_stmts.insert_dim_task,
                    params = {"table_name":SNOWFLAKE_DIM_TASK_TABLE, "pipeline_id": "dfs_pipeline", "task_name":group_id}
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
               # + f" && dbt build --models {node_info['name']} ", # run the model!
            )
           
            insert_dim_task_model >> node_id 
            
        return tg
         


# class TaskBuilder():
#     def __init__(self, nodes, sources, dbt_path):
#         self.nodes = nodes
#         self.sources = sources
#         self.dbt_path = dbt_path


#     def build_source_task_group(self, node_info):
#         source_id = node_info["depends_on"]["nodes"][0]
#         item = self.sources[source_id]
#         group_id = source_id.replace(".", "_")
#         group_id_name = f"{group_id}_source"

#         with TaskGroup(group_id=group_id_name) as tg:

#             # insert_task_dim = BaseException(
#             #     task_id="dummy_bash_operator_pre",
#             #     bash_command= "echo Hello")


#             insert_dim_task_source =  SnowflakeOperator(
#                     task_id = "insert_dim_task_source",
#                     sql = sql_stmts.insert_dim_task,
#                     params = {"table_name":SNOWFLAKE_DIM_TASK_TABLE, "pipeline_id": tg.dag_id, "task_name":group_id}
#                 )
            
#             source_id = BashOperator(
#                     task_id = ".".join(
#                         [
#                             item["resource_type"],
#                             item["package_name"],
#                             item['source_name'],
#                             item["name"],
#                         ]
#                     ),
#                 bash_command=f"cd {self.dbt_path}" # Go to the path containing your dbt project
#                 + f" && dbt run --models {node_info['name']}", # run the model!
#                 )

#             insert_dim_task_source >> source_id 
            
#         return tg
    
#     def build_node_task_group(self,node_id, node_info):

#         group_id = node_id.replace(".", "_")
#         group_name = f"{group_id}_model"
#         with TaskGroup(group_id=group_name) as tg:
            
#             insert_dim_task_model =  SnowflakeOperator(
#                     task_id = "insert_dim_task_model",
#                     sql = sql_stmts.insert_dim_task,
#                     params = {"table_name":SNOWFLAKE_DIM_TASK_TABLE, "pipeline_id": tg.dag_id, "task_name":group_id}
#             )
            
#             node_id = BashOperator(
#                 task_id= ".".join(
#                     [
#                         node_info["resource_type"],
#                         node_info["package_name"],
#                         node_info["name"],
#                     ]
#                 ),

#                 bash_command=f"cd {self.dbt_path}" # Go to the path containing your dbt project
#                 + f" && dbt run --models {node_info['name']}", # run the model!
#             )

#             insert_dim_task_model >> node_id
            
#         return tg
         


               



    

