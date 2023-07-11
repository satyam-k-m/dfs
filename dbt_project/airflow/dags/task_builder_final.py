
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import snowflake_operations as sql_stmts
import os
import pendulum

SNOWFLAKE_DIM_PIPELLINE_TABLE = "dim_pipeline"
SNOWFLAKE_DIM_TASK_TABLE = "dim_task"
SNOWFLAKE_FCT_PIPELINE_TABLE = "fct_pipeline"
SNOWFLAKE_FCT_TASKS_TABLE = "fct_task"

ts = pendulum.now()

class TaskBuilder():
    def __init__(self, nodes, sources, dbt_path, run_ts):
        self.nodes = nodes
        self.sources = sources
        self.dbt_path = dbt_path
        self.run_ts = run_ts
        self.run_ts_dbt_var = {'run_time_stamp':run_ts}
        print(f"getting time_stamp: {self.run_ts}")



    def build_source_task_group(self, node_info):
        print(f"getting time_stamp: {self.run_ts}")
        source_id = node_info["depends_on"]["nodes"][0]
        item = self.sources[source_id]
        group_id = source_id.replace(".", "_")
        
        with TaskGroup(group_id=group_id) as tg:

            update_pre_status = SnowflakeOperator(
                task_id = "update_fct_task_pre_execution",
                sql = sql_stmts.update_task_status_pre,
                params = {"task_table_name": SNOWFLAKE_FCT_TASKS_TABLE, "pipeline_table_name":SNOWFLAKE_FCT_PIPELINE_TABLE, "task_name":group_id,
                        "run_ts":self.run_ts, "status_cd":"running",
                       }
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
                + f" && dbt build --models {node_info['name']}  --vars '{self.run_ts_dbt_var}'", # run the model!
                )
            update_post_status = SnowflakeOperator(
                task_id = "update_fct_task_post_execution",
                sql = sql_stmts.update_task_status_post,
                params = {"task_table_name": SNOWFLAKE_FCT_TASKS_TABLE, "pipeline_table_name":SNOWFLAKE_FCT_PIPELINE_TABLE, "task_name":group_id,
                        "run_ts":self.run_ts, "status_cd":"success",
                }
            )
            
            update_pre_status >> source_id >> update_post_status
            
        return tg
    
    def build_node_task_group(self,node_id, node_info):
        print(f"getting time_stamp: {self.run_ts}")
        group_id = node_id.replace(".", "_")
        with TaskGroup(group_id=group_id) as tg:

            update_pre_status = SnowflakeOperator(
                task_id = "update_fct_task_pre_execution",
                sql = sql_stmts.update_task_status_pre,
                params = {"task_table_name": SNOWFLAKE_FCT_TASKS_TABLE, "pipeline_table_name":SNOWFLAKE_FCT_PIPELINE_TABLE, "task_name":group_id,
                         "run_ts":self.run_ts, "status_cd":"running",
                        }
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
                + f" && dbt build --models {node_info['name']}  --vars '{self.run_ts_dbt_var}'", # run the model!
            )
            update_post_status = SnowflakeOperator(
                task_id = "update_fct_task_post_execution",
                sql = sql_stmts.update_task_status_post,
                params = {"task_table_name": SNOWFLAKE_FCT_TASKS_TABLE, "pipeline_table_name":SNOWFLAKE_FCT_PIPELINE_TABLE,  "task_name":group_id,
                        "run_ts":self.run_ts, "status_cd":"sucess"
                        }
            )
            update_pre_status >> node_id >> update_post_status
            
        return tg
         
