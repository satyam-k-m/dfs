
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

class TaskBuilderTest():
    def __init__(self, nodes, sources, dbt_path):
        self.nodes = nodes
        self.sources = sources
        self.dbt_path = dbt_path

    def build_source_task_group(self, source_id, node_info):
        
        item = self.sources[source_id]
        group_id = source_id.replace(".", "_")
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
            
        return source_id
    
    def build_node_task_group(self,node_id, node_info):

        group_id = node_id.replace(".", "_")
        
        print(node_id)
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
            
        return node_id
         
        
    def build_task_group(self, node_id, node_info):
        try:
            if len(node_info['sources']):
                return self.build_source_task_group(node_info)

            return self.build_node_task_group(node_id, node_info)
        except Exception as e:
            print(e)

             
               



    

