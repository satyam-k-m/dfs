from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# [START howto_task_group]
with DAG(
    dag_id="task_group",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    start = EmptyOperator(task_id="start")

    # [START howto_task_group_section_1]
    with TaskGroup("section_1", tooltip="Tasks for section_1") as section_1:
        task_1 = EmptyOperator(task_id="task_1")
        task_2 = BashOperator(task_id="task_2", bash_command="echo 1")
        task_3 = EmptyOperator(task_id="task_3")

        task_1 >> [task_2, task_3]
    # [END howto_task_group_section_1]

    # [START howto_task_group_section_2]
    with TaskGroup("section_2", tooltip="Tasks for section_2") as section_2:
        task_1 = EmptyOperator(task_id="task_1")

        # [START howto_task_group_inner_section_2]
        with TaskGroup("inner_section_2", tooltip="Tasks for inner_section2") as inner_section_2:
            task_2 = BashOperator(task_id="task_2", bash_command="echo 1")
            task_3 = EmptyOperator(task_id="task_3")
            task_4 = EmptyOperator(task_id="task_4")

            [task_2, task_3] >> task_4
        # [END howto_task_group_inner_section_2]

    # [END howto_task_group_section_2]

    end = EmptyOperator(task_id="end")

    start >> section_1 >> section_2 >> end