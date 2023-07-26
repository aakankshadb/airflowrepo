from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator

default_args = {
    "owner":"airflow",
    "retries": 1,
    "retry_delay":timedelta(seconds=5)
                }

with DAG(dag_id="dag_version_2_v5",
         start_date=datetime(2023,7,26),
         schedule_interval="@daily",
         default_args=default_args,
         catchup=False
        ) as dag:

    task1 = BashOperator(
                        task_id="task1",
                        bash_command="echo Hello world! This is the first task"
                    )
    task2 = BashOperator(
                        task_id="task2",
                        bash_command="echo I am the second task and I will be running after task1"
                    )
    task3 = BashOperator(
        task_id="task3",
        bash_command="echo I am the third task and I will be running with task2 after task1"
    )
    # Task dependency Method1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # Task dependency Method2
    # task1 >> task2
    # task1 >> task3

    # Task dependency Method3
    # task1 >> [task2,task3]

    task1 << [task2,task3]

