from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator

default_args = {
    "owner":"airflow",
    "retries": 1,
    "retry_delay":timedelta(seconds=5)
                }

with DAG(dag_id="Assignment2",
         start_date=datetime(2023,7,27),
         schedule="@daily",
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
        bash_command="echo I am the third task and I will be running parallely with task2 after task1"
    )

    task1 >> [task2,task3]