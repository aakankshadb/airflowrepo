from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator

default_args = {
    "owner":"airflow",
    "retries": 1,
    "retry_delay":timedelta(seconds=5)
}

def printString():
    print("This is my first dag")
def printArray():
    arr=[1,2,3,4,5,6]
    print(arr)
with DAG(dag_id="dag_version_1",
         start_date=datetime(2023,7,26),
         schedule_interval="@daily",
         default_args=default_args,
         catchup=False
         ) as dag:
    task1 = PythonOperator(task_id="task1",
                           python_callable=printString,
                           dag=dag)
    task2 = PythonOperator(task_id="task2",
                           python_callable=printArray,
                           dag=dag)
    task1 >> task2
