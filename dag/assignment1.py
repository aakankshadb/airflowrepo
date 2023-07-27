from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator

default_args = {
    "owner":"airflow",
    "retries": 1,
    "retry_delay":timedelta(seconds=5)
}
def maxNumber():
    first_num=5
    second_num=10
    if first_num>=second_num:
        print("The max number is {f_num}".format(f_num=first_num))
    else:
        print("The max number is {s_num}".format(s_num=second_num))

def addNumber():
    first_num = 6
    second_num = 30
    total = first_num+second_num
    print("Summation:",total)

with DAG(dag_id="Assignment1",
         start_date=datetime(2023,7,27),
         schedule_interval="@daily",
         default_args=default_args,
         catchup=False
         ) as dag:
    task1 = PythonOperator(task_id="task1",
                           python_callable=maxNumber,
                           dag=dag)

    task2 = PythonOperator(task_id="task2",
                           python_callable=addNumber,
                           dag=dag)
    task1 >> task2
