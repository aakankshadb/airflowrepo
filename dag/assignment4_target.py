from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta

default_args={
    "owner":"airflow",
    "retries": 2,
    "retry_delay":timedelta(seconds=5)
}
def _cleaning():
    print("cleaning from target DAG")


with DAG(
    dag_id="target_dag",
    start_date=datetime(2023,7,28),
    schedule="@daily",
    default_args=default_args,
    catchup=False) as dag:

    task1 = PythonOperator(
        task_id="cleaning",
        python_callable=_cleaning
    )
    task2 = BashOperator(
        task_id="Sleeping",
        bash_command="sleep 30"
    )
    task1 >> task2