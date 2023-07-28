from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime,timedelta

default_args={
    "owner":"airflow",
    "retries": 2,
    "retry_delay":timedelta(seconds=5)
}
def _downloading():
    print("Downloading")


with DAG(
    dag_id="trigger_dag",
    start_date=datetime(2023,7,28),
    schedule="@daily",
    default_args=default_args,
    catchup=False) as dag:

    task1 = PythonOperator(
        task_id="downloading",
        python_callable=_downloading
    )

    task2 = TriggerDagRunOperator(
        task_id="trigger_target",
        trigger_dag_id="target_dag",
        execution_date='{{ ds }}',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30
             )
    task1 >>task2