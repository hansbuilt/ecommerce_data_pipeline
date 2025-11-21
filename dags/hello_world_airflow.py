from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def say_hello():
    print("Hello from Airflow!")


with DAG(
    dag_id="hello_world_airflow_hb",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test"],
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello_task",
        python_callable=say_hello,
    )
