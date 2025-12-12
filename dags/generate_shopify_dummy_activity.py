import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from code.shopify_simulation import simulate_shopify_cycle


with DAG(
    dag_id="shopify_simulation",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["simulation"],
) as dag:

    hello_task = PythonOperator(
        task_id="shopify_simulation_task",
        python_callable=simulate_shopify_cycle,
    )
