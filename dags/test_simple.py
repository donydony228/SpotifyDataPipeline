from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello():
    print("Hello World!")
    return "success"

dag = DAG(
    'test_simple',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

task = PythonOperator(
    task_id='hello',
    python_callable=hello,
    dag=dag
)
