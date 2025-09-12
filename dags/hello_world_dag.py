from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

# 定義 Python 函數
def hello_world():
    print("Hello World from Airflow!")
    print(f"當前時間: {datetime.now()}")
    return "Hello World!"

def print_context(**context):
    print(f"DAG ID: {context['dag'].dag_id}")
    print(f"Task ID: {context['task_instance'].task_id}")
    print(f"執行日期: {context['ds']}")
    return "Context printed!"

# 預設參數
default_args = {
    'owner': 'desmond',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# 定義 DAG (注意：Airflow 3.x 使用 schedule 而不是 schedule_interval)
dag = DAG(
    'hello_world_dag',
    default_args=default_args,
    description='我的第一個 Airflow DAG',
    schedule='@daily',
    catchup=False,
    tags=['tutorial', 'hello_world']
)

# 任務 1: Hello World
hello_task = PythonOperator(
    task_id='hello_world_task',
    python_callable=hello_world,
    dag=dag
)

# 任務 2: 打印上下文信息
context_task = PythonOperator(
    task_id='print_context_task',
    python_callable=print_context,
    dag=dag
)

# 任務 3: Bash 命令
bash_task = BashOperator(
    task_id='bash_task',
    bash_command='echo "Hello from Bash! 目前時間: $(date)"',
    dag=dag
)

# 任務 4: 檢查系統信息
system_info_task = BashOperator(
    task_id='system_info_task',
    bash_command='echo "系統信息:" && uname -a && echo "Python 版本:" && python3 --version',
    dag=dag
)

# 設置任務依賴關係
hello_task >> context_task >> bash_task >> system_info_task
