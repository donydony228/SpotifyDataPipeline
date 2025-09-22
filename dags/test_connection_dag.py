from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

def test_connections():
    """測試數據庫連接"""
    try:
        import psycopg2
        
        # 從環境變量獲取URL
        supabase_url = os.getenv('SUPABASE_DB_URL')
        if supabase_url:
            conn = psycopg2.connect(supabase_url, connect_timeout=10)
            conn.close()
            print("✅ Supabase連接成功")
        else:
            print("❌ SUPABASE_DB_URL未設置")
            
    except Exception as e:
        print(f"❌ 連接失敗: {e}")
        raise

# 定義DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'test_connection_dag',
    default_args=default_args,
    description='測試數據庫連接',
    schedule_interval=None,  # 手動觸發
    catchup=False,
    tags=['test', 'connection']
)

# 創建任務
test_task = PythonOperator(
    task_id='test_database_connections',
    python_callable=test_connections,
    dag=dag
)
