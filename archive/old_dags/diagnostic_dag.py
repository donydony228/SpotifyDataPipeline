# dags/diagnostic_dag.py
# 診斷 DAG 實際運行環境

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys
import sqlite3
from pathlib import Path

default_args = {
    'owner': 'diagnostic',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'diagnostic_environment',
    default_args=default_args,
    description='診斷 DAG 運行環境',
    schedule=None,
    catchup=False,
    tags=['diagnostic']
)

def check_environment(**context):
    """檢查 DAG 運行環境"""
    print("=" * 60)
    print("🔍 DAG 運行環境診斷")
    print("=" * 60)
    print()
    
    # 1. 檢查 Python 環境
    print("🐍 Python 資訊:")
    print(f"   可執行文件: {sys.executable}")
    print(f"   版本: {sys.version}")
    print(f"   當前目錄: {os.getcwd()}")
    print()
    
    # 2. 檢查環境變數
    print("📋 環境變數:")
    important_vars = [
        'AIRFLOW_HOME',
        'AIRFLOW__CORE__FERNET_KEY',
        'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN',
        'SUPABASE_DB_URL',
        'MONGODB_ATLAS_URL'
    ]
    
    for var in important_vars:
        value = os.environ.get(var, '未設置')
        if value != '未設置' and len(value) > 60:
            value = f"{value[:60]}..."
        print(f"   {var}: {value}")
    print()
    
    # 3. 檢查 AIRFLOW_HOME
    print("📁 Airflow Home:")
    airflow_home = os.environ.get('AIRFLOW_HOME', '未設置')
    print(f"   環境變數: {airflow_home}")
    
    # 嘗試從 airflow.configuration 讀取
    try:
        from airflow.configuration import conf
        conf_home = conf.get('core', 'dags_folder', fallback='未找到')
        # 從 dags_folder 推斷 AIRFLOW_HOME
        if conf_home != '未找到':
            conf_home = str(Path(conf_home).parent)
        print(f"   配置文件: {conf_home}")
    except:
        print(f"   配置文件: 無法讀取")
    print()
    
    # 4. 檢查資料庫文件
    print("🗄️ 資料庫文件:")
    possible_db_paths = [
        Path(airflow_home) / 'airflow.db' if airflow_home != '未設置' else None,
        Path.cwd() / 'airflow_home' / 'airflow.db',
        Path.cwd() / 'airflow.db',
        Path.home() / 'airflow' / 'airflow.db',
    ]
    
    for db_path in possible_db_paths:
        if db_path and db_path.exists():
            print(f"   ✅ 存在: {db_path}")
            print(f"      大小: {db_path.stat().st_size / 1024:.2f} KB")
            
            # 檢查這個資料庫中的 Variables
            try:
                conn = sqlite3.connect(str(db_path))
                cursor = conn.cursor()
                cursor.execute("SELECT key, is_encrypted FROM variable ORDER BY key")
                vars = cursor.fetchall()
                if vars:
                    print(f"      Variables: {[(k, '🔐' if e else '📝') for k, e in vars]}")
                else:
                    print(f"      Variables: (空)")
                conn.close()
            except Exception as e:
                print(f"      ❌ 無法讀取: {e}")
        elif db_path:
            print(f"   ❌ 不存在: {db_path}")
    print()
    
    # 5. 嘗試讀取 Variables
    print("🔍 嘗試讀取 Airflow Variables:")
    try:
        from airflow.models import Variable
        
        test_vars = ['SUPABASE_DB_URL', 'MONGODB_ATLAS_URL', 'MONGODB_ATLAS_DB_NAME']
        for var_name in test_vars:
            try:
                value = Variable.get(var_name)
                masked = f"{value[:50]}..." if len(value) > 50 else value
                print(f"   ✅ {var_name}: {masked}")
            except KeyError:
                print(f"   ❌ {var_name}: 不存在")
            except Exception as e:
                print(f"   ❌ {var_name}: {str(e)[:80]}")
    except Exception as e:
        print(f"   ❌ 無法使用 Variable API: {e}")
    
    print()
    print("=" * 60)
    
    return "Diagnostic complete"

diagnostic_task = PythonOperator(
    task_id='check_environment',
    python_callable=check_environment,
    dag=dag
)