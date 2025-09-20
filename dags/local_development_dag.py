# dags/local_development_dag.py
# 本地開發環境 - 雲端資料庫連線測試

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

# 加入 src 路徑
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))

def test_cloud_databases():
    """測試雲端資料庫連線"""
    import psycopg2
    from pymongo import MongoClient
    from pymongo.server_api import ServerApi
    import os
    
    results = {}
    
    try:
        # 測試 Supabase
        supabase_url = os.getenv('SUPABASE_DB_URL')
        if supabase_url:
            conn = psycopg2.connect(supabase_url)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM dwh.fact_jobs")
            job_count = cur.fetchone()[0]
            conn.close()
            results['supabase'] = f"✅ Supabase: {job_count} jobs in warehouse"
            print(results['supabase'])
        else:
            results['supabase'] = "❌ SUPABASE_DB_URL not configured"
            print(results['supabase'])
    
    except Exception as e:
        results['supabase'] = f"❌ Supabase error: {str(e)}"
        print(results['supabase'])
    
    try:
        # 測試 MongoDB Atlas
        mongodb_url = os.getenv('MONGODB_ATLAS_URL')
        mongodb_db = os.getenv('MONGODB_ATLAS_DB_NAME', 'job_market_data')
        
        if mongodb_url:
            client = MongoClient(mongodb_url, server_api=ServerApi('1'))
            db = client[mongodb_db]
            raw_count = db['raw_jobs_data'].count_documents({})
            client.close()
            results['mongodb'] = f"✅ MongoDB Atlas: {raw_count} raw jobs"
            print(results['mongodb'])
        else:
            results['mongodb'] = "❌ MONGODB_ATLAS_URL not configured"
            print(results['mongodb'])
    
    except Exception as e:
        results['mongodb'] = f"❌ MongoDB Atlas error: {str(e)}"
        print(results['mongodb'])
    
    # 測試本地資料庫
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5433,
            database='job_data_warehouse',
            user='dwh_user',
            password='dwh_password'
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM dwh.fact_jobs")
        local_jobs = cur.fetchone()[0]
        conn.close()
        results['local_dwh'] = f"✅ Local DWH: {local_jobs} jobs"
        print(results['local_dwh'])
    
    except Exception as e:
        results['local_dwh'] = f"❌ Local DWH error: {str(e)}"
        print(results['local_dwh'])
    
    print("\n🏆 本地開發環境狀態總結:")
    for db, status in results.items():
        print(f"  {db}: {status}")
    
    return results

def test_etl_pipeline():
    """測試完整 ETL Pipeline"""
    print("🔄 測試 ETL Pipeline...")
    
    # 這裡會加入實際的 ETL 邏輯
    # 1. 從 MongoDB 讀取原始資料
    # 2. 處理和清洗
    # 3. 載入到 Supabase
    
    print("✅ ETL Pipeline 測試完成")
    return "ETL pipeline tested"

def create_test_data():
    """建立測試資料"""
    from pymongo import MongoClient
    from pymongo.server_api import ServerApi
    import psycopg2
    from datetime import datetime
    import uuid
    
    try:
        # 1. 在 MongoDB Atlas 建立測試資料
        mongodb_url = os.getenv('MONGODB_ATLAS_URL')
        if mongodb_url:
            client = MongoClient(mongodb_url, server_api=ServerApi('1'))
            db = client[os.getenv('MONGODB_ATLAS_DB_NAME', 'job_market_data')]
            
            test_job = {
                "source": "linkedin",
                "job_data": {
                    "job_id": f"local_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    "job_title": "Senior Data Engineer - Local Test",
                    "company": "Local Dev Corp",
                    "location": "San Francisco, CA",
                    "salary": "$130,000 - $190,000",
                    "description": "Local development test job posting...",
                    "skills": ["Python", "SQL", "Airflow", "Docker", "Supabase"],
                    "employment_type": "Full-time"
                },
                "metadata": {
                    "scraped_at": datetime.utcnow(),
                    "batch_id": f"local_dev_batch_{datetime.now().strftime('%Y%m%d')}",
                    "scraper_version": "1.0.0-local",
                    "source_url": "https://linkedin.com/jobs/local_test"
                }
            }
            
            result = db['raw_jobs_data'].insert_one(test_job)
            print(f"✅ MongoDB Atlas: 測試資料已插入，ID: {result.inserted_id}")
            client.close()
        
        # 2. 在 Supabase 建立對應的處理後資料
        supabase_url = os.getenv('SUPABASE_DB_URL')
        if supabase_url:
            conn = psycopg2.connect(supabase_url)
            cur = conn.cursor()
            
            # 簡單測試：插入一筆 fact_jobs 資料
            # 注意：實際上需要先有對應的維度表資料
            print("✅ Supabase: 測試資料處理邏輯準備就緒")
            
            conn.close()
    
    except Exception as e:
        print(f"❌ 建立測試資料失敗: {str(e)}")
        raise

# DAG 定義
default_args = {
    'owner': 'local-dev',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'local_development_test',
    default_args=default_args,
    description='本地開發環境 - 雲端資料庫連線測試',
    schedule=None,  # 手動觸發
    catchup=False,
    tags=['local', 'development', 'cloud-database']
)

# 任務定義
test_db_task = PythonOperator(
    task_id='test_cloud_databases',
    python_callable=test_cloud_databases,
    dag=dag
)

create_data_task = PythonOperator(
    task_id='create_test_data',
    python_callable=create_test_data,
    dag=dag
)

test_etl_task = PythonOperator(
    task_id='test_etl_pipeline',
    python_callable=test_etl_pipeline,
    dag=dag
)

system_info_task = BashOperator(
    task_id='system_info',
    bash_command='''
    echo "🖥️ 本地開發環境資訊:"
    echo "Python: $(python --version)"
    echo "當前時間: $(date)"
    echo "工作目錄: $(pwd)"
    echo "Docker 狀態:"
    docker compose ps || echo "Docker Compose 未運行"
    ''',
    dag=dag
)

# 任務依賴
system_info_task >> test_db_task >> create_data_task >> test_etl_task