from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def health_check():
    import psycopg2, os
    from pymongo import MongoClient
    from pymongo.server_api import ServerApi
    
    try:
        # Supabase 測試
        conn = psycopg2.connect(os.getenv('SUPABASE_DB_URL'))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM dwh.fact_jobs")
        jobs = cur.fetchone()[0]
        conn.close()
        print(f"✅ Supabase: {jobs} jobs")
        
        # MongoDB 測試
        client = MongoClient(os.getenv('MONGODB_ATLAS_URL'), server_api=ServerApi('1'))
        db = client[os.getenv('MONGODB_ATLAS_DB_NAME', 'job_market_data')]
        raw_jobs = db['raw_jobs_data'].count_documents({})
        client.close()
        print(f"✅ MongoDB: {raw_jobs} raw jobs")
        
        print(f"🚀 Railway health check: {datetime.now()}")
        return "healthy"
        
    except Exception as e:
        print(f"❌ Health check failed: {e}")
        raise

default_args = {
    'owner': 'railway',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'railway_health_check',
    default_args=default_args,
    description='Railway Health Check',
    schedule='*/10 * * * *',
    catchup=False,
    tags=['railway', 'health-check']
)

health_task = PythonOperator(
    task_id='health_check',
    python_callable=health_check,
    dag=dag
)

bash_task = BashOperator(
    task_id='system_check',
    bash_command='echo "✅ Railway system check: $(date)"',
    dag=dag
)

health_task >> bash_task
