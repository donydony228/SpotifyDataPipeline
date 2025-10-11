from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import psycopg2
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import random
import json

def generate_mock_jobs():
    """生成模擬職缺資料"""
    companies = ['TechCorp', 'DataCo', 'AI Labs', 'Cloud Systems']
    titles = ['Senior Data Engineer', 'ML Engineer', 'Data Analyst']
    locations = ['San Francisco, CA', 'New York, NY', 'Remote', 'Seattle, WA']
    
    jobs = []
    batch_id = f"dual_db_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    for i in range(5):
        job = {
            'job_id': f'job_{batch_id}_{i}',
            'title': titles[i % len(titles)],
            'company': companies[i % len(companies)],
            'location': locations[i % len(locations)],
            'description': f'Exciting opportunity for {titles[i % len(titles)]}',
            'salary_min': random.randint(120000, 150000),
            'salary_max': random.randint(180000, 250000),
            'source': 'linkedin',
            'posted_date': datetime.now().isoformat(),
            'batch_id': batch_id
        }
        jobs.append(job)
    
    return jobs, batch_id

def store_to_postgres(**context):
    """存儲到 PostgreSQL"""
    jobs, batch_id = generate_mock_jobs()
    
    SUPABASE_URL = os.getenv('SUPABASE_DB_URL')
    
    conn = psycopg2.connect(SUPABASE_URL)
    cursor = conn.cursor()
    
    success_count = 0
    for job in jobs:
        try:
            cursor.execute("""
                INSERT INTO raw_staging.linkedin_jobs_raw 
                (job_data, source_url, batch_id, scraped_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            """, (json.dumps(job), 'dual_db_test', batch_id))
            success_count += 1
        except Exception as e:
            print(f"⚠️ PostgreSQL 插入失敗: {e}")
    
    conn.commit()
    print(f"✅ PostgreSQL: 成功存儲 {success_count}/{len(jobs)} 筆職缺")
    
    cursor.close()
    conn.close()
    
    # 傳遞給下一個 task
    context['ti'].xcom_push(key='jobs', value=jobs)
    context['ti'].xcom_push(key='batch_id', value=batch_id)

def store_to_mongodb(**context):
    """存儲到 MongoDB (使用正確的 Date 類型)"""
    # 從上一個 task 取得資料
    jobs = context['ti'].xcom_pull(key='jobs', task_ids='store_to_postgres')
    batch_id = context['ti'].xcom_pull(key='batch_id', task_ids='store_to_postgres')
    
    MONGODB_URL = os.getenv('MONGODB_ATLAS_URL')
    DB_NAME = os.getenv('MONGODB_ATLAS_DB_NAME')
    
    client = MongoClient(
        MONGODB_URL,
        server_api=ServerApi('1'),
        serverSelectionTimeoutMS=10000
    )
    
    db = client[DB_NAME]
    collection = db['raw_jobs_data']
    
    # 轉換為符合 MongoDB Schema 的格式
    mongo_docs = []
    now = datetime.utcnow()  # 使用 datetime 物件,不是字串!
    
    for job in jobs:
        mongo_doc = {
            'source': job['source'],  # 必須是 linkedin/indeed/glassdoor/angellist
            'job_data': job,  # 原始 job 資料
            'metadata': {
                'batch_id': batch_id,
                'scraped_at': now,  # Date 物件,不是 ISO 字串
                'scraper_version': '1.0',
                'is_test': True
            }
        }
        mongo_docs.append(mongo_doc)
    
    # 插入到 MongoDB
    try:
        result = collection.insert_many(mongo_docs)
        print(f"✅ MongoDB: 成功存儲 {len(result.inserted_ids)} 筆職缺")
        print(f"📦 Batch ID: {batch_id}")
        print(f"📝 插入的前 3 個 IDs: {result.inserted_ids[:3]}")
    except Exception as e:
        print(f"❌ MongoDB 插入失敗: {e}")
        raise
    
    client.close()

with DAG(
    'dual_database_scraper',
    description='同時寫入 PostgreSQL 和 MongoDB 的測試 DAG',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'dual-db']
) as dag:
    
    postgres_task = PythonOperator(
        task_id='store_to_postgres',
        python_callable=store_to_postgres
    )
    
    mongo_task = PythonOperator(
        task_id='store_to_mongodb',
        python_callable=store_to_mongodb
    )
    
    postgres_task >> mongo_task
