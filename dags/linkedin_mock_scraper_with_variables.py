# dags/linkedin_mock_scraper_with_variables.py
# 使用 Airflow Variables 解決環境變數問題

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import random
import json
import uuid

# ============================================================================
# DAG 配置
# ============================================================================

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'linkedin_mock_scraper_with_variables',
    default_args=default_args,
    description='🎯 LinkedIn 模擬爬蟲 - 使用 Airflow Variables',
    schedule=None,
    catchup=False,
    tags=['scraper', 'linkedin', 'mock', 'variables']
)

# ============================================================================
# Mock 爬蟲
# ============================================================================

class MockLinkedInScraper:
    """模擬 LinkedIn 爬蟲"""
    
    def __init__(self, target_jobs=10):
        self.target_jobs = target_jobs
        self.job_titles = [
            'Senior Data Engineer', 'Data Engineer', 'ML Engineer',
            'Data Scientist', 'Analytics Engineer', 'Backend Engineer'
        ]
        self.companies = [
            'Tech Corp', 'Data Inc', 'AI Solutions', 'Cloud Systems',
            'Innovation Labs', 'Digital Ventures'
        ]
        self.locations = [
            'San Francisco, CA', 'New York, NY', 'Seattle, WA',
            'Austin, TX', 'Boston, MA', 'Remote'
        ]
    
    def scrape(self):
        """生成模擬職缺數據"""
        jobs = []
        for i in range(self.target_jobs):
            job = {
                'job_id': f'mock_linkedin_{uuid.uuid4().hex[:8]}',
                'title': random.choice(self.job_titles),
                'company': random.choice(self.companies),
                'location': random.choice(self.locations),
                'salary_min': random.randint(80000, 150000),
                'salary_max': random.randint(150000, 250000),
                'posted_date': datetime.now().isoformat(),
                'description': f'Exciting opportunity for {random.choice(self.job_titles)}',
                'source': 'linkedin',
                'is_mock': True
            }
            jobs.append(job)
        return jobs

# ============================================================================
# Task 函數
# ============================================================================

def check_variables(**context):
    """檢查 Airflow Variables 是否設置"""
    print("🔍 檢查 Airflow Variables...")
    
    required_vars = {
        'SUPABASE_DB_URL': 'Supabase PostgreSQL 連接字串',
        'MONGODB_ATLAS_URL': 'MongoDB Atlas 連接字串',
        'MONGODB_ATLAS_DB_NAME': 'MongoDB 資料庫名稱'
    }
    
    results = {}
    all_found = True
    
    for var_name, description in required_vars.items():
        try:
            value = Variable.get(var_name)
            if value:
                # 只顯示前 40 個字符
                masked = f"{value[:40]}..." if len(value) > 40 else value
                print(f"  ✅ {var_name}: {masked}")
                results[var_name] = 'found'
            else:
                print(f"  ❌ {var_name}: 設置但為空")
                results[var_name] = 'empty'
                all_found = False
        except Exception as e:
            print(f"  ❌ {var_name}: 未設置 ({description})")
            results[var_name] = 'missing'
            all_found = False
    
    if not all_found:
        print("\n⚠️  警告：部分變數缺失，將使用模擬模式")
        print("💡 提示：運行 ./fix_supabase_connection.sh 來設置 Airflow Variables")
    
    context['task_instance'].xcom_push(key='variables_check', value=results)
    return results

def scrape_jobs(**context):
    """爬取（生成）職缺數據"""
    print("🎯 開始模擬爬取...")
    
    scraper = MockLinkedInScraper(target_jobs=10)
    jobs_data = scraper.scrape()
    
    batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    result = {
        'batch_id': batch_id,
        'jobs': jobs_data,
        'total_jobs': len(jobs_data),
        'timestamp': datetime.now().isoformat()
    }
    
    print(f"✅ 成功生成 {len(jobs_data)} 個模擬職缺")
    
    context['task_instance'].xcom_push(key='scrape_result', value=result)
    return f"Scraped {len(jobs_data)} jobs"

def store_to_mongodb(**context):
    """存儲到 MongoDB"""
    scrape_result = context['task_instance'].xcom_pull(
        task_ids='scrape_jobs',
        key='scrape_result'
    )
    
    jobs_data = scrape_result['jobs']
    batch_id = scrape_result['batch_id']
    
    print(f"🍃 開始存儲 {len(jobs_data)} 個職缺到 MongoDB...")
    
    # 嘗試從 Airflow Variables 獲取連接資訊
    try:
        mongodb_url = Variable.get('MONGODB_ATLAS_URL')
        db_name = Variable.get('MONGODB_ATLAS_DB_NAME')
        
        print(f"✅ 從 Airflow Variables 獲取 MongoDB 連接資訊")
        print(f"   URL: {mongodb_url[:40]}...")
        print(f"   DB: {db_name}")
        
        # 實際連接 MongoDB
        from pymongo import MongoClient
        from pymongo.server_api import ServerApi
        
        client = MongoClient(mongodb_url, server_api=ServerApi('1'))
        db = client[db_name]
        collection = db['raw_jobs_data']
        
        # 為每個職缺添加批次資訊
        for job in jobs_data:
            job['batch_id'] = batch_id
            job['stored_at'] = datetime.now().isoformat()
        
        # 插入數據
        result = collection.insert_many(jobs_data)
        inserted_count = len(result.inserted_ids)
        
        client.close()
        
        print(f"✅ 成功存儲 {inserted_count} 個職缺到 MongoDB Atlas")
        
        stats = {
            'inserted': inserted_count,
            'batch_id': batch_id,
            'collection': 'raw_jobs_data'
        }
        
        context['task_instance'].xcom_push(key='mongodb_stats', value=stats)
        return f"Stored {inserted_count} jobs to MongoDB"
        
    except Exception as e:
        print(f"⚠️  MongoDB 連接失敗: {str(e)}")
        print("💡 提示：確保已運行 ./fix_supabase_connection.sh")
        print("📝 模擬模式：記錄但不實際存儲")
        
        stats = {
            'inserted': 0,
            'batch_id': batch_id,
            'error': str(e),
            'simulated': True
        }
        
        context['task_instance'].xcom_push(key='mongodb_stats', value=stats)
        return f"MongoDB storage failed: {str(e)}"

def store_to_postgres(**context):
    """存儲到 PostgreSQL (Supabase)"""
    scrape_result = context['task_instance'].xcom_pull(
        task_ids='scrape_jobs',
        key='scrape_result'
    )
    
    jobs_data = scrape_result['jobs']
    batch_id = scrape_result['batch_id']
    
    print(f"🐘 開始存儲 {len(jobs_data)} 個職缺到 PostgreSQL...")
    
    # 嘗試從 Airflow Variables 獲取連接資訊
    try:
        supabase_url = Variable.get('SUPABASE_DB_URL')
        
        print(f"✅ 從 Airflow Variables 獲取 Supabase 連接資訊")
        print(f"   URL: {supabase_url[:60]}...")
        
        # 實際連接 PostgreSQL
        import psycopg2
        import psycopg2.extras
        
        conn = psycopg2.connect(supabase_url)
        cur = conn.cursor()
        
        print("✅ Supabase 連接成功")
        
        # 插入數據到 raw_staging 表
        inserted = 0
        for job in jobs_data:
            try:
                cur.execute("""
                    INSERT INTO raw_staging.linkedin_jobs_raw 
                    (job_data, source_url, batch_id, scraped_at)
                    VALUES (%s, %s, %s, %s)
                """, (
                    json.dumps(job),
                    'mock_scraper',
                    batch_id,
                    datetime.now()
                ))
                inserted += 1
            except Exception as e:
                print(f"⚠️  插入失敗: {str(e)}")
                continue
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"✅ 成功存儲 {inserted} 個職缺到 Supabase")
        
        stats = {
            'inserted': inserted,
            'batch_id': batch_id,
            'table': 'raw_staging.linkedin_jobs_raw'
        }
        
        context['task_instance'].xcom_push(key='postgres_stats', value=stats)
        return f"Stored {inserted} jobs to PostgreSQL"
        
    except Exception as e:
        print(f"⚠️  Supabase 連接失敗: {str(e)}")
        print("💡 提示：")
        print("   1. 確保已運行 ./fix_supabase_connection.sh")
        print("   2. 檢查 Supabase URL 是否正確（使用 pooler 地址）")
        print("   3. 驗證網路連接")
        print("📝 模擬模式：記錄但不實際存儲")
        
        stats = {
            'inserted': 0,
            'batch_id': batch_id,
            'error': str(e),
            'simulated': True
        }
        
        context['task_instance'].xcom_push(key='postgres_stats', value=stats)
        return f"PostgreSQL storage failed: {str(e)}"

def log_summary(**context):
    """記錄執行摘要"""
    print("📊 執行摘要")
    print("=" * 60)
    
    # 獲取各個步驟的結果
    variables_check = context['task_instance'].xcom_pull(
        task_ids='check_variables',
        key='variables_check'
    )
    
    scrape_result = context['task_instance'].xcom_pull(
        task_ids='scrape_jobs',
        key='scrape_result'
    )
    
    mongodb_stats = context['task_instance'].xcom_pull(
        task_ids='store_to_mongodb',
        key='mongodb_stats'
    )
    
    postgres_stats = context['task_instance'].xcom_pull(
        task_ids='store_to_postgres',
        key='postgres_stats'
    )
    
    print(f"\n📋 批次資訊:")
    print(f"   批次 ID: {scrape_result['batch_id']}")
    print(f"   時間: {scrape_result['timestamp']}")
    
    print(f"\n🎯 爬取結果:")
    print(f"   生成職缺: {scrape_result['total_jobs']}")
    
    print(f"\n🍃 MongoDB 存儲:")
    if mongodb_stats.get('simulated'):
        print(f"   ⚠️  模擬模式（未實際存儲）")
        print(f"   錯誤: {mongodb_stats.get('error', 'N/A')}")
    else:
        print(f"   ✅ 成功存儲: {mongodb_stats['inserted']} 個職缺")
    
    print(f"\n🐘 PostgreSQL 存儲:")
    if postgres_stats.get('simulated'):
        print(f"   ⚠️  模擬模式（未實際存儲）")
        print(f"   錯誤: {postgres_stats.get('error', 'N/A')}")
    else:
        print(f"   ✅ 成功存儲: {postgres_stats['inserted']} 個職缺")
    
    print("\n" + "=" * 60)
    
    return "Summary logged"

# ============================================================================
# 定義 Tasks
# ============================================================================

check_vars_task = PythonOperator(
    task_id='check_variables',
    python_callable=check_variables,
    dag=dag
)

scrape_task = PythonOperator(
    task_id='scrape_jobs',
    python_callable=scrape_jobs,
    dag=dag
)

mongodb_task = PythonOperator(
    task_id='store_to_mongodb',
    python_callable=store_to_mongodb,
    dag=dag
)

postgres_task = PythonOperator(
    task_id='store_to_postgres',
    python_callable=store_to_postgres,
    dag=dag
)

summary_task = PythonOperator(
    task_id='log_summary',
    python_callable=log_summary,
    dag=dag
)

# ============================================================================
# 定義依賴關係
# ============================================================================

check_vars_task >> scrape_task >> [mongodb_task, postgres_task] >> summary_task