# dags/scrapers/linkedin_scraper_dag.py
# LinkedIn 每日爬蟲 DAG - Phase 1 基礎框架

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

# 加入 src 路徑
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

# ============================================================================
# DAG 配置
# ============================================================================

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=2)  # 防止爬蟲卡住
}

dag = DAG(
    'linkedin_daily_scraper',
    default_args=default_args,
    description='LinkedIn 職缺每日爬蟲 - 完整數據收集流程',
    schedule='@daily',  # 每天執行一次
    max_active_runs=1,  # 避免重疊執行
    catchup=False,      # 不回補歷史執行
    tags=['scraper', 'linkedin', 'daily', 'jobs']
)

# ============================================================================
# Task 函數定義 (Phase 1 - 基礎版本)
# ============================================================================

def setup_scraper_config(**context):
    """設定今日爬蟲配置參數"""
    from datetime import datetime
    
    # 動態生成批次 ID
    execution_date = context['ds']  # YYYY-MM-DD 格式
    batch_id = f"linkedin_{execution_date.replace('-', '')}"
    
    config = {
        'batch_id': batch_id,
        'execution_date': execution_date,
        
        # 爬取目標設定
        'target_jobs': 100,  # Phase 1 先設少一點測試
        'max_pages_per_search': 3,
        
        # 搜尋條件
        'search_terms': [
            'data engineer',
            'senior data engineer',
            'data scientist'
        ],
        'locations': [
            'San Francisco Bay Area',
            'New York',
            'Seattle'
        ],
        
        # 反爬設定
        'delay_range': (2, 4),  # 隨機延遲 2-4 秒
        'request_timeout': 30,
        'max_retries': 3,
        
        # 品質控制
        'min_required_fields': ['job_title', 'company_name', 'location'],
        'skip_duplicates': True
    }
    
    print(f"✅ 爬蟲配置已生成:")
    print(f"   批次 ID: {config['batch_id']}")
    print(f"   目標職缺: {config['target_jobs']}")
    print(f"   搜尋關鍵字: {len(config['search_terms'])} 個")
    print(f"   目標城市: {len(config['locations'])} 個")
    
    # 儲存配置到 XCom 供後續 Task 使用
    context['task_instance'].xcom_push(key='scraper_config', value=config)
    
    return f"Config ready for batch {config['batch_id']}"


def check_rate_limits(**context):
    """檢查爬取頻率限制"""
    import redis
    from datetime import datetime, timedelta
    
    try:
        # 連接 Redis (本地開發環境)
        r = redis.from_url('redis://localhost:6379')
        
        # 檢查上次 LinkedIn 爬取時間
        last_scrape_key = 'linkedin:last_scrape_time'
        last_scrape = r.get(last_scrape_key)
        
        current_time = datetime.now()
        
        if last_scrape:
            last_scrape_time = datetime.fromisoformat(last_scrape.decode())
            time_diff = current_time - last_scrape_time
            
            # 最少間隔 12 小時 (保守策略)
            min_interval = timedelta(hours=12)
            
            if time_diff < min_interval:
                remaining = min_interval - time_diff
                print(f"⚠️  爬取頻率限制: 還需等待 {remaining}")
                print(f"   上次爬取: {last_scrape_time}")
                # Phase 1 暫時不跳過，只記錄警告
                # raise AirflowSkipException("Rate limit hit")
        
        # 更新爬取時間記錄
        r.set(last_scrape_key, current_time.isoformat(), ex=86400)  # 24小時過期
        
        print(f"✅ 頻率檢查通過，可以開始爬取")
        return "Rate limit check passed"
        
    except Exception as e:
        print(f"⚠️  Redis 連線失敗，跳過頻率檢查: {str(e)}")
        return "Rate limit check skipped (Redis unavailable)"


def scrape_linkedin_jobs(**context):
    """執行 LinkedIn 職缺爬蟲 - Phase 1 基礎版本"""
    
    # 取得配置
    config = context['task_instance'].xcom_pull(
        task_ids='setup_scraper_config', 
        key='scraper_config'
    )
    
    print(f"🚀 開始爬取 LinkedIn 職缺...")
    print(f"   批次 ID: {config['batch_id']}")
    
    try:
        # Phase 1: 使用基礎爬蟲邏輯
        from scrapers.linkedin_scraper import LinkedInBasicScraper
        
        scraper = LinkedInBasicScraper(config)
        jobs_data = scraper.scrape_jobs()
        
        # 統計結果
        total_jobs = len(jobs_data)
        success_rate = scraper.get_success_rate()
        
        print(f"✅ 爬取完成:")
        print(f"   總計職缺: {total_jobs}")
        print(f"   成功率: {success_rate:.1%}")
        
        # 儲存結果到 XCom
        result = {
            'batch_id': config['batch_id'],
            'jobs_data': jobs_data,
            'total_jobs': total_jobs,
            'success_rate': success_rate,
            'scrape_timestamp': datetime.now().isoformat()
        }
        
        context['task_instance'].xcom_push(key='scrape_result', value=result)
        
        return f"Successfully scraped {total_jobs} jobs"
        
    except Exception as e:
        print(f"❌ 爬取失敗: {str(e)}")
        raise


def validate_scraped_data(**context):
    """驗證爬取資料品質"""
    
    # 取得爬取結果
    scrape_result = context['task_instance'].xcom_pull(
        task_ids='scrape_linkedin_jobs',
        key='scrape_result'
    )
    
    if not scrape_result or not scrape_result.get('jobs_data'):
        raise ValueError("No scraped data found")
    
    jobs_data = scrape_result['jobs_data']
    
    print(f"🔍 開始驗證 {len(jobs_data)} 筆職缺資料...")
    
    # Phase 1 基礎驗證邏輯
    validation_results = {
        'total_jobs': len(jobs_data),
        'valid_jobs': 0,
        'invalid_jobs': 0,
        'completeness_scores': [],
        'quality_flags': []
    }
    
    valid_jobs = []
    
    for i, job in enumerate(jobs_data):
        # 檢查必要欄位
        required_fields = ['job_title', 'company_name', 'location', 'job_url']
        missing_fields = [field for field in required_fields if not job.get(field)]
        
        # 計算完整性分數
        total_fields = ['job_title', 'company_name', 'location', 'job_url', 
                       'job_description', 'salary_range', 'employment_type']
        filled_fields = sum(1 for field in total_fields if job.get(field))
        completeness_score = filled_fields / len(total_fields)
        
        validation_results['completeness_scores'].append(completeness_score)
        
        if missing_fields:
            validation_results['invalid_jobs'] += 1
            validation_results['quality_flags'].append({
                'job_index': i,
                'missing_fields': missing_fields,
                'completeness_score': completeness_score
            })
            print(f"⚠️  職缺 {i+1} 缺少必要欄位: {missing_fields}")
        else:
            validation_results['valid_jobs'] += 1
            job['completeness_score'] = completeness_score
            valid_jobs.append(job)
    
    # 計算整體品質指標
    avg_completeness = sum(validation_results['completeness_scores']) / len(validation_results['completeness_scores'])
    validation_results['average_completeness'] = avg_completeness
    
    print(f"✅ 資料驗證完成:")
    print(f"   有效職缺: {validation_results['valid_jobs']}")
    print(f"   無效職缺: {validation_results['invalid_jobs']}")
    print(f"   平均完整性: {avg_completeness:.2%}")
    
    # 更新結果
    validated_result = scrape_result.copy()
    validated_result['jobs_data'] = valid_jobs
    validated_result['validation_results'] = validation_results
    
    context['task_instance'].xcom_push(key='validated_data', value=validated_result)
    
    return f"Validated {validation_results['valid_jobs']} valid jobs"


def store_to_mongodb(**context):
    """儲存資料到 MongoDB Atlas"""
    
    # 取得驗證後的資料
    validated_data = context['task_instance'].xcom_pull(
        task_ids='validate_scraped_data',
        key='validated_data'
    )
    
    if not validated_data or not validated_data.get('jobs_data'):
        print("⚠️  沒有有效資料需要儲存")
        return "No data to store"
    
    jobs_data = validated_data['jobs_data']
    batch_id = validated_data['batch_id']
    
    print(f"💾 開始儲存 {len(jobs_data)} 筆資料到 MongoDB Atlas...")
    
    try:
        from pymongo import MongoClient
        from pymongo.server_api import ServerApi
        import os
        
        # 連接 MongoDB Atlas
        mongodb_url = os.getenv('MONGODB_ATLAS_URL')
        if not mongodb_url:
            raise ValueError("MONGODB_ATLAS_URL environment variable not set")
        
        client = MongoClient(mongodb_url, server_api=ServerApi('1'))
        db = client[os.getenv('MONGODB_ATLAS_DB_NAME', 'job_market_data')]
        collection = db['raw_jobs_data']
        
        # 批次插入/更新資料
        operations = []
        for job in jobs_data:
            document = {
                'source': 'linkedin',
                'job_data': job,
                'metadata': {
                    'scraped_at': datetime.now(),
                    'batch_id': batch_id,
                    'scraper_version': '1.0.0-phase1',
                    'source_url': job.get('job_url', '')
                },
                'data_quality': {
                    'completeness_score': job.get('completeness_score', 0),
                    'flags': []
                }
            }
            
            # 使用 job_url 作為唯一識別，避免重複
            filter_condition = {
                'job_data.job_url': job.get('job_url'),
                'source': 'linkedin'
            }
            
            operations.append({
                'filter': filter_condition,
                'document': document,
                'upsert': True
            })
        
        # 執行批次 upsert
        if operations:
            results = []
            for op in operations:
                result = collection.replace_one(
                    op['filter'], 
                    op['document'], 
                    upsert=op['upsert']
                )
                results.append(result)
            
            inserted_count = sum(1 for r in results if r.upserted_id)
            updated_count = sum(1 for r in results if r.modified_count > 0)
            
            print(f"✅ MongoDB 儲存完成:")
            print(f"   新增: {inserted_count} 筆")
            print(f"   更新: {updated_count} 筆")
            
            # 記錄儲存統計
            storage_stats = {
                'mongodb_inserted': inserted_count,
                'mongodb_updated': updated_count,
                'mongodb_total': len(operations)
            }
            
            context['task_instance'].xcom_push(key='mongodb_stats', value=storage_stats)
            
            client.close()
            return f"Stored {len(operations)} jobs to MongoDB"
        
    except Exception as e:
        print(f"❌ MongoDB 儲存失敗: {str(e)}")
        raise


def store_to_postgres_raw(**context):
    """儲存資料到 PostgreSQL Raw Staging"""
    
    # 取得驗證後的資料
    validated_data = context['task_instance'].xcom_pull(
        task_ids='validate_scraped_data',
        key='validated_data'
    )
    
    if not validated_data or not validated_data.get('jobs_data'):
        print("⚠️  沒有有效資料需要儲存")
        return "No data to store"
    
    jobs_data = validated_data['jobs_data']
    batch_id = validated_data['batch_id']
    
    print(f"🐘 開始儲存 {len(jobs_data)} 筆資料到 PostgreSQL Raw Staging...")
    
    try:
        import psycopg2
        import json
        import os
        
        # 連接 Supabase PostgreSQL
        supabase_url = os.getenv('SUPABASE_DB_URL')
        if not supabase_url:
            raise ValueError("SUPABASE_DB_URL environment variable not set")
        
        conn = psycopg2.connect(supabase_url)
        cur = conn.cursor()
        
        # 準備插入資料
        insert_sql = """
        INSERT INTO raw_staging.linkedin_jobs_raw (
            source_job_id, source_url, job_title, company_name,
            location_raw, job_description, employment_type,
            work_arrangement, raw_json, batch_id, scraped_at
        ) VALUES (
            %(source_job_id)s, %(source_url)s, %(job_title)s, %(company_name)s,
            %(location_raw)s, %(job_description)s, %(employment_type)s,
            %(work_arrangement)s, %(raw_json)s, %(batch_id)s, %(scraped_at)s
        ) ON CONFLICT (source_job_id, batch_id) DO UPDATE SET
            job_title = EXCLUDED.job_title,
            company_name = EXCLUDED.company_name,
            location_raw = EXCLUDED.location_raw,
            job_description = EXCLUDED.job_description,
            raw_json = EXCLUDED.raw_json,
            scraped_at = EXCLUDED.scraped_at
        """
        
        inserted_count = 0
        for job in jobs_data:
            # 從 job_url 提取 job_id (LinkedIn URL 結構)
            job_url = job.get('job_url', '')
            job_id = 'unknown'
            if '/jobs/view/' in job_url:
                try:
                    job_id = job_url.split('/jobs/view/')[-1].split('?')[0]
                except:
                    job_id = f"batch_{batch_id}_{inserted_count}"
            
            row_data = {
                'source_job_id': job_id,
                'source_url': job_url,
                'job_title': job.get('job_title', ''),
                'company_name': job.get('company_name', ''),
                'location_raw': job.get('location', ''),
                'job_description': job.get('job_description', ''),
                'employment_type': job.get('employment_type', ''),
                'work_arrangement': job.get('work_arrangement', ''),
                'raw_json': json.dumps(job),
                'batch_id': batch_id,
                'scraped_at': datetime.now()
            }
            
            cur.execute(insert_sql, row_data)
            inserted_count += 1
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"✅ PostgreSQL 儲存完成: {inserted_count} 筆")
        
        # 記錄儲存統計
        storage_stats = {
            'postgres_inserted': inserted_count
        }
        
        context['task_instance'].xcom_push(key='postgres_stats', value=storage_stats)
        
        return f"Stored {inserted_count} jobs to PostgreSQL"
        
    except Exception as e:
        print(f"❌ PostgreSQL 儲存失敗: {str(e)}")
        raise


def log_scraping_metrics(**context):
    """記錄爬取指標和統計"""
    
    # 收集所有 Task 的執行結果
    scrape_result = context['task_instance'].xcom_pull(
        task_ids='scrape_linkedin_jobs',
        key='scrape_result'
    ) or {}
    
    validated_data = context['task_instance'].xcom_pull(
        task_ids='validate_scraped_data',
        key='validated_data'
    ) or {}
    
    mongodb_stats = context['task_instance'].xcom_pull(
        task_ids='store_to_mongodb',
        key='mongodb_stats'
    ) or {}
    
    postgres_stats = context['task_instance'].xcom_pull(
        task_ids='store_to_postgres_raw',
        key='postgres_stats'
    ) or {}
    
    # 編譯完整的執行報告
    execution_report = {
        'dag_id': context['dag'].dag_id,
        'execution_date': context['ds'],
        'batch_id': scrape_result.get('batch_id', 'unknown'),
        
        # 爬取統計
        'scraping': {
            'total_scraped': scrape_result.get('total_jobs', 0),
            'success_rate': scrape_result.get('success_rate', 0),
            'scrape_timestamp': scrape_result.get('scrape_timestamp')
        },
        
        # 驗證統計
        'validation': validated_data.get('validation_results', {}),
        
        # 儲存統計
        'storage': {
            'mongodb': mongodb_stats,
            'postgresql': postgres_stats
        },
        
        # 執行時間
        'execution_time': {
            'start_time': context['task_instance'].start_date.isoformat() if context['task_instance'].start_date else None,
            'end_time': datetime.now().isoformat()
        }
    }
    
    print(f"📊 爬取執行報告:")
    print(f"=" * 50)
    print(f"批次 ID: {execution_report['batch_id']}")
    print(f"執行日期: {execution_report['execution_date']}")
    print(f"爬取職缺: {execution_report['scraping']['total_scraped']}")
    print(f"成功率: {execution_report['scraping']['success_rate']:.1%}")
    print(f"有效職缺: {execution_report['validation'].get('valid_jobs', 0)}")
    print(f"MongoDB 儲存: {execution_report['storage']['mongodb'].get('mongodb_total', 0)}")
    print(f"PostgreSQL 儲存: {execution_report['storage']['postgresql'].get('postgres_inserted', 0)}")
    
    # 儲存報告 (可以後續加入到資料庫或檔案)
    context['task_instance'].xcom_push(key='execution_report', value=execution_report)
    
    return "Metrics logged successfully"


# ============================================================================
# Task 定義
# ============================================================================

# Task 1: 設定爬蟲配置
setup_config_task = PythonOperator(
    task_id='setup_scraper_config',
    python_callable=setup_scraper_config,
    dag=dag
)

# Task 2: 檢查頻率限制
rate_limit_task = PythonOperator(
    task_id='check_rate_limits',
    python_callable=check_rate_limits,
    dag=dag
)

# Task 3: 執行爬蟲
scrape_task = PythonOperator(
    task_id='scrape_linkedin_jobs',
    python_callable=scrape_linkedin_jobs,
    dag=dag
)

# Task 4: 驗證資料
validate_task = PythonOperator(
    task_id='validate_scraped_data',
    python_callable=validate_scraped_data,
    dag=dag
)

# Task 5: 儲存到 MongoDB
mongodb_task = PythonOperator(
    task_id='store_to_mongodb',
    python_callable=store_to_mongodb,
    dag=dag
)

# Task 6: 儲存到 PostgreSQL
postgres_task = PythonOperator(
    task_id='store_to_postgres_raw',
    python_callable=store_to_postgres_raw,
    dag=dag
)

# Task 7: 記錄指標
metrics_task = PythonOperator(
    task_id='log_scraping_metrics',
    python_callable=log_scraping_metrics,
    dag=dag
)

# 系統檢查 Task (可選)
system_check_task = BashOperator(
    task_id='system_check',
    bash_command='''
    echo "🖥️  LinkedIn 爬蟲系統檢查:"
    echo "執行時間: $(date)"
    echo "Python 版本: $(python3 --version)"
    echo "可用記憶體: $(free -h | grep Mem | awk '{print $7}')" || echo "記憶體資訊不可用"
    ''',
    dag=dag
)

# ============================================================================
# Task 依賴關係
# ============================================================================

# 線性執行流程
system_check_task >> setup_config_task >> rate_limit_task >> scrape_task >> validate_task

# 並行儲存 (MongoDB 和 PostgreSQL 可同時進行)
validate_task >> [mongodb_task, postgres_task]

# 最終指標記錄 (等待所有儲存完成)
[mongodb_task, postgres_task] >> metrics_task