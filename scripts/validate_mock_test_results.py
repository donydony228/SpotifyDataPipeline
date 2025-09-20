# scripts/validate_mock_test_results.py
# 驗證模擬測試結果的腳本

import psycopg2
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def validate_mock_test_results():
    """驗證模擬測試的完整結果"""
    
    print("🧪 驗證模擬測試結果")
    print("=" * 50)
    
    validation_results = {
        'mongodb_validation': False,
        'postgresql_validation': False,
        'data_consistency': False,
        'overall_success': False
    }
    
    # 1. 驗證 MongoDB Atlas 中的模擬資料
    print("\n📊 1. 檢查 MongoDB Atlas...")
    try:
        mongodb_url = os.getenv('MONGODB_ATLAS_URL')
        if not mongodb_url:
            print("   ⚠️  MONGODB_ATLAS_URL 未設定，跳過 MongoDB 驗證")
        else:
            client = MongoClient(mongodb_url, server_api=ServerApi('1'))
            db = client[os.getenv('MONGODB_ATLAS_DB_NAME', 'job_market_data')]
            
            # 查找最近的模擬資料
            recent_cutoff = datetime.now() - timedelta(hours=1)
            mock_jobs = db['raw_jobs_data'].find({
                'metadata.is_mock_data': True,
                'metadata.scraped_at': {'$gte': recent_cutoff}
            })
            
            mock_job_list = list(mock_jobs)
            mock_count = len(mock_job_list)
            
            if mock_count > 0:
                print(f"   ✅ 找到 {mock_count} 筆模擬資料")
                
                # 檢查資料結構
                sample_job = mock_job_list[0]
                required_fields = ['source', 'job_data', 'metadata', 'data_quality']
                
                missing_fields = [field for field in required_fields if field not in sample_job]
                if not missing_fields:
                    print("   ✅ 資料結構正確")
                    validation_results['mongodb_validation'] = True
                else:
                    print(f"   ❌ 缺少必要欄位: {missing_fields}")
                
                # 檢查模擬資料標記
                mock_marked = all(job.get('metadata', {}).get('is_mock_data', False) for job in mock_job_list)
                if mock_marked:
                    print("   ✅ 所有資料都正確標記為模擬資料")
                else:
                    print("   ⚠️  部分資料未正確標記為模擬資料")
                
            else:
                print("   ❌ 未找到最近的模擬資料")
            
            client.close()
    
    except Exception as e:
        print(f"   ❌ MongoDB 驗證失敗: {str(e)}")
    
    # 2. 驗證 PostgreSQL Supabase 中的模擬資料
    print("\n📊 2. 檢查 PostgreSQL Supabase...")
    try:
        supabase_url = os.getenv('SUPABASE_DB_URL')
        if not supabase_url:
            print("   ⚠️  SUPABASE_DB_URL 未設定，跳過 PostgreSQL 驗證")
        else:
            conn = psycopg2.connect(supabase_url)
            cur = conn.cursor()
            
            # 查找最近的模擬資料
            recent_cutoff = datetime.now() - timedelta(hours=1)
            cur.execute("""
                SELECT COUNT(*), batch_id, scraped_at 
                FROM raw_staging.linkedin_jobs_raw 
                WHERE 'mock_data' = ANY(data_quality_flags) 
                AND scraped_at >= %s
                GROUP BY batch_id, scraped_at
                ORDER BY scraped_at DESC
                LIMIT 5
            """, (recent_cutoff,))
            
            results = cur.fetchall()
            
            if results:
                total_mock_jobs = sum(row[0] for row in results)
                latest_batch = results[0]
                
                print(f"   ✅ 找到 {total_mock_jobs} 筆模擬資料")
                print(f"   📋 最新批次: {latest_batch[1]} ({latest_batch[0]} 筆)")
                
                # 檢查資料完整性
                cur.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(job_title) as has_title,
                        COUNT(company_name) as has_company,
                        COUNT(location_raw) as has_location,
                        COUNT(job_description) as has_description
                    FROM raw_staging.linkedin_jobs_raw 
                    WHERE 'mock_data' = ANY(data_quality_flags)
                    AND scraped_at >= %s
                """, (recent_cutoff,))
                
                completeness = cur.fetchone()
                if completeness:
                    total = completeness[0]
                    required_complete = completeness[1] == completeness[2] == completeness[3] == total
                    
                    if required_complete:
                        print("   ✅ 必要欄位完整性檢查通過")
                        validation_results['postgresql_validation'] = True
                    else:
                        print("   ❌ 部分資料缺少必要欄位")
                        print(f"      標題: {completeness[1]}/{total}")
                        print(f"      公司: {completeness[2]}/{total}")
                        print(f"      地點: {completeness[3]}/{total}")
                
            else:
                print("   ❌ 未找到最近的模擬資料")
            
            conn.close()
    
    except Exception as e:
        print(f"   ❌ PostgreSQL 驗證失敗: {str(e)}")
    
    # 3. 檢查資料一致性
    print("\n📊 3. 檢查資料一致性...")
    if validation_results['mongodb_validation'] and validation_results['postgresql_validation']:
        try:
            # 比較兩個資料庫中的資料數量
            # 這裡可以加入更詳細的一致性檢查
            print("   ✅ 兩個資料庫都有模擬資料")
            validation_results['data_consistency'] = True
        except Exception as e:
            print(f"   ❌ 資料一致性檢查失敗: {str(e)}")
    else:
        print("   ⚠️  無法進行一致性檢查（部分資料庫驗證失敗）")
    
    # 4. 整體評估
    print("\n📊 4. 整體評估...")
    
    success_count = sum(validation_results.values())
    total_checks = len(validation_results) - 1  # 排除 overall_success
    
    if success_count >= total_checks - 1:  # 允許一個檢查失敗
        validation_results['overall_success'] = True
        print("   🎉 整體測試成功！模擬資料流程運作正常")
    else:
        print("   ❌ 測試部分失敗，需要檢查問題")
    
    # 5. 總結報告
    print("\n" + "=" * 50)
    print("📋 測試結果總結:")
    print(f"   MongoDB 驗證: {'✅ 通過' if validation_results['mongodb_validation'] else '❌ 失敗'}")
    print(f"   PostgreSQL 驗證: {'✅ 通過' if validation_results['postgresql_validation'] else '❌ 失敗'}")
    print(f"   資料一致性: {'✅ 通過' if validation_results['data_consistency'] else '❌ 失敗'}")
    print(f"   整體結果: {'🎉 成功' if validation_results['overall_success'] else '❌ 失敗'}")
    
    if validation_results['overall_success']:
        print("\n🚀 下一步建議:")
        print("   1. 模擬測試通過，可以開始真實爬蟲測試")
        print("   2. 修改 LinkedIn Scraper DAG 使用真實爬蟲")
        print("   3. 進行小規模真實資料測試 (5-10 個職缺)")
    else:
        print("\n🔧 修復建議:")
        print("   1. 檢查 Airflow DAG 執行日誌")
        print("   2. 確認資料庫連線設定")
        print("   3. 重新執行模擬測試")
    
    return validation_results['overall_success']


def check_recent_airflow_runs():
    """檢查最近的 Airflow DAG 執行狀況"""
    print("\n🌊 檢查最近的 Airflow 執行...")
    
    # 這裡可以加入檢查 Airflow 執行狀況的邏輯
    # 比如檢查 dag_run 表或使用 Airflow API
    
    print("   💡 請手動檢查 Airflow UI:")
    print("   1. 前往 http://localhost:8080")
    print("   2. 找到 'linkedin_mock_scraper_test' DAG")
    print("   3. 確認最近執行是否成功")
    print("   4. 查看每個 Task 的日誌輸出")


if __name__ == "__main__":
    print("🧪 開始驗證模擬測試結果...")
    print("確保你已經執行了 linkedin_mock_scraper_test DAG")
    print()
    
    success = validate_mock_test_results()
    
    check_recent_airflow_runs()
    
    if success:
        exit(0)
    else:
        exit(1)